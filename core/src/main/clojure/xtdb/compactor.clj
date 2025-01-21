(ns xtdb.compactor
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.bitemporal :as bitemp]
            [xtdb.file-list :as fl]
            [xtdb.metrics :as metrics]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (java.lang AutoCloseable)
           [java.nio.file Path]
           [java.time Duration]
           [java.util Arrays Comparator HashSet LinkedList PriorityQueue]
           [java.util.concurrent Executors Semaphore TimeUnit]
           [java.util.concurrent.locks ReentrantLock]
           (java.util.function Predicate)
           [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.memory.util ArrowBufPointer]
           (org.apache.arrow.vector.types.pojo Field FieldType)
           (xtdb Compactor IBufferPool)
           xtdb.api.CompactorConfig
           (xtdb.arrow Relation RelationReader RowCopier Vector VectorWriter)
           xtdb.bitemporal.IPolygonReader
           (xtdb.metadata IMetadataManager)
           (xtdb.trie EventRowPointer EventRowPointer$XtArrow HashTrieKt IDataRel MergePlanTask)
           (xtdb.util TemporalBounds)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defprotocol PCompactor
  (-compact-all! [compactor timeout])
  (signal-block! [compactor]))

(def ^:dynamic *ignore-signal-block?* false)

(defn- ->reader->copier [^Relation data-wtr]
  (let [iid-wtr (.get data-wtr "_iid")
        sf-wtr (.get data-wtr "_system_from")
        vf-wtr (.get data-wtr "_valid_from")
        vt-wtr (.get data-wtr "_valid_to")
        op-wtr (.get data-wtr "op")]
    (fn reader->copier [^RelationReader data-rdr]
      (let [iid-copier (-> (.get data-rdr "_iid") (.rowCopier iid-wtr))
            sf-copier (-> (.get data-rdr "_system_from") (.rowCopier sf-wtr))
            vf-copier (-> (.get data-rdr "_valid_from") (.rowCopier vf-wtr))
            vt-copier (-> (.get data-rdr "_valid_to") (.rowCopier vt-wtr))
            op-copier (-> (.get data-rdr "op") (.rowCopier op-wtr))]
        (reify RowCopier
          (copyRow [_ ev-idx]
            (let [pos (.copyRow iid-copier ev-idx)]
              (.copyRow sf-copier ev-idx)
              (.copyRow vf-copier ev-idx)
              (.copyRow vt-copier ev-idx)
              (.copyRow op-copier ev-idx)
              (.endRow data-wtr)

              pos)))))))

(defn merge-segments-into [^Relation data-rel, ^VectorWriter recency-wtr, segments, ^bytes path-filter]
  (let [reader->copier (->reader->copier data-rel)
        calculate-polygon (bitemp/polygon-calculator)

        is-valid-ptr (ArrowBufPointer.)]

    (doseq [^MergePlanTask merge-plan-task (HashTrieKt/toMergePlan segments
                                                                   (when path-filter
                                                                     (let [path-len (alength path-filter)]
                                                                       (reify Predicate
                                                                         (test [_ page-path]
                                                                           (let [^bytes page-path page-path
                                                                                 len (min path-len (alength page-path))]
                                                                             (Arrays/equals path-filter 0 len
                                                                                            page-path 0 len))))))
                                                                   (TemporalBounds.))
            :let [_ (when (Thread/interrupted)
                      (throw (InterruptedException.)))

                  mp-nodes (.getMpNodes merge-plan-task)
                  ^bytes path (.getPath merge-plan-task)
                  data-rdrs (mapv trie/load-data-page mp-nodes)
                  merge-q (PriorityQueue. (Comparator/comparing :ev-ptr (EventRowPointer/comparator)))
                  path (if (or (nil? path-filter)
                               (> (alength path) (alength path-filter)))
                         path
                         path-filter)]]

      (doseq [^RelationReader data-rdr data-rdrs
              :when data-rdr
              :let [ev-ptr (EventRowPointer$XtArrow. data-rdr path)
                    row-copier (reader->copier data-rdr)]]
        (when (.isValid ev-ptr is-valid-ptr path)
          (.add merge-q {:ev-ptr ev-ptr, :row-copier row-copier})))

      (loop []
        (when-let [{:keys [^EventRowPointer ev-ptr, ^RowCopier row-copier] :as q-obj} (.poll merge-q)]
          (.copyRow row-copier (.getIndex ev-ptr))

          (.writeLong recency-wtr
                      (.getRecency ^IPolygonReader (calculate-polygon ev-ptr)))

          (.nextIndex ev-ptr)
          (when (.isValid ev-ptr is-valid-ptr path)
            (.add merge-q q-obj))
          (recur))))

    nil))

(defn ->log-data-rel-schema ^org.apache.arrow.vector.types.pojo.Schema [data-rels]
  (trie/data-rel-schema (-> (for [^IDataRel data-rel data-rels]
                                   (-> (.getSchema data-rel)
                                       (.findField "op")
                                       (.getChildren) ^Field first))
                            (->> (apply types/merge-fields))
                            (types/field-with-name "put"))))

(defn open-recency-wtr ^xtdb.arrow.Vector [allocator]
  (Vector/fromField allocator
                    (Field. "_recency"
                            (FieldType/notNullable #xt.arrow/type [:timestamp-tz :micro "UTC"])
                            nil)))

(defn exec-compaction-job! [^BufferAllocator allocator, ^IBufferPool buffer-pool, ^IMetadataManager metadata-mgr
                            {:keys [page-size]} {:keys [^Path table-path path trie-keys out-trie-key]}]
  (try
    (log/debugf "compacting '%s' '%s' -> '%s'..." table-path trie-keys out-trie-key)

    (util/with-open [table-metadatas (LinkedList.)
                     data-rels (trie/open-data-rels buffer-pool table-path trie-keys)]
      (doseq [trie-key trie-keys]
        (.add table-metadatas (.openTableMetadata metadata-mgr (trie/->table-meta-file-path table-path trie-key))))

      (let [segments (mapv (fn [{:keys [trie] :as _table-metadata} data-rel]
                             (-> (trie/->Segment trie) (assoc :data-rel data-rel)))
                           table-metadatas
                           data-rels)
            schema (->log-data-rel-schema (map :data-rel segments))]

        (util/with-open [data-rel (Relation. allocator schema)
                         recency-wtr (open-recency-wtr allocator)]
          (merge-segments-into data-rel recency-wtr segments path)

          (util/with-open [trie-wtr (trie/open-trie-writer allocator buffer-pool
                                                           schema table-path out-trie-key
                                                           true)]

            (Compactor/writeRelation trie-wtr data-rel recency-wtr page-size)))))

    (log/debugf "compacted '%s' -> '%s'." table-path out-trie-key)

    (catch InterruptedException e (throw e))

    (catch Throwable t
      (log/error t "Error running compaction job.")
      (throw t))))

(defn- l0->l1-compaction-job [fl-state {:keys [^long l1-file-size-rows]}]
  (when-let [live-l0 (seq (->> (get fl-state [0 []])
                               (take-while #(= :live (:state %)))))]
    (let [latest-l1 (->> (get fl-state [1 []])
                         (take-while #(< (:rows %) l1-file-size-rows))
                         first)]
      (loop [rows (:rows latest-l1 0)
             [{^long l0-rows :rows, :as l0-file} & more-l0s] (reverse live-l0)
             res (cond-> [] latest-l1 (conj latest-l1))]
        (if (and l0-file (< rows l1-file-size-rows))
          (recur (+ rows l0-rows) more-l0s (conj res l0-file))

          {:trie-keys (mapv :trie-key res)
           :out-trie-key (trie/->log-l0-l1-trie-key 1
                                                    (:first-row (first res))
                                                    (:next-row (last res))
                                                    rows)})))))

(defn- l1p-compaction-jobs [fl-state {:keys [^long l1-file-size-rows]}]
  (for [[[level part] files] fl-state
        :when (> level 0)
        :let [live-files (-> files
                             (->> (remove #(= :garbage (:state %))))
                             (cond->> (= level 1) (filter #(>= (:rows %) l1-file-size-rows))))]
        :when (>= (count live-files) fl/branch-factor)
        :let [live-files (reverse live-files)]

        p (range fl/branch-factor)
        :let [out-level (inc level)
              out-path (conj part p)
              {lnp1-next-row :next-row} (first (get fl-state [out-level out-path]))

              in-files (-> live-files
                           (cond->> lnp1-next-row (drop-while #(<= (:next-row %) lnp1-next-row)))
                           (->> (take fl/branch-factor)))]

        :when (seq in-files)
        :let [trie-keys (mapv :trie-key in-files)
              out-next-row (:next-row (last in-files))]]
    {:trie-keys trie-keys
     :path (byte-array out-path)
     :out-trie-key (trie/->log-l2+-trie-key out-level out-path out-next-row)}))

(defn compaction-jobs [meta-file-names opts]
  (let [fl-state (->> (sort meta-file-names)
                      (reduce fl/apply-file-notification {}))]
    (concat (when-let [job (l0->l1-compaction-job fl-state opts)]
              [job])
            (l1p-compaction-jobs fl-state opts))))

(defrecord NoOp []
  PCompactor
  (signal-block! [_])

  AutoCloseable
  (close [_]))

(defmethod ig/prep-key :xtdb/compactor [_ ^CompactorConfig config]
  {:allocator (ig/ref :xtdb/allocator)
   :buffer-pool (ig/ref :xtdb/buffer-pool)
   :metadata-mgr (ig/ref :xtdb.metadata/metadata-manager)
   :threads (max 1 (/ (.availableProcessors (Runtime/getRuntime)) 2))
   :metrics-registry (ig/ref :xtdb.metrics/registry)
   :enabled? (.getEnabled config)})

(def ^:dynamic *page-size* 1024)
(def ^:dynamic *l1-file-size-rows* (bit-shift-left 1 18))

(defn- open-compactor [{:keys [allocator, ^IBufferPool buffer-pool, metadata-mgr, ^long threads metrics-registry]}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "compactor")]
    (metrics/add-allocator-gauge metrics-registry "compactor.allocator.allocated_memory" allocator)
    (let [page-size *page-size*
          l1-file-size-rows *l1-file-size-rows*
          ignore-signal-block? *ignore-signal-block?*]
      (letfn [(available-jobs []
                (for [table-path (.listObjects buffer-pool util/tables-dir)
                      job (compaction-jobs (trie/list-meta-files buffer-pool table-path)
                                           {:l1-file-size-rows l1-file-size-rows})]
                  (assoc job :table-path table-path)))]

        (let [lock (ReentrantLock.)
              wake-up-mgr (.newCondition lock)
              nothing-to-do (.newCondition lock)
              active-task-limit (Semaphore. threads)

              !queued-jobs (HashSet.)
              !available-jobs (atom #{})
              task-pool (Executors/newThreadPerTaskExecutor (-> (Thread/ofVirtual)
                                                                (.name "xtdb.compactor-" 0)
                                                                (.factory)))]

          (letfn [(compactor-task [{:keys [out-trie-key] :as job}]
                    (.acquire active-task-limit)
                    (try
                      (when (contains? @!available-jobs out-trie-key)
                        (exec-compaction-job! allocator buffer-pool metadata-mgr {:page-size page-size} job))

                      (finally
                        (.release active-task-limit)))

                    (.lock lock)
                    (try
                      (.remove !queued-jobs out-trie-key)
                      (.signalAll wake-up-mgr)
                      (finally
                        (.unlock lock))))]

            (let [mgr-thread (-> (Thread/ofPlatform)
                                 (.name "xtdb.compactor.manager")
                                 (.uncaughtExceptionHandler util/uncaught-exception-handler)
                                 (.start (fn []
                                           (try
                                             (while true
                                               (when (Thread/interrupted)
                                                 (throw (InterruptedException.)))

                                               (.lock lock)
                                               (try
                                                 (let [available-jobs (available-jobs)
                                                       available-job-keys (into #{} (map :out-trie-key) available-jobs)]
                                                   (reset! !available-jobs available-job-keys)
                                                   (if (and (empty? available-jobs)
                                                            (empty? !queued-jobs))
                                                     (do
                                                       (log/trace "nothing to do")
                                                       (.signalAll nothing-to-do))

                                                     (doseq [{:keys [out-trie-key] :as job} available-jobs
                                                             :when (.add !queued-jobs out-trie-key)]
                                                       (log/trace "submitting" out-trie-key)
                                                       (.submit task-pool ^Runnable #(compactor-task job))))

                                                   (log/trace "sleeping")
                                                   (.await wake-up-mgr))
                                                 (finally
                                                   (.unlock lock))))

                                             (catch InterruptedException _))

                                           (log/debug "main compactor thread exiting"))))]

              (reify PCompactor
                (-compact-all! [_ timeout]
                  (log/info "compact-all")
                  (.lock lock)
                  (try
                    (.signalAll wake-up-mgr)
                    (if (if timeout
                          (.await nothing-to-do (.toMillis ^Duration timeout) TimeUnit/MILLISECONDS)
                          (do
                            (.await nothing-to-do)
                            true))
                      (log/info "all compacted")
                      (throw (ex-info "timed out waiting for compaction"
                                      {:available-jobs @!available-jobs
                                       :queued-jobs (into #{} !queued-jobs)})))
                    (finally
                      (.unlock lock))))

                (signal-block! [_]
                  (if ignore-signal-block?
                    (log/debug "ignoring signal")

                    (do
                      (log/debug "signal")
                      (.lock lock)
                      (try
                        (.signalAll wake-up-mgr)
                        (finally
                          (.unlock lock))))))

                AutoCloseable
                (close [_]
                  (.lock lock)
                  (try
                    (.shutdownNow task-pool)
                    (.interrupt mgr-thread)
                    (finally
                      (.unlock lock)))

                  (when-not (.awaitTermination task-pool 20 TimeUnit/SECONDS)
                    (log/warn "could not close compaction thread-pool after 20s"))

                  (when-not (.join mgr-thread (Duration/ofSeconds 5))
                    (log/warn "could not close compaction manager after 5s"))

                  (util/close allocator))))))))))

(defmethod ig/init-key :xtdb/compactor [_ {:keys [enabled?] :as opts}]
  (if enabled?
    (open-compactor opts)
    (->NoOp)))

(defmethod ig/halt-key! :xtdb/compactor [_ compactor]
  (util/close compactor))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn compact-all!
  ([node] (compact-all! node nil))
  ([node timeout] (-compact-all! (util/component node :xtdb/compactor) timeout)))

