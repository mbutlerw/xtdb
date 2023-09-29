(ns xtdb.trie
  (:require [xtdb.buffer-pool]
            [xtdb.metadata :as meta]
            [xtdb.object-store]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]
            xtdb.watermark)
  (:import (java.lang AutoCloseable)
           (java.nio ByteBuffer)
           java.nio.channels.WritableByteChannel
           java.security.MessageDigest
           (java.util ArrayList Arrays List PriorityQueue)
           (java.util.concurrent.atomic AtomicInteger)
           (java.util.function IntConsumer Supplier)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector VectorLoader VectorSchemaRoot)
           [org.apache.arrow.vector.ipc ArrowFileWriter]
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           org.apache.arrow.vector.types.UnionMode
           xtdb.buffer_pool.IBufferPool
           (xtdb.object_store ObjectStore)
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf HashTrie HashTrie$Node ILeafRow LiveHashTrie LiveHashTrie$Leaf)
           (xtdb.util WritableByteBufferChannel)
           (xtdb.vector IVectorReader RelationReader)
           xtdb.watermark.ILiveTableWatermark))

(def ^:private ^java.lang.ThreadLocal !msg-digest
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (MessageDigest/getInstance "SHA-256")))))

(defn ->iid ^ByteBuffer [eid]
  (if (uuid? eid)
    (util/uuid->byte-buffer eid)
    (ByteBuffer/wrap
     (let [^bytes eid-bytes (cond
                              (string? eid) (.getBytes (str "s" eid))
                              (keyword? eid) (.getBytes (str "k" eid))
                              (integer? eid) (.getBytes (str "i" eid))
                              :else (throw (UnsupportedOperationException. (pr-str (class eid)))))]
       (-> ^MessageDigest (.get !msg-digest)
           (.digest eid-bytes)
           (Arrays/copyOfRange 0 16))))))

(def ^org.apache.arrow.vector.types.pojo.Schema trie-schema
  (Schema. [(types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "nil" :null)
                           (types/col-type->field "branch" [:list [:union #{:null :i32}]])
                           (types/col-type->field "leaf" [:struct {'page-idx :i32
                                                                   'columns meta/metadata-col-type}]))]))

(defn log-leaf-schema ^org.apache.arrow.vector.types.pojo.Schema [put-doc-col-type]
  (Schema. [(types/col-type->field "xt$iid" [:fixed-size-binary 16])
            (types/col-type->field "xt$system_from" types/temporal-col-type)
            (types/->field "op" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "put" [:struct {'xt$valid_from types/temporal-col-type
                                                                  'xt$valid_to types/temporal-col-type
                                                                  'xt$doc put-doc-col-type}])
                           (types/col-type->field "delete" [:struct {'xt$valid_from types/temporal-col-type
                                                                     'xt$valid_to types/temporal-col-type}])
                           (types/col-type->field "evict" :null))]))

(defn open-leaf-root
  (^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
   (open-leaf-root allocator (log-leaf-schema [:union #{:null [:struct {}]}])))

  (^xtdb.vector.IRelationWriter [^BufferAllocator allocator log-leaf-schema]
   (util/with-close-on-catch [root (VectorSchemaRoot/create log-leaf-schema allocator)]
     (vw/root->writer root))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ITrieWriter
  (^xtdb.vector.IRelationWriter getLeafWriter [])
  (^int writeLeaf [])
  (^int writeBranch [^ints idxs])
  (^void end [])
  (^void close []))

(defn open-trie-writer ^xtdb.trie.ITrieWriter [^BufferAllocator allocator, ^Schema leaf-schema
                                               ^WritableByteChannel leaf-out-ch, ^WritableByteChannel trie-out-ch]
  (util/with-close-on-catch [leaf-vsr (VectorSchemaRoot/create leaf-schema allocator)
                             leaf-out-wtr (ArrowFileWriter. leaf-vsr nil leaf-out-ch)
                             trie-vsr (VectorSchemaRoot/create trie-schema allocator)]
    (.start leaf-out-wtr)
    (let [leaf-rel-wtr (vw/root->writer leaf-vsr)
          trie-rel-wtr (vw/root->writer trie-vsr)

          node-wtr (.writerForName trie-rel-wtr "nodes")
          node-wp (.writerPosition node-wtr)

          branch-wtr (.writerForTypeId node-wtr (byte 1))
          branch-el-wtr (.listElementWriter branch-wtr)

          leaf-wtr (.writerForTypeId node-wtr (byte 2))
          page-idx-wtr (.structKeyWriter leaf-wtr "page-idx")
          page-meta-wtr (meta/->page-meta-wtr (.structKeyWriter leaf-wtr "columns"))
          !page-idx (AtomicInteger. 0)]

      (reify ITrieWriter
        (getLeafWriter [_] leaf-rel-wtr)

        (writeLeaf [_]
          (.syncRowCount leaf-rel-wtr)

          (let [leaf-rdr (vw/rel-wtr->rdr leaf-rel-wtr)
                put-rdr (-> leaf-rdr
                            (.readerForName "op")
                            (.legReader :put)
                            (.metadataReader))

                doc-rdr (.structKeyReader put-rdr "xt$doc")]

            (.writeMetadata page-meta-wtr (into [(.readerForName leaf-rdr "xt$system_from")
                                                 (.readerForName leaf-rdr "xt$iid")
                                                 (.structKeyReader put-rdr "xt$valid_from")
                                                 (.structKeyReader put-rdr "xt$valid_to")]
                                                (map #(.structKeyReader doc-rdr %))
                                                (.structKeys doc-rdr))))

          (.writeBatch leaf-out-wtr)
          (.clear leaf-rel-wtr)
          (.clear leaf-vsr)

          (let [pos (.getPosition node-wp)]
            (.startStruct leaf-wtr)
            (.writeInt page-idx-wtr (.getAndIncrement !page-idx))
            (.endStruct leaf-wtr)
            (.endRow trie-rel-wtr)

            pos))

        (writeBranch [_ idxs]
          (let [pos (.getPosition node-wp)]
            (.startList branch-wtr)

            (dotimes [n (alength idxs)]
              (let [idx (aget idxs n)]
                (if (= idx -1)
                  (.writeNull branch-el-wtr nil)
                  (.writeInt branch-el-wtr idx))))

            (.endList branch-wtr)
            (.endRow trie-rel-wtr)

            pos))

        (end [_]
          (.end leaf-out-wtr)

          (.syncSchema trie-vsr)
          (.syncRowCount trie-rel-wtr)

          (util/with-open [trie-out-wtr (ArrowFileWriter. trie-vsr nil trie-out-ch)]
            (.start trie-out-wtr)
            (.writeBatch trie-out-wtr)
            (.end trie-out-wtr)))

        AutoCloseable
        (close [_]
          (util/close [trie-vsr leaf-out-wtr leaf-vsr]))))))

(defn write-live-trie [^ITrieWriter trie-wtr, ^LiveHashTrie trie, ^RelationReader leaf-rel]
  (let [trie (.compactLogs trie)
        copier (vw/->rel-copier (.getLeafWriter trie-wtr) leaf-rel)]
    (letfn [(write-node! [^HashTrie$Node node]
              (if-let [children (.children node)]
                (let [child-count (alength children)
                      !idxs (int-array child-count)]
                  (dotimes [n child-count]
                    (aset !idxs n
                          (unchecked-int
                           (if-let [child (aget children n)]
                             (write-node! child)
                             -1))))

                  (.writeBranch trie-wtr !idxs))

                (let [^LiveHashTrie$Leaf leaf node]
                  (-> (Arrays/stream (.data leaf))
                      (.forEach (reify IntConsumer
                                  (accept [_ idx]
                                    (.copyRow copier idx)))))

                  (.writeLeaf trie-wtr))))]

      (write-node! (.rootNode trie)))))

(defn live-trie->bufs [^BufferAllocator allocator, ^LiveHashTrie trie, ^RelationReader leaf-rel]
  (util/with-open [leaf-bb-ch (WritableByteBufferChannel/open)
                   trie-bb-ch (WritableByteBufferChannel/open)
                   trie-wtr (open-trie-writer allocator
                                              (Schema. (for [^IVectorReader rdr leaf-rel]
                                                         (.getField rdr)))
                                              (.getChannel leaf-bb-ch)
                                              (.getChannel trie-bb-ch))]

    (write-live-trie trie-wtr trie leaf-rel)

    (.end trie-wtr)

    {:leaf-buf (.getAsByteBuffer leaf-bb-ch)
     :trie-buf (.getAsByteBuffer trie-bb-ch)}))

(defn ->trie-key [^long level, ^long chunk-idx, ^long next-chunk-idx]
  (format "l%s-cf%s-ct%s" (util/->lex-hex-string level) (util/->lex-hex-string chunk-idx) (util/->lex-hex-string next-chunk-idx)))

(defn ->table-leaf-obj-key [table-name trie-key]
  (format "tables/%s/log-leaves/leaf-%s.arrow" table-name trie-key))

(defn ->table-trie-obj-key [table-name trie-key]
  (format "tables/%s/log-tries/trie-%s.arrow" table-name trie-key))

(defn write-trie-bufs! [^ObjectStore obj-store, ^String table-name, trie-key
                        {:keys [^ByteBuffer leaf-buf ^ByteBuffer trie-buf]}]
  (-> (.putObject obj-store (->table-leaf-obj-key table-name trie-key) leaf-buf)
      (util/then-compose
        (fn [_]
          (.putObject obj-store (->table-trie-obj-key table-name trie-key) trie-buf)))))

(defn parse-trie-filename [file-name]
  (when-let [[_ table-name trie-key level-str row-from-str row-to-str] (re-find #"tables/(.+)/log-tries/trie-(l(\p{XDigit}+)-cf(\p{XDigit}+)-ct(\p{XDigit}+)+?)\.arrow$" file-name)]
    {:table-name table-name
     :trie-file file-name
     :trie-key trie-key
     :level (util/<-lex-hex-string level-str)
     :row-from (util/<-lex-hex-string row-from-str)
     :row-to (util/<-lex-hex-string row-to-str)}))

(defn current-table-tries [trie-files]
  (loop [next-row 0
         [table-tries & more-levels] (->> trie-files
                                          (group-by :level)
                                          (sort-by key #(Long/compare %2 %1))
                                          (vals))
         res []]
    (if-not table-tries
      res
      (if-let [tries (not-empty
                      (->> table-tries
                           (into [] (drop-while (fn [{:keys [^long row-from]}]
                                                  (< row-from next-row))))))]
        (recur (long (:row-to (first (rseq tries)))) more-levels (into res tries))
        (recur next-row more-levels res)))))

(defn list-table-trie-files [^ObjectStore obj-store, table-name]
  (->> (sort (.listObjects obj-store (format "tables/%s/log-tries" table-name)))
       (into [] (keep parse-trie-filename))))

(defn postwalk-merge-tries
  "Post-walks the merged tries, passing the nodes from each of the tries to the given fn.
   e.g. for a leaf: passes the trie-nodes to the fn, returns the result.
        for a branch: passes a vector the return values of the postwalk fn
                      for the inner nodes, for the fn to combine

   Returns the value returned from the postwalk fn for the root node.

  tries :: [HashTrie]

  f :: path, merge-node -> ret
    where
    merge-node :: [:branch [ret]]
                | [:leaf [HashTrie$Node]]"
  [tries f]

  (letfn [(postwalk* [nodes path-vec]
            (let [trie-children (mapv #(some-> ^HashTrie$Node % (.children)) nodes)
                  path (byte-array path-vec)]
              (f path
                 (if-let [^objects first-children (some identity trie-children)]
                   [:branch (lazy-seq
                             (->> (range (alength first-children))
                                  (mapv (fn [bucket-idx]
                                          (postwalk* (mapv (fn [node ^objects node-children]
                                                             (if node-children
                                                               (aget node-children bucket-idx)
                                                               node))
                                                           nodes trie-children)
                                                     (conj path-vec bucket-idx))))))]

                   [:leaf nodes]))))]

    (postwalk* (mapv (fn [^HashTrie trie]
                       (some-> trie .rootNode))
                     tries)
               [])))

(defrecord ArrowTrie [table-trie, ^HashTrie trie, ^ArrowBuf buf, ^RelationReader trie-rdr]
  AutoCloseable
  (close [_]
    (util/close trie-rdr)
    (util/close buf)))

(defn open-arrow-trie-file [^IBufferPool buffer-pool {:keys [trie-file] :as table-trie}]
  (util/with-close-on-catch [^ArrowBuf buf @(.getBuffer buffer-pool trie-file)]
    (let [{:keys [^VectorLoader loader root arrow-blocks]} (util/read-arrow-buf buf)]
      (with-open [record-batch (util/->arrow-record-batch-view (first arrow-blocks) buf)]
        (.load loader record-batch)
        (->ArrowTrie table-trie (ArrowHashTrie/from root) buf (vr/<-root root))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILeafLoader
  (^org.apache.arrow.vector.types.pojo.Schema getSchema [])
  (^xtdb.vector.RelationReader loadLeaf [leaf]))

(deftype ArrowLeafLoader [^ArrowBuf buf
                          ^VectorSchemaRoot root
                          ^VectorLoader loader
                          ^List arrow-blocks
                          ^:unsynchronized-mutable ^int current-page-idx]
  ILeafLoader
  (getSchema [_] (.getSchema root))

  (loadLeaf [this leaf]
    (let [page-idx (.getPageIndex ^ArrowHashTrie$Leaf leaf)]
      (when-not (= page-idx current-page-idx)
        (set! (.current-page-idx this) page-idx)

        (with-open [rb (util/->arrow-record-batch-view (nth arrow-blocks page-idx) buf)]
          (.load loader rb))))

    (vr/<-root root))

  AutoCloseable
  (close [_]
    (util/close root)
    (util/close buf)))

(deftype LiveLeafLoader [^RelationReader live-rel]
  ILeafLoader
  (getSchema [_]
    (Schema. (for [^IVectorReader rdr live-rel]
               (.getField rdr))))

  (loadLeaf [_ leaf]
    (.select live-rel (.data ^LiveHashTrie$Leaf leaf)))

  AutoCloseable
  (close [_]))

(defn open-leaves [^IBufferPool buffer-pool, table-name, table-tries, ^ILiveTableWatermark live-table-wm]
  (util/with-close-on-catch [leaf-bufs (ArrayList.)]
    ;; TODO get hold of these a page at a time if it's a small query,
    ;; rather than assuming we'll always have/use the whole file.
    (let [arrow-leaves (->> table-tries
                            (mapv (fn [{:keys [trie-key]}]
                                    (.add leaf-bufs @(.getBuffer buffer-pool (->table-leaf-obj-key table-name trie-key)))
                                    (let [leaf-buf (.get leaf-bufs (dec (.size leaf-bufs)))
                                          {:keys [^VectorSchemaRoot root loader arrow-blocks]} (util/read-arrow-buf leaf-buf)]

                                      (ArrowLeafLoader. leaf-buf root loader arrow-blocks -1)))))]
      (cond-> arrow-leaves
        live-table-wm (conj (->LiveLeafLoader (.liveRelation live-table-wm)))))))

(defn load-leaves [leaf-loaders trie-leaves]
  (->> trie-leaves
       (into [] (map-indexed (fn [ordinal trie-leaf]
                               (when trie-leaf
                                 (let [^ILeafLoader leaf-loader (nth leaf-loaders ordinal)]
                                   (.loadLeaf leaf-loader trie-leaf))))))))

(defn ->merge-queue ^java.util.PriorityQueue []
  (PriorityQueue. (ILeafRow/comparator)))