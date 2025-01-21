(ns xtdb.file-list
  (:require [cognitect.transit :as transit]
            [xtdb.util :as util])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.nio.file Path]
           (xtdb.api.log FileLog$Notification)))

(defrecord FileNotification [added deleted]
  FileLog$Notification)

;; used from Kotlin
#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn addition [k size]
  (FileNotification. [{:k k, :size size}] []))

(def ^:private transit-write-handlers
  {FileNotification (transit/write-handler "xtdb/file-notification" #(into {} %))
   Path (transit/write-handler "xtdb/path" (fn [^Path path]
                                             (str path)))})

(defn file-notification->transit [n]
  (with-open [os (ByteArrayOutputStream.)]
    (let [w (transit/writer os :msgpack {:handlers transit-write-handlers})]
      (transit/write w n))
    (.toByteArray os)))

(def ^:private transit-read-handlers
  {"xtdb/file-notification" (transit/read-handler map->FileNotification)
   "xtdb/path" (transit/read-handler (fn [path-str]
                                       (util/->path path-str)))})

(defn transit->file-notification [bytes]
  (with-open [in (ByteArrayInputStream. bytes)]
    (let [rdr (transit/reader in :msgpack {:handlers transit-read-handlers})]
      (transit/read rdr))))

(def ^:private trie-file-path-regex
  ;; e.g. `log-l01-fr0-nr12e-rs20.arrow` or `log-l04-p0010-nr12e.arrow`
  #"(log-l(\p{XDigit}+)(?:-p(\p{XDigit}+))?(?:-fr(\p{XDigit}+))?-nr(\p{XDigit}+)(?:-rs(\p{XDigit}+))?)\.arrow$")

(defn parse-trie-file-path [^Path file-path]
  (let [trie-key (str (.getFileName file-path))]
    (when-let [[_ trie-key level-str part-str first-row next-row-str rows-str] (re-find trie-file-path-regex trie-key)]
      (cond-> {:file-path file-path
               :trie-key trie-key
               :level (util/<-lex-hex-string level-str)
               :next-row (util/<-lex-hex-string next-row-str)}
        first-row (assoc :first-row (util/<-lex-hex-string first-row))
        part-str (assoc :part (byte-array (map #(Character/digit ^char % 4) part-str)))
        rows-str (assoc :rows (Long/parseLong rows-str 16))))))

(def branch-factor 4)

(defn- superseded-l0-file? [file-store-state {:keys [next-row]}]
  (or (when-let [{l0-next-row :next-row} (first (get file-store-state [0 []]))]
        (>= l0-next-row next-row))

      (when-let [{l1-next-row :next-row} (first (get file-store-state [1 []]))]
        (>= l1-next-row next-row))))

(defn- superseded-l1-file? [file-store-state {:keys [next-row first-row]}]
  (or (when-let [{l1-next-row :next-row, l1-first-row :first-row} (first (get file-store-state [1 []]))]
        (or (> l1-first-row first-row)
            (>= l1-next-row next-row)))

      (->> (map file-store-state (for [p (range branch-factor)]
                                   [2 [p]]))
           (every? (fn [{l2-next-row :next-row, :as l2-file}]
                     (and l2-file
                          (>= l2-next-row next-row)))))))

(defn superseded-ln-file [file-store-state {:keys [level next-row part]}]
  (or (when-let [{ln-next-row :next-row} (first (get file-store-state [level part]))]
        (>= ln-next-row next-row))

      (->> (map file-store-state (for [p (range branch-factor)]
                                   [(inc level) (conj part p)]))
           (every? (fn [{lnp1-next-row :next-row, :as lnp1-file}]
                     (when lnp1-file
                       (>= lnp1-next-row next-row)))))))

(defn- superseded-file? [file-store-state {:keys [level] :as file}]
  (case (long level)
    0 (superseded-l0-file? file-store-state file)
    1 (superseded-l1-file? file-store-state file)
    (superseded-ln-file file-store-state file)))

(defn- supersede-partial-l1-files [l1-files {:keys [first-row next-row]}]
  (->> l1-files
       (map (fn [{l1-first-row :first-row, l1-next-row :next-row, l1-state :state, :as l1-file}]
              (cond-> l1-file
                (and (= l1-state :live)
                     (= first-row l1-first-row)
                     (>= next-row l1-next-row))
                (assoc :state :garbage))))))

(defn- supersede-l0-files [l0-files {:keys [next-row]}]
  (->> l0-files
       (map (fn [{l0-next-row :next-row, :as l0-file}]
              (cond-> l0-file
                (<= l0-next-row next-row)
                (assoc :state :garbage))))))

(defn- conj-nascent-ln-file [file-store-state {:keys [level part] :as file}]
  (-> file-store-state
      (update [level part]
              (fnil (fn [ln-part-files]
                      (conj ln-part-files (assoc file :state :nascent)))
                    '()))))

(defn- completed-ln-group? [file-store-state {:keys [level next-row part]}]
  (->> (let [pop-part (pop part)]
         (for [p (range branch-factor)]
           [level (conj pop-part p)]))
       (map (comp first file-store-state))

       (every? (fn [{ln-next-row :next-row, ln-state :state, :as ln-file}]
                 (and ln-file
                      (or (> ln-next-row next-row)
                          (= :nascent ln-state)))))))

(defn- mark-ln-group-live [file-store-state {:keys [next-row level part]}]
  (reduce (fn [file-store-state part]
            (-> file-store-state
                (update part
                        (fn [ln-files]
                          (->> ln-files
                               (map (fn [{ln-state :state, ln-next-row :next-row, :as ln-file}]
                                      (cond-> ln-file
                                        (and (= ln-state :nascent)
                                             (= ln-next-row next-row))
                                        (assoc :state :live)))))))))
          file-store-state
          (let [pop-part (pop part)]
            (for [p (range branch-factor)]
              [level (conj pop-part p)]))))

(defn- supersede-lnm1-files [file-store-state {:keys [next-row level part]}]
  (-> file-store-state
      (update [(dec level) (pop part)]
              (fn [lnm1-files]
                (->> lnm1-files
                     (map (fn [{lnm1-state :state, lnm1-next-row :next-row, :as lnm1-file}]
                            (cond-> lnm1-file
                              (and (= lnm1-state :live)
                                   (<= lnm1-next-row next-row))
                              (assoc :state :garbage)))))))))

(defn- conj-file [file-store-state {:keys [level] :as file}]
  (case (long level)
    0 (-> file-store-state
          (update [0 []] (fnil conj '())
                  (assoc file :state :live)))

    1 (-> file-store-state
          (update [1 []] (fnil (fn [l1-files file]
                                 (-> l1-files
                                     (supersede-partial-l1-files file)
                                     (conj (assoc file :state :live))))
                               '())
                  file)
          (update [0 []] supersede-l0-files file))

    (as-> file-store-state file-store-state
      (conj-nascent-ln-file file-store-state file)

      (cond-> file-store-state
        (completed-ln-group? file-store-state file) (-> (mark-ln-group-live file)
                                                        (supersede-lnm1-files file))))))

(defn apply-file-notification [file-store-state file-name]
  (let [file (-> (parse-trie-file-path file-name)
                 (update :part vec))]
    (cond-> file-store-state
      (not (superseded-file? file-store-state file)) (conj-file file))))

(defn current-trie-files [file-names]
  (->> (sort file-names)
       (reduce apply-file-notification {})
       (into [] (comp (mapcat val)
                      (filter #(= (:state %) :live))))
       (sort-by (juxt (comp - :level) :part :next-row))
       #_(#(doto % clojure.pprint/pprint))
       (mapv :file-path)))

(comment
  (current-trie-files [(util/->path "log-l00-fr00-nr12e-rs20.arrow")
                       (util/->path "log-l00-fr12e-nr130-rs2.arrow")
                       (util/->path "log-l01-fr00-nr12e-rs20.arrow")
                       (util/->path "log-l01-fr00-nr130-rs22.arrow")
                       (util/->path "log-l02-p0-nr130.arrow")
                       (util/->path "log-l02-p1-nr130.arrow")
                       (util/->path "log-l02-p3-nr130.arrow")
                       (util/->path "log-l02-p2-nr130.arrow")
                       (util/->path "log-l03-p21-nr130.arrow")
                       (util/->path "log-l03-p22-nr130.arrow")
                       (util/->path "log-l03-p23-nr130.arrow")
                       (util/->path "log-l03-p20-nr130.arrow")]))
