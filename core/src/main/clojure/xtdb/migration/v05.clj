(ns xtdb.migration.v05
  (:require [clojure.tools.logging :as log]
            xtdb.node.impl
            [xtdb.object-store :as os]
            [xtdb.serde :as serde]
            [xtdb.table-catalog :as table-cat]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import [java.nio ByteBuffer]
           [java.nio.channels ClosedByInterruptException]
           [java.nio.file Path]
           org.apache.arrow.memory.BufferAllocator
           (org.apache.arrow.vector.ipc ArrowFileReader)
           (org.apache.arrow.vector.types.pojo Field)
           xtdb.arrow.VectorReader
           xtdb.BufferPool
           xtdb.catalog.BlockCatalog
           (xtdb.indexer HllCalculator TrieMetadataCalculator)
           xtdb.trie.TrieCatalog))

(defn copy-file! [{^BufferPool src :xtdb.migration/source
                   ^BufferPool target :xtdb/buffer-pool}
                  ^Path src-path, ^Path target-path]
  (try
    (let [buf (ByteBuffer/wrap (.getByteArray src src-path))]
      (.putObject target target-path (.duplicate buf))
      buf)

    (catch InterruptedException e (throw e))
    (catch ClosedByInterruptException _ (throw (InterruptedException.)))
    (catch Throwable e
      (log/errorf e "error copying file: '%s' -> '%s" src-path target-path))))

(defn trie-details [{^BufferAllocator al :xtdb/allocator}, ^ByteBuffer data-buf]
  (with-open [file-reader (ArrowFileReader. (util/->seekable-byte-channel data-buf) al)
              root (.getVectorSchemaRoot file-reader)]
    (let [rdr (vr/<-root root)
          tm-calc (TrieMetadataCalculator. (VectorReader/from (.vectorForOrNull rdr "_iid"))
                                           (.vectorForOrNull rdr "_valid_from")
                                           (.vectorForOrNull rdr "_valid_to")
                                           (.vectorForOrNull rdr "_system_from"))
          hll-calc (HllCalculator.)
          !row-count (volatile! 0)]

      (while (.loadNextBatch file-reader)
        (let [row-count (.getRowCount root)]
          (vswap! !row-count + row-count)
          (.update tm-calc 0 row-count)
          (.update hll-calc (.vectorForOrNull rdr "op") 0 row-count)))

      {:trie-metadata (.build tm-calc)
       :hlls (.build hll-calc)
       :fields (-> (.vectorFor rdr "op") (.legReader "put")
                   (.getField) (.getChildren)
                   (->> (into {} (map (juxt #(.getName ^Field %) identity)))))
       :row-count @!row-count
       :data-file-size (.capacity data-buf)})))

(defn migrate->v06! [{^BufferPool src :xtdb.migration/source
                      ^BlockCatalog block-cat :xtdb/block-catalog
                      table-cat :xtdb/table-catalog
                      ^TrieCatalog trie-cat :xtdb/trie-catalog
                      :as system}]
  (let [chunk-meta-objs (.listAllObjects src (util/->path "chunk-metadata"))]
    #_(log/infof "%d blocks to migrate..." (count chunk-meta-objs))
    (dorun
     (->> chunk-meta-objs
          (map-indexed (fn [block-idx obj]
                         (log/debugf "Migrating block %d..." block-idx)
                         (when (Thread/interrupted) (throw (InterruptedException.)))

                         (let [{obj-key :key} (os/<-StoredObject obj)
                               [_ chunk-idx-hex] (re-matches #"chunk-metadata/(\p{XDigit}+)\.transit\.json" (str obj-key))
                               {:keys [tables latest-completed-tx next-chunk-idx]} (-> (.getByteArray src obj-key)
                                                                                       (serde/read-transit :json))
                               table-res (->> (for [[table-name {:keys [row-count]}] tables
                                                    :let [old-trie-key (format "log-l00-fr%s-nr%s-rs%s"
                                                                               chunk-idx-hex
                                                                               (util/->lex-hex-string next-chunk-idx)
                                                                               (Long/toString row-count 16))
                                                          table-path (trie/table-name->table-path table-name)
                                                          data-path (.resolve table-path "data")
                                                          meta-path (.resolve table-path "meta")
                                                          new-trie-key (trie/->l0-trie-key block-idx)]]
                                                (do
                                                  (log/debugf "Copying '%s' '%s' -> '%s'" table-name old-trie-key new-trie-key)
                                                  (copy-file! system
                                                              (.resolve meta-path (str old-trie-key ".arrow"))
                                                              (.resolve meta-path (str new-trie-key ".arrow")))

                                                  (let [buf (copy-file! system
                                                                        (.resolve data-path (str old-trie-key ".arrow"))
                                                                        (.resolve data-path (str new-trie-key ".arrow")))]
                                                    [table-name (into {:trie-key new-trie-key} (trie-details system buf))])))
                                              (into {}))]

                           (.addTries trie-cat (for [[table-name {:keys [trie-key data-file-size trie-metadata]}] table-res]
                                                 (trie/->trie-details table-name trie-key data-file-size trie-metadata)))

                           (log/debugf "Writing table-block files for block %d" block-idx)
                           (let [table-block-paths (table-cat/finish-block! table-cat block-idx table-res
                                                                            (->> (for [table-name (.getTableNames trie-cat)]
                                                                                   [table-name (->> (trie-cat/trie-state trie-cat table-name)
                                                                                                    trie-cat/all-tries)])
                                                                                 (into {})))]

                             (log/debugf "Writing block file for block %d" block-idx)
                             (.finishBlock block-cat block-idx latest-completed-tx table-block-paths)))))))))
