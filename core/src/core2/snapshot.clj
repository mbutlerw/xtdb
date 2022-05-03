(ns core2.snapshot
  (:require core2.api
            [core2.expression.temporal :as expr.temp]
            [core2.indexer :as idx]
            [core2.operator.scan :as scan]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.api.TransactionInstant
           [core2.indexer IChunkManager TransactionIndexer]
           java.time.Duration
           [java.util.concurrent CompletableFuture TimeUnit]))

(definterface ISnapshot
  (^core2.ICursor scan [^org.apache.arrow.memory.BufferAllocator allocator
                        ^java.util.List #_<String> colNames,
                        metadataPred
                        ^java.util.Map #_#_<String, IRelationSelector> colPreds
                        ^longs temporalMinRange
                        ^longs temporalMaxRange]))

(definterface ISnapshotFactory
  (^java.util.concurrent.CompletableFuture #_<ISnapshot> snapshot [^core2.api.TransactionInstant tx]))

(deftype Snapshot [metadata-mgr temporal-mgr buffer-pool ^IChunkManager indexer,
                   ^TransactionInstant tx]
  ISnapshot
  (scan [_ allocator col-names metadata-pred col-preds temporal-min-range temporal-max-range]
    (when-let [tx-time (.tx-time tx)]
      (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                  '<= "_tx-time-start" tx-time)

      (when-not (or (contains? col-preds "_tx-time-start")
                    (contains? col-preds "_tx-time-end"))
        (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                    '> "_tx-time-end" tx-time)))

    (let [watermark (.getWatermark indexer)]
      (try
        (-> (scan/->scan-cursor allocator metadata-mgr temporal-mgr buffer-pool
                                watermark col-names metadata-pred col-preds
                                temporal-min-range temporal-max-range)
            (util/and-also-close watermark))
        (catch Throwable t
          (util/try-close watermark)
          (throw t))))))

(defmethod ig/prep-key ::snapshot-factory [_ opts]
  (merge {:indexer (ig/ref ::idx/indexer)
          :metadata-mgr (ig/ref :core2.metadata/metadata-manager)
          :temporal-mgr (ig/ref :core2.temporal/temporal-manager)
          :buffer-pool (ig/ref :core2.buffer-pool/buffer-pool)}
         opts))

(defmethod ig/init-key ::snapshot-factory [_ {:keys [^TransactionIndexer indexer metadata-mgr temporal-mgr buffer-pool]}]
  (reify
    ISnapshotFactory
    (snapshot [_ tx]
      (-> (if tx
            (.awaitTxAsync indexer tx)
            (CompletableFuture/completedFuture (.latestCompletedTx indexer)))
          (util/then-apply (fn [tx]
                             (Snapshot. metadata-mgr temporal-mgr buffer-pool indexer tx)))))))

(defn snapshot-async ^java.util.concurrent.CompletableFuture [^ISnapshotFactory snapshot-factory, tx]
  (-> (if-not (instance? CompletableFuture tx)
        (CompletableFuture/completedFuture tx)
        tx)
      (util/then-compose (fn [tx]
                           (.snapshot snapshot-factory tx)))))

(defn snapshot
  ([^ISnapshotFactory snapshot-factory]
   (snapshot snapshot-factory nil))

  ([^ISnapshotFactory snapshot-factory, tx]
   (snapshot snapshot-factory tx nil))

  ([^ISnapshotFactory snapshot-factory, tx, ^Duration timeout]
   @(-> (snapshot-async snapshot-factory tx)
        (cond-> timeout (.orTimeout (.toMillis timeout) TimeUnit/MILLISECONDS)))))