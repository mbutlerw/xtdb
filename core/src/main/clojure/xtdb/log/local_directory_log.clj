(ns xtdb.log.local-directory-log
  (:require [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.log :as xt.log]
            [xtdb.node :as xtn]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import clojure.lang.MapEntry
           (java.io BufferedInputStream BufferedOutputStream DataInputStream DataOutputStream EOFException)
           java.nio.ByteBuffer
           (java.nio.channels Channels ClosedByInterruptException FileChannel)
           (java.nio.file Path)
           (java.time InstantSource)
           java.time.temporal.ChronoUnit
           java.util.ArrayList
           (java.util.concurrent ArrayBlockingQueue BlockingQueue CompletableFuture ExecutorService Executors Future)
           [java.util.concurrent.atomic AtomicLong]
           (xtdb.api Xtdb$Config)
           (xtdb.api.log FileLog Log Logs Logs$LocalLogFactory TxLog$Record)
           (xtdb.log INotifyingSubscriberHandler)))

(def ^:private ^{:tag 'byte} record-separator 0x1E)
(def ^:private ^{:tag 'long} header-size (+ Byte/BYTES Integer/BYTES Long/BYTES))
(def ^:private ^{:tag 'long} footer-size Long/BYTES)

(deftype LocalDirectoryLog [^Path root-path, ^INotifyingSubscriberHandler subscriber-handler
                            ^ExecutorService pool, ^BlockingQueue queue, ^Future append-loop-future
                            ^:volatile-mutable ^FileChannel log-channel
                            ^FileLog file-log
                            ^AtomicLong !latest-submitted-tx-id]
  Log
  (latestSubmittedTxId [_] (.get !latest-submitted-tx-id))

  (readTxs [_ after-offset limit]
    (when-not log-channel
      (let [log-path (.resolve root-path "LOG")]
        (when (util/path-exists log-path)
          (set! log-channel (util/->file-channel log-path)))))

    (when log-channel
      (let [log-in (DataInputStream. (BufferedInputStream. (Channels/newInputStream log-channel)))]
        (.position log-channel (long (or after-offset 0)))
        (loop [limit (int (if after-offset
                            (inc limit)
                            limit))
               acc []
               offset (.position log-channel)]
          (if (or (zero? limit) (= offset (.size log-channel)))
            (if after-offset
              (subvec acc 1)
              acc)
            (if-let [record (try
                              (when-not (= record-separator (.read log-in))
                                (throw (IllegalStateException. "invalid record")))
                              (let [size (.readInt log-in)
                                    msg-ts (time/micros->instant (.readLong log-in))
                                    record (byte-array size)
                                    read-bytes (.read log-in record)
                                    offset-check (.readLong log-in)]
                                (when (and (= size read-bytes)
                                           (= offset-check offset))
                                  (TxLog$Record. offset msg-ts (ByteBuffer/wrap record))))
                              (catch EOFException _))]
              (recur (dec limit)
                     (conj acc record)
                     (+ offset header-size (.capacity ^ByteBuffer (.getRecord ^TxLog$Record record)) footer-size))
              (if after-offset
                (subvec acc 1)
                acc)))))))

  (appendTx [_ record]
    (if (.isShutdown pool)
      (throw (IllegalStateException. "writer is closed"))
      (let [f (CompletableFuture.)]
        (.put queue (MapEntry/create f record))
        f)))

  (subscribeTxs [this after-tx-id subscriber]
    (.subscribe subscriber-handler this after-tx-id subscriber))

  (appendFileNotification [_ n] (.appendFileNotification file-log n))
  (subscribeFileNotifications [_ subscriber] (.subscribeFileNotifications file-log subscriber))

  (close [_]
    (when log-channel
      (.close log-channel))

    (try
      (future-cancel append-loop-future)
      (util/shutdown-pool pool)
      (finally
        (loop []
          (when-let [[^CompletableFuture f] (.poll queue)]
            (when-not (.isDone f)
              (.cancel f true))
            (recur)))))))

(defn- writer-append-loop [^Path root-path, ^BlockingQueue queue, ^InstantSource instant-src,
                           {:keys [^long buffer-size
                                   ^INotifyingSubscriberHandler subscriber-handler
                                   ^AtomicLong !latest-submitted-tx-id]}]
  (with-open [log-channel (util/->file-channel (.resolve root-path "LOG") #{:create :write})]
    (let [elements (ArrayList. buffer-size)]
      (.position log-channel (.size log-channel))
      (while (not (Thread/interrupted))
        (try
          (when-let [element (.take queue)]
            (.add elements element)
            (.drainTo queue elements (.size queue))
            (let [previous-offset (.position log-channel)
                  log-out (DataOutputStream. (BufferedOutputStream. (Channels/newOutputStream log-channel)))]
              (try
                (loop [n (int 0)
                       offset previous-offset]
                  (when-not (= n (.size elements))
                    (let [[f ^ByteBuffer record] (.get elements n)
                          msg-ts (-> (.instant instant-src) (.truncatedTo ChronoUnit/MICROS))
                          size (.remaining record)
                          written-record (.duplicate record)]
                      (.write log-out ^byte record-separator)
                      (.writeInt log-out size)
                      (.writeLong log-out (time/instant->micros msg-ts))
                      (while (>= (.remaining written-record) Long/BYTES)
                        (.writeLong log-out (.getLong written-record)))
                      (while (.hasRemaining written-record)
                        (.write log-out (.get written-record)))
                      (.writeLong log-out offset)
                      (.set elements n (MapEntry/create f (TxLog$Record. offset msg-ts record)))
                      (recur (inc n) (+ offset header-size size footer-size)))))
                (catch Throwable t
                  (.truncate log-channel previous-offset)
                  (throw t)))
              (.flush log-out)
              (.force log-channel true)
              (doseq [[^CompletableFuture f, ^TxLog$Record log-record] elements]
                (let [tx-id (.getTxId log-record)]
                  (.set !latest-submitted-tx-id tx-id)
                  (.notifyTx subscriber-handler tx-id)
                  (.complete f tx-id)))))
          (catch ClosedByInterruptException e
            (log/warn e "channel interrupted while closing")
            (doseq [[^CompletableFuture f] elements
                    :when (not (.isDone f))]
              (.cancel f true)))
          (catch InterruptedException _
            (doseq [[^CompletableFuture f] elements
                    :when (not (.isDone f))]
              (.cancel f true))
            (.interrupt (Thread/currentThread)))
          (catch Throwable t
            (log/error t "failed appending to log")
            (doseq [[^CompletableFuture f] elements
                    :when (not (.isDone f))]
              (.completeExceptionally f t)))
          (finally
            (.clear elements)))))))

(defmethod xtn/apply-config! :xtdb.log/local-directory-log [^Xtdb$Config config _ {:keys [path instant-src buffer-size poll-sleep-duration]}]
  (doto config
    (.setTxLog (cond-> (Logs/localLog (util/->path path))
                 instant-src (.instantSource instant-src)
                 buffer-size (.bufferSize buffer-size)
                 poll-sleep-duration (.pollSleepDuration (time/->duration poll-sleep-duration))))))

(defn- latest-submitted-tx-id ^long [^Path root-path]
  (let [log-path (.resolve root-path "LOG")]
    (if-not (util/path-exists log-path)
      -1
      (with-open [log-channel (util/->file-channel log-path)]
        (let [size (.size log-channel)]
          (if (zero? size)
            -1
            (try
              (.position log-channel (- size Long/BYTES))
              (let [log-in (DataInputStream. (BufferedInputStream. (Channels/newInputStream log-channel)))
                    offset (.readLong log-in)]
                (.position log-channel offset)
                (let [read-record-seperator (.read log-in)
                      record-size (.readInt log-in)]
                  (if (and (= record-separator read-record-seperator)
                           ;; offset + record-seperator + record size + system time + record + start of whole record
                           (= size (+ offset 1 Integer/BYTES Long/BYTES record-size Long/BYTES)))
                    ;; offset is the latest-submitted-tx
                    offset
                    ;; o/w the log file channel is somehow corrupted
                    (throw (IllegalArgumentException. "LOG file corrupted!")))))
              (catch IllegalArgumentException e
                (let [iae (IllegalArgumentException. "LOG file corrupted!")]
                  (.addSuppressed iae e)
                  (throw e))))))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-log [^Logs$LocalLogFactory factory]
  (let [root-path (.getPath factory)
        buffer-size (.getBufferSize factory)]
    (util/mkdirs root-path)

    (let [pool (Executors/newSingleThreadExecutor (util/->prefix-thread-factory "local-directory-log-writer-"))
          queue (ArrayBlockingQueue. buffer-size)
          latest-submitted-tx-id (latest-submitted-tx-id root-path)
          !latest-submitted-tx-id (AtomicLong. latest-submitted-tx-id)
          subscriber-handler (xt.log/->notifying-subscriber-handler latest-submitted-tx-id)
          append-loop-future (.submit pool ^Runnable #(writer-append-loop root-path queue (.getInstantSource factory)
                                                                          {:buffer-size buffer-size
                                                                           :subscriber-handler subscriber-handler
                                                                           :!latest-submitted-tx-id !latest-submitted-tx-id}))]
      (->LocalDirectoryLog root-path subscriber-handler pool queue append-loop-future nil FileLog/SOLO !latest-submitted-tx-id))))
