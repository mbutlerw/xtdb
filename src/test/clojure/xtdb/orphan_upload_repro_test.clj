(ns xtdb.orphan-upload-repro-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.buffer-pool :as bp]
            [xtdb.db-catalog :as db]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import (xtdb.api.storage SharedObjectStore)
           (java.nio.file Path)))

(def ^:dynamic ^SharedObjectStore *store* nil)

(defmethod bp/->object-store-factory ::shared [_ _] *store*)

(defn- start ^xtdb.api.Xtdb [^Path log-path epoch ^Path cache-path]
  (xtn/start-node {:log [:local {:path log-path, :epoch epoch}]
                   :storage [:remote {:object-store [::shared {}]}]
                   :disk-cache {:path cache-path}
                   :compactor {:threads 0}}))

(defn- flush! [node]
  (xt-log/send-flush-block-msg! (db/primary-db node))
  (xt-log/sync-node node #xt/duration "PT10S"))

(defn- stored-keys []
  (->> (.keySet (.getObjects *store*)) (map str) sort vec))

(t/deftest repro-i10-orphaned-block-upload-reused-by-new-history
  (util/with-tmp-dirs #{dir}
    (binding [*store* (SharedObjectStore.)]
      (let [log-1 (.resolve dir "log-1")
            log-2 (.resolve dir "log-2")
            q ["SELECT _id, v FROM docs ORDER BY _id"]]

        ;; history 1: block b00 recorded, then block b01's trie uploads land but its block file doesn't
        (let [first-node-close
              (try
                (with-open [node (start log-1 0 (.resolve dir "cache-1"))]
                  (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1, v: 'h1-recorded'}"])
                  (flush! node)
                  (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 2, v: 'h1-orphaned'}"])
                  (.setFailPut *store* (fn [^Path k] (= "blocks/b01.binpb" (str k))))
                  (try (flush! node) (catch Throwable e {:flush-error (ex-message e)})))
                (catch Throwable e {:close-error (ex-message e)}))]
          (.setFailPut *store* (fn [_] false))

          (let [after-h1 (stored-keys)]

            (let [writer-read (with-open [node (start log-2 1 (.resolve dir "cache-2"))]
                                (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 3, v: 'h2'}"])
                                (flush! node)
                                (jdbc/execute! node q))
                  ;; a node with no disk cache of its own reads b01 from the store
                  fresh-read (try
                               (with-open [node (start log-2 1 (.resolve dir "cache-3"))]
                                 (xt-log/sync-node node #xt/duration "PT10S")
                                 (jdbc/execute! node q))
                               (catch Throwable e {:error (ex-message e)}))]
              (prn :i10 {:h1-flush first-node-close
                         :stored-after-h1 after-h1
                         :stored-after-h2 (stored-keys)
                         :writer-read writer-read
                         :fresh-read fresh-read})
              (t/is (= [{:_id 1, :v "h1-recorded"} {:_id 3, :v "h2"}] writer-read)
                    "the writing node reads its own history")
              (t/is (= [{:_id 1, :v "h1-recorded"} {:_id 3, :v "h2"}] fresh-read)
                    "a node reading from the store sees the same history"))))))))
