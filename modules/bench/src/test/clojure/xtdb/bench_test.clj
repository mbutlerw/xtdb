(ns xtdb.bench-test
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.db-catalog :as db]
            [xtdb.table-catalog :as table-cat]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.io File)
           (xtdb.api.error Incorrect)
           (xtdb.block.proto TableBlock)
           (xtdb.log.proto TrieDetails)))

(defn- temp-log-file ^File []
  (doto (File/createTempFile "bench-test" ".log") (.deleteOnExit)))

(defn- run-benchmark! [log-file tasks]
  ((b/compile-benchmark {:title "test", :benchmark-type :test, :bench-log-file log-file, :tasks tasks}) nil))

(defn- stage-reports [log-file]
  (with-open [rdr (io/reader log-file)]
    (->> (line-seq rdr)
         (map #(json/read-str % :key-fn keyword))
         (into {} (map (juxt :stage identity))))))

(deftest verify-gets-the-stage-result-outside-the-stage-timing
  (let [log-file (temp-log-file)
        !verified (atom nil)]
    (run-benchmark! log-file [{:t :do, :stage :outer
                               :tasks [{:t :call, :stage :inner
                                        :f (fn [_] :result)
                                        :verify (fn [result]
                                                  (reset! !verified result)
                                                  (Thread/sleep 500))}]}])

    (t/is (= :result @!verified))

    (let [{:strs [inner outer]} (stage-reports log-file)]
      (t/is (< (:time-taken-ms inner) 500))
      (t/is (<= 500 (:time-taken-ms outer)) "an enclosing stage's time is wall clock, verify included"))))

(deftest a-failing-verify-fails-the-run-before-the-stage-reports
  (let [log-file (temp-log-file)]
    (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #"wrong"
                            (run-benchmark! log-file [{:t :call, :stage :inner
                                                       :f (fn [_] :result)
                                                       :verify (fn [_] (throw (ex-info "wrong" {})))}])))

    (t/is (not (contains? (stage-reports log-file) "inner")))))

(deftest verify-is-rejected-outside-a-staged-call
  (doseq [task [{:t :call, :f (fn [_] :result), :verify (fn [_])}
                {:t :do, :stage :outer, :tasks [], :verify (fn [_])}]]
    (t/is (thrown-with-msg? Incorrect #"needs a `:stage` and `:t :call`"
                            (b/compile-benchmark {:title "test", :benchmark-type :test, :tasks [task]}))
          (pr-str task))))

(deftest after-settle-the-latest-block-lists-only-compacted-tries
  (util/with-tmp-dirs #{node-dir}
    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 1})]
      (xt/execute-tx node [[:put-docs :foo {:xt/id 1}]])
      (b/flush-block! node)
      (b/compact! node)

      ((b/compile-benchmark {:title "test", :benchmark-type :test, :bench-log-file (temp-log-file)
                             :tasks [(b/settle-stage)]})
       node)

      (let [trie-keys (->> (.getByteArray (.getBufferPool (db/primary-db node))
                                          (util/->path "tables/public$foo/blocks/b01.binpb"))
                           TableBlock/parseFrom
                           table-cat/<-table-block
                           :partitions
                           (mapcat :tries)
                           (map #(.getTrieKey ^TrieDetails %)))]
        (t/is (seq trie-keys))
        (t/is (every? #(re-find #"^l01-" %) trie-keys)
              (str "expected only L1 tries, got " (vec trie-keys)))))))
