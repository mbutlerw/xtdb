(ns xtdb.file-list-test
  (:require [clojure.test :as t]
            [xtdb.file-list :as fl]
            [xtdb.trie :as trie]
            [xtdb.util :as util]))

(t/deftest parses-trie-paths
  (letfn [(parse [trie-key]
            (-> (fl/parse-trie-file-path (util/->path (str trie-key ".arrow")))
                (update :part #(some-> % vec))
                (mapv [:level :part :first-row :next-row :rows])))]
    (t/is (= [0 nil 22 46 32] (parse (trie/->log-l0-l1-trie-key 0 22 46 32))))
    (t/is (= [2 [0 0 1 3] nil 120 nil] (parse (trie/->log-l2+-trie-key 2 (byte-array [0 0 1 3]) 120))))))

(defn ->trie-file-name
  " L0/L1 keys are submitted as [level first-row next-row rows]; L2+ as [level part-vec next-row]"
  [[level & args]]

  (case (long level)
    (0 1) (let [[first-row next-row rows] args]
            (util/->path (str (trie/->log-l0-l1-trie-key level first-row next-row (or rows 0)) ".arrow")))

    (let [[part next-row] args]
      (util/->path (str (trie/->log-l2+-trie-key level (byte-array part) next-row) ".arrow")))))

(t/deftest test-selects-current-tries
  (letfn [(f [trie-keys]
            (->> (fl/current-trie-files (map ->trie-file-name trie-keys))
                 (mapv (comp (juxt :level (comp #(some-> % vec) :part) :next-row)
                             fl/parse-trie-file-path))))]

    (t/is (= [] (f [])))

    (t/testing "L0/L1 only"
      (t/is (= [[0 nil 1] [0 nil 2] [0 nil 3]]
               (f [[0 0 1 1] [0 1 2 1] [0 2 3 1]])))

      (t/is (= [[1 nil 2] [0 nil 3]]
               (f [[1 0 2 2] [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L1 file supersedes two L0 files")

      (t/is (= [[1 nil 3] [1 nil 4] [0 nil 5]]
               (f [[0 0 1 1] [0 1 2 1] [0 2 3 1] [0 3 4 1] [0 4 5 1]
                   [1 0 1 1] [1 0 2 2] [1 0 3 3] [1 3 4 1]]))
            "Superseded L1 files should not get returned"))

    (t/testing "L2"
      (t/is (= [[1 nil 2]]
               (f [[0 0 2 2] [1 0 2 2]
                   [2 [0] 2] [2 [3] 2]]))
            "L2 file doesn't supersede because not all parts complete")

      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]
               (f [[0 0 2 2] [1 0 2 2]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]))
            "now the L2 file is complete")

      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2] [0 nil 3]]
               (f [[0 0 1 1] [0 1 2 1] [0 2 3 1]
                   [1 0 2 2]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]))
            "L2 file supersedes L1, L1 supersedes L0, left with a single L0 file"))

    (t/testing "L3+"
      (t/is (= [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                ;; L2 path 0 covered
                [2 [1] 2] [2 [2] 2] [2 [3] 2]
                [0 nil 3]]

               (f [[0 0 1 1] [0 1 2 1] [0 2 3 1]
                   [1 0 2 2]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]

                   ;; L2 path 1 not covered yet, missing [1 1]
                   [3 [1 0] 2] [3 [1 2] 2] [3 [1 3] 2]]))

            "L3 covered idx 0 but not 1")

      (t/is (= [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                [3 [0 0] 2] [3 [0 2] 2] [3 [0 3] 2] ; L3 path [0 1] covered
                [2 [1] 2] [2 [2] 2] [2 [3] 2] ; L2 path 0 covered
                [0 nil 3]]

               (f [[0 0 1 1] [0 1 2 1] [0 2 3 1]
                   [1 0 2 2]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 2] 2] [3 [1 3] 2] ; L2 path 1 not covered yet, missing [1 1]
                   [4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]]))
            "L4 covers L3 path [0 1]"))))
