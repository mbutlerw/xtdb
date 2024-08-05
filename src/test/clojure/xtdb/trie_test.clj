(ns xtdb.trie-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.operator.scan :as scan]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw]
            [xtdb.buffer-pool :as bp])
  (:import (java.nio.file Paths)
           (org.apache.arrow.memory RootAllocator)
           (org.apache.arrow.vector.types.pojo Field)
           org.apache.arrow.vector.VectorSchemaRoot
           (xtdb ICursor)
           xtdb.arrow.Relation
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf HashTrieKt MergePlanNode MergePlanTask)
           (xtdb.util TemporalBounds)
           (xtdb.vector RelationWriter)))

(t/use-fixtures :each tu/with-allocator)

(deftest parses-trie-paths
  (letfn [(parse [trie-key]
            (-> (trie/parse-trie-file-path (util/->path (str trie-key ".arrow")))
                (update :part #(some-> % vec))
                (mapv [:level :part :first-row :next-row :rows])))]
    (t/is (= [0 nil 22 46 32] (parse (trie/->log-l0-l1-trie-key 0 22 46 32))))
    (t/is (= [2 [0 0 1 3] nil 120 nil] (parse (trie/->log-l2+-trie-key 2 (byte-array [0 0 1 3]) 120))))))

(defn- ->arrow-hash-trie [^Relation meta-rel]
  (ArrowHashTrie. (.get meta-rel "nodes")))

(defn- merge-plan-nodes->path+pages [mp-nodes]
  (->> mp-nodes
       (map (fn [^MergePlanTask merge-plan-node]
              (let [path (.getPath merge-plan-node)
                    mp-nodes (.getMpNodes merge-plan-node)]
                {:path (vec path)
                 :pages (mapv (fn [^MergePlanNode merge-plan-node]
                                (let [segment (.getSegment merge-plan-node)
                                      ^ArrowHashTrie$Leaf node (.getNode merge-plan-node)]
                                  {:seg (:seg segment), :page-idx (.getDataPageIndex node)}))
                              mp-nodes)})))
       (sort-by :path)))

(deftest test-merge-plan-with-nil-nodes-2700
  (with-open [al (RootAllocator.)
              t1-rel (tu/open-arrow-hash-trie-rel al [{Long/MAX_VALUE [nil 0 nil 1]} 2 nil
                                                      {(time/instant->micros (time/->instant #inst "2023-01-01")) 3
                                                       Long/MAX_VALUE 4}])
              log-rel (tu/open-arrow-hash-trie-rel al 0)
              log2-rel (tu/open-arrow-hash-trie-rel al [nil nil 0 1])]

    (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2} {:seg :log, :page-idx 0}]}
              {:path [2], :pages [{:seg :log, :page-idx 0} {:seg :log2, :page-idx 0}]}
              {:path [3], :pages [{:page-idx 3, :seg :t1} {:page-idx 4, :seg :t1} {:page-idx 0, :seg :log} {:page-idx 1, :seg :log2}]}
              {:path [0 0], :pages [{:seg :log, :page-idx 0}]}
              {:path [0 1], :pages [{:seg :t1, :page-idx 0} {:seg :log, :page-idx 0}]}
              {:path [0 2], :pages [{:seg :log, :page-idx 0}]}
              {:path [0 3], :pages [{:seg :t1, :page-idx 1} {:seg :log, :page-idx 0} ]}]
             (->> (HashTrieKt/toMergePlan [(-> (trie/->Segment (->arrow-hash-trie t1-rel))
                                               (assoc :seg :t1))
                                           (-> (trie/->Segment (->arrow-hash-trie log-rel))
                                               (assoc :seg :log))
                                           (-> (trie/->Segment (->arrow-hash-trie log2-rel))
                                               (assoc :seg :log2))]
                                          nil
                                          (TemporalBounds.))
                  (merge-plan-nodes->path+pages))))))

(deftest test-merge-plan-recency-filtering
  (with-open [al (RootAllocator.)
              t1-rel (tu/open-arrow-hash-trie-rel al [{Long/MAX_VALUE [nil 0 nil 1]}
                                                      {Long/MAX_VALUE 2}
                                                      nil
                                                      {(time/instant->micros (time/->instant #inst "2020-01-01")) 3
                                                       Long/MAX_VALUE 4}])
              t2-rel (tu/open-arrow-hash-trie-rel al [{(time/instant->micros (time/->instant #inst "2019-01-01")) 0
                                                       (time/instant->micros (time/->instant #inst "2020-01-01")) 1
                                                       Long/MAX_VALUE [nil 2 nil 3]}
                                                      nil
                                                      nil
                                                      {(time/instant->micros (time/->instant #inst "2020-01-01")) 4
                                                       Long/MAX_VALUE 5}])]
    (t/testing "pages 3 of t1 and 0,1 and 4 of t2 should not make it to the output"
      ;; setting up bounds for a current-time 2020-01-01
      (let [temporal-bounds (TemporalBounds.)
            current-time (time/instant->micros (time/->instant #inst "2020-01-01"))]
        (.lte (.getSystemFrom temporal-bounds) current-time)
        (.gt (.getSystemTo temporal-bounds) current-time)
        (.lte (.getValidFrom temporal-bounds) current-time)
        (.gt (.getValidTo temporal-bounds) current-time)


        (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2}]}
                  {:path [3], :pages [{:seg :t1, :page-idx 4} {:seg :t2, :page-idx 5}]}
                  {:path [0 1],
                   :pages [{:seg :t1, :page-idx 0} {:seg :t2, :page-idx 2}]}
                  {:path [0 3],
                   :pages [{:seg :t1, :page-idx 1} {:seg :t2, :page-idx 3}]}]
                 (->> (HashTrieKt/toMergePlan [(-> (trie/->Segment (->arrow-hash-trie t1-rel))
                                                   (assoc :seg :t1))
                                               (-> (trie/->Segment (->arrow-hash-trie t2-rel))
                                                   (assoc :seg :t2))]
                                              nil
                                              temporal-bounds)
                      (merge-plan-nodes->path+pages))))))

    (t/testing "going one chronon below should bring in pages 3 of t1 and pages 1 and 4 of t2"
      (let [temporal-bounds (TemporalBounds.)
            current-time (- (time/instant->micros (time/->instant #inst "2020-01-01")) 1) ]
        (.lte (.getSystemFrom temporal-bounds) current-time)
        (.gt (.getSystemTo temporal-bounds) current-time)
        (.lte (.getValidFrom temporal-bounds) current-time)
        (.gt (.getValidTo temporal-bounds) current-time)


        (t/is (=
               [{:path [1], :pages [{:seg :t1, :page-idx 2}]}
                {:path [3],
                 :pages [{:seg :t1, :page-idx 3} {:seg :t1, :page-idx 4} {:seg :t2, :page-idx 4} {:seg :t2, :page-idx 5}]}
                {:path [0 0],
                 :pages [{:seg :t2, :page-idx 1}]}
                {:path [0 1],
                 :pages [{:seg :t1, :page-idx 0} {:seg :t2, :page-idx 1} {:seg :t2, :page-idx 2}]}
                {:path [0 2],
                 :pages [{:seg :t2, :page-idx 1}]}
                {:path [0 3],
                 :pages [{:seg :t1, :page-idx 1} {:seg :t2, :page-idx 1} {:seg :t2, :page-idx 3}]}]
               (->> (HashTrieKt/toMergePlan [(-> (trie/->Segment (->arrow-hash-trie t1-rel))
                                                 (assoc :seg :t1))
                                             (-> (trie/->Segment (->arrow-hash-trie t2-rel))
                                                 (assoc :seg :t2))]
                                            nil
                                            temporal-bounds)
                    (merge-plan-nodes->path+pages))))))))

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
            (->> (trie/current-trie-files (map ->trie-file-name trie-keys))
                 (mapv (comp (juxt :level (comp #(some-> % vec) :part) :next-row)
                             trie/parse-trie-file-path))))]

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
               (f [[2 [0] 2] [2 [3] 2] [1 0 2 2]]))
            "L2 file doesn't supersede because not all parts complete")

      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]
               (f [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 0 2 2]]))
            "now the L2 file is complete")

      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2] [0 nil 3]]
               (f [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 0 2 2]
                   [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L2 file supersedes L1, L1 supersedes L0, left with a single L0 file"))

    (t/testing "L3+"
      (t/is (= [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                [2 [1] 2] [2 [2] 2] [2 [3] 2] ; L2 path 0 covered
                [0 nil 3]]
               (f [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 2] 2] [3 [1 3] 2] ; L2 path 1 not covered yet, missing [1 1]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 0 2 2]
                   [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L3 covered idx 0 but not 1")

      (t/is (= [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                [3 [0 0] 2] [3 [0 2] 2] [3 [0 3] 2] ; L3 path [0 1] covered
                [2 [1] 2] [2 [2] 2] [2 [3] 2] ; L2 path 0 covered
                [0 nil 3]]

               (f [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                   [3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 2] 2] [3 [1 3] 2] ; L2 path 1 not covered yet, missing [1 1]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 0 2 2]
                   [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L4 covers L3 path [0 1]")

      (t/is (= [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                [3 [0 0] 2] [3 [0 2] 2] [3 [0 3] 2] ; L3 path [0 1] covered
                [2 [1] 2] [2 [2] 2] [2 [3] 2] ; L2 path 0 covered even though [0 1] GC'd
                [0 nil 3]]

               (f [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                   [3 [0 0] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 2] 2] [3 [1 3] 2] ; L2 path 1 not covered yet, missing [1 1]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 0 2 2]
                   [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L4 covers L3 path [0 1], L3 [0 1] GC'd, still correctly covered"))

    (t/testing "empty levels"
      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2] [0 nil 3]]
               (f [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L1 empty")

      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]
               (f [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]))
            "L1 and L0 empty")

      (t/is (= [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                [3 [1 0] 2] [3 [1 1] 2] [3 [1 2] 2] [3 [1 3] 2]
                [3 [2 0] 2] [3 [2 1] 2] [3 [2 2] 2] [3 [2 3] 2]
                [3 [3 0] 2] [3 [3 1] 2] [3 [3 2] 2] [3 [3 3] 2]
                [0 nil 3]]
               (f [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 1] 2] [3 [1 2] 2] [3 [1 3] 2]
                   [3 [2 0] 2] [3 [2 1] 2] [3 [2 2] 2] [3 [2 3] 2]
                   [3 [3 0] 2] [3 [3 1] 2] [3 [3 2] 2] [3 [3 3] 2]
                   [1 0 2 2]
                   [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L2 missing, still covers L1"))))

(deftest test-data-file-writing
  (let [page-idx->documents
        {0 [[:put {:xt/id #uuid "00000000-0000-0000-0000-000000000000"
                   :foo "bar"}]
            [:put {:xt/id #uuid "00100000-0000-0000-0000-000000000000"
                   :foo "bar"}]]
         1 [[:put {:xt/id #uuid "01000000-0000-0000-0000-000000000000"
                   :foo "bar"}]]}]
    (util/with-tmp-dirs #{tmp-dir}
      (let [data-file-path (.resolve tmp-dir "data-file.arrow")]

        (tu/write-arrow-data-file tu/*allocator* page-idx->documents data-file-path)

        (tj/check-json (.toPath (io/as-file (io/resource "xtdb/trie-test/data-file-writing")))
                       tmp-dir)))))

(defn- str->path [s]
  (Paths/get s, (make-array String 0)))

(deftest test-trie-cursor-with-multiple-recency-nodes-from-same-file-3298
  (let [eid #uuid "00000000-0000-0000-0000-000000000000"]
    (util/with-tmp-dirs #{tmp-dir}
      (with-open [al (RootAllocator.)
                  iid-arrow-buf (util/->arrow-buf-view al (trie/->iid eid))
                  t1-rel (tu/open-arrow-hash-trie-rel al [{(time/instant->micros (time/->instant #inst "2023-01-01"))
                                                           [0 nil nil 1]
                                                           Long/MAX_VALUE [2 nil nil 3]}
                                                          nil nil 4])
                  t2-rel (tu/open-arrow-hash-trie-rel al [0 nil nil nil])]
        (let [id #uuid "00000000-0000-0000-0000-000000000000"
              t1-data-file "t1-data-file.arrow"
              t2-data-file "t2-data-file.arrow"]


          (tu/write-arrow-data-file al
                                    {0 [[:put {:xt/id id :foo "bar2" :xt/system-from 2}]
                                        [:put {:xt/id id :foo "bar1" :xt/system-from 1}]]
                                     1 []
                                     2 [[:put {:xt/id id :foo "bar0" :xt/system-from 0}]]
                                     3 []
                                     4 []}
                                    (.resolve tmp-dir t1-data-file))

          (tu/write-arrow-data-file al
                                    {0 [[:put {:xt/id id :foo "bar3" :xt/system-from 3}]]}
                                    (.resolve tmp-dir t2-data-file))

          (with-open [buffer-pool (bp/dir->buffer-pool al tmp-dir)]

            (let [arrow-hash-trie1 (->arrow-hash-trie t1-rel)
                  arrow-hash-trie2 (->arrow-hash-trie t2-rel)
                  seg-t1 (-> (trie/->Segment arrow-hash-trie1)
                             (assoc :data-file-path (str->path t1-data-file)))
                  seg-t2 (-> (trie/->Segment arrow-hash-trie2)
                             (assoc :data-file-path (str->path t2-data-file)))
                  leaves [[:arrow seg-t1 0] [:arrow seg-t1 2] [:arrow seg-t2 0]]
                  merge-tasks [{:leaves leaves :path (byte-array [0 0])} ]]
              (util/with-close-on-catch [out-rel (RelationWriter. al (for [^Field field
                                                                           [(types/->field "xt$id" (types/->arrow-type :uuid) false)
                                                                            (types/->field "foo" (types/->arrow-type :utf8) false)]]
                                                                       (vw/->writer (.createVector field al))))]

                (let [^ICursor trie-cursor (scan/->TrieCursor al (.iterator ^Iterable merge-tasks) out-rel
                                                              ["xt$id" "foo"] {}
                                                              (TemporalBounds.)
                                                              {} nil
                                                              (scan/->vsr-cache buffer-pool al)
                                                              buffer-pool)]

                  (t/is (= [[{:xt/id #uuid "00000000-0000-0000-0000-000000000000", :foo "bar3"}
                             {:xt/id #uuid "00000000-0000-0000-0000-000000000000", :foo "bar2"}
                             {:xt/id #uuid "00000000-0000-0000-0000-000000000000", :foo "bar1"}
                             {:xt/id #uuid "00000000-0000-0000-0000-000000000000", :foo "bar0"}]]
                           (tu/<-cursor trie-cursor)))
                  (.close trie-cursor))

                (t/is (= [{:path [0 0],
                           :pages [{:seg :t1, :page-idx 0}
                                   {:seg :t1, :page-idx 2}
                                   {:seg :t2, :page-idx 0}]}]
                         (->> (HashTrieKt/toMergePlan [(-> (trie/->Segment arrow-hash-trie1) (assoc :seg :t1))
                                                       (-> (trie/->Segment arrow-hash-trie2) (assoc :seg :t2))]
                                                      (scan/->path-pred iid-arrow-buf)
                                                      (TemporalBounds.))
                              (merge-plan-nodes->path+pages))))))))))))
