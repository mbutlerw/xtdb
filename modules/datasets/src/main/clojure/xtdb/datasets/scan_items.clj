(ns xtdb.datasets.scan-items
  (:require [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench.random :as random]))

(def ^:private langs ["EN" "ES" "DE" "FR" "IT" "PT" "PL" "NL"])

(defn- gen-id [random id-type i]
  (case id-type
    :uuid (random/next-uuid random)
    :string (str (random/next-uuid random))
    :keyword (keyword "item" (str i))))

(defn- gen-item [^java.util.Random random cat-pool name-pool id-type i]
  (let [categories (vec (repeatedly (random/next-int random 5) #(random/uniform-nth random cat-pool)))]
    (cond-> {:xt/id (gen-id random id-type i)
             :item/title (str "Item-" i)
             :item/excerpt (random/next-string random (+ 40 (random/next-int random 120)))
             :item/published-at (random/next-instant random 2020 2025)
             :item/length (+ 100 (random/next-int random 2000))
             :item/content-key (random/next-uuid random)
             :item/lang (random/uniform-nth random langs)
             :item/author-name (random/uniform-nth random name-pool)
             :item/author-email (str (random/next-string random 8) "@example.com")
             :item/author-url (str "https://example.com/author/" i)
             :item/categories categories
             :item/ingested-at (random/next-instant random 2020 2025)
             :item/url (random/next-string random (+ 10 (random/next-int random 50)))}
      (random/chance? random 0.001) (assoc :item/doc-type :item/direct
                                           :item.direct/candidate-status (random/weighted-nth random [0.956 0.035 0.008] [:approved :failed :ingest-failed])))))

(def ^:private batch-size 1000)

(defn load-data!
  "Generate and load `n-items` into the `items` table, streaming in batches to avoid holding
   the full dataset in memory."
  [conn ^java.util.Random random n-items id-type]
  (log/info "Generating" n-items "items with id-type" id-type)
  (let [cat-pool (vec (repeatedly 64 #(random/next-string random (+ 3 (random/next-int random 10)))))
        name-pool (vec (repeatedly 64 #(random/next-string random (+ 6 (random/next-int random 20)))))
        submit-batch! (fn [batch]
                        (when (seq batch)
                          (xt/submit-tx conn [(into [:put-docs :items] batch)])))]
    (loop [i 0, batch []]
      (if (>= i n-items)
        (submit-batch! batch)
        (let [batch (conj batch (gen-item random cat-pool name-pool id-type i))]
          (if (= (count batch) batch-size)
            (do (submit-batch! batch)
                (recur (inc i) []))
            (recur (inc i) batch)))))))
