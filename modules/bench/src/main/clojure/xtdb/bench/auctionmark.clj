(ns xtdb.bench.auctionmark
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.bench.xtdb2 :as bxt]
            [xtdb.test-util :as tu])
  (:import (java.time Duration Instant)
           (java.util ArrayList Random UUID)
           (java.util.concurrent ConcurrentHashMap)))

(defn random-price [worker] (.nextDouble (b/rng worker)))

(def user-id (partial str "u_"))
(def region-id (partial str "r_"))
(def item-id (partial str "i_"))
(def item-bid-id (partial str "ib_"))
(def category-id (partial str "c_"))
(def item-image-id (partial str "ii_"))
(def item-comment-id (partial str "ic_"))
(def item-purchase-id (partial str "ip_"))
(def item-feedback-id (partial str "if_"))
(def global-attribute-group-id (partial str "gag_"))
(def gag-id global-attribute-group-id)
(def global-attribute-value-id (partial str "gav_"))
(def gav-id global-attribute-value-id)

(def user-attribute-id (partial str "ua_"))

(defn generate-user [worker]
  (let [u_id (b/increment worker user-id)]
    {:xt/id u_id
     :u_id u_id
     :u_r_id (b/sample-flat worker region-id)
     :u_rating 0
     :u_balance 0.0
     :u_created (b/current-timestamp worker)
     :u_sattr0 (b/random-str worker)
     :u_sattr1 (b/random-str worker)
     :u_sattr2 (b/random-str worker)
     :u_sattr3 (b/random-str worker)
     :u_sattr4 (b/random-str worker)
     :u_sattr5 (b/random-str worker)
     :u_sattr6 (b/random-str worker)
     :u_sattr7 (b/random-str worker)}))

(defn proc-new-user
  "Creates a new USER record. The rating and balance are both set to zero.

  The benchmark randomly selects id from a pool of region ids as an input for u_r_id parameter using flat distribution."
  [worker]
  (xt/submit-tx (:sut worker) [[:put-docs :user (generate-user worker)]]))

(defn- sample-category-id [worker]
  (if-some [weighting (::category-weighting (:custom-state worker))]
    (weighting (b/rng worker))
    (b/sample-gaussian worker category-id)))

(defn sample-status [worker]
  (nth [:open :waiting-for-purchase :closed] (mod (.nextInt ^Random (b/rng worker)) 3)))

(defn proc-new-item
  "Insert a new ITEM record for a user.

  The benchmark client provides all the preliminary information required for the new item, as well as optional information to create derivative image and attribute records.
  After inserting the new ITEM record, the transaction then inserts any GLOBAL ATTRIBUTE VALUE and ITEM IMAGE.

  After these records are inserted, the transaction then updates the USER record to add the listing fee to the seller’s balance.

  The benchmark randomly selects id from a pool of users as an input for u_id parameter using Gaussian distribution. A c_id parameter is randomly selected using a flat histogram from the real auction site’s item category statistic."
  [{:keys [sut] :as worker}]
  (let [i_id-raw (.getAndIncrement (b/counter worker item-id))
        i_id (item-id i_id-raw)
        u_id (b/sample-gaussian worker user-id)
        c_id (sample-category-id worker)
        name (b/random-str worker)
        description (b/random-str worker)
        initial-price (random-price worker)
        attributes (b/random-str worker)
        gag-ids (remove nil? (b/random-seq worker {:min 0, :max 16, :unique true} b/sample-flat global-attribute-group-id))
        gav-ids (remove nil? (b/random-seq worker {:min 0, :max 16, :unique true} b/sample-flat global-attribute-value-id))
        images (b/random-seq worker {:min 0, :max 16, :unique true} b/random-str)
        start-date (b/current-timestamp worker)
        ;; up to 42 days
        end-date (.plusSeconds ^Instant start-date (* 60 60 24 (* (inc (.nextInt (b/rng worker) 42)))))
        ;; append attribute names to desc
        description-with-attributes
        (->> (for [gag-id gag-ids
                   gav-id gav-ids]
               (let [{:keys [gag_name gav_name]}
                     (first (xt/q sut
                                  "SELECT gag.gag_name, gav.gav_name, gag.gag_c_id FROM gag, gav
                                   WHERE gav.xt$id = ? AND gag.xt$id = ? AND gav.gav_gag_id = gag.xt$id"
                                  {:args [gav-id gag-id] , :key-fn :snake-case-keyword}))]
                 (str gag_name " " gav_name)))
             (str/join " ")
             (str description " "))]

    (->> (concat
          [[:put-docs :item
            {:xt/id i_id
             :i_id i_id
             :i_u_id u_id
             :i_c_id c_id
             :i_name name
             :i_description description-with-attributes
             :i_user_attributes attributes
             :i_initial_price initial-price
             :i_num_bids 0
             :i_num_images (count images)
             :i_num_global_attrs (count gav-ids)
             :i_start_date start-date
             :i_end_date end-date
             :i_status :open}]]
          (for [[i image] (map-indexed vector images)
                :let [ii_id (item-image-id (bit-or (bit-shift-left i 60) (bit-and i_id-raw 0x0FFFFFFFFFFFFFFF)))]]
            [:put-docs :item-image
             {:xt/id ii_id
              :ii_id ii_id
              :ii_i_id i_id
              :ii_u_id u_id
              :ii_path image}])
          (when u_id [[:sql "UPDATE user SET u_balance = user.u_balance - 1 WHERE user.xt$id = ? "
                       [u_id]]]))
         (xt/submit-tx sut))))

(defn random-item [worker & {:keys [status] :or {status :all}}]
  (let [isg (-> worker :custom-state :item-status-groups (get status) vec)
        item (b/random-nth worker isg)]
    item))

(defn generate-new-bid-params [worker]
  (let [{:keys [i_id, i_u_id]} (random-item worker :status :open)
        i_buyer_id (b/sample-gaussian worker user-id)]
    (if (and i_buyer_id (= i_buyer_id i_u_id))
      (generate-new-bid-params worker)
      {:i_id i_id,
       :u_id i_u_id,
       :i_buyer_id i_buyer_id
       :bid (random-price worker)
       :max_bid (random-price worker)
       :new_bid_id (b/increment worker item-bid-id)
       :now (b/current-timestamp worker)})))

(defn proc-new-bid [{:keys [sut] :as worker}]
  (let [{:keys [i_id u_id i_buyer_id bid max_bid new_bid_id now]} (generate-new-bid-params worker)]
    (when (and i_id u_id)
      (let [{:keys [imb imb_ib_id] :as _res}
            (-> (xt/q sut "SELECT imb.imb, imb.imb_ib_id FROM item_max_bid AS imb WHERE imb.xt$id = ?"
                      {:args [i_id], :key-fn :snake-case-keyword})
                first)
            {:keys [curr_bid, curr_max]}
            (-> (xt/q sut "SELECT ib.ib_bid AS curr_bid, ib.ib_max_bid AS curr_max FROM item_bid AS ib
                           WHERE ib.xt$id = ?"
                      {:args [imb_ib_id] :key-fn :snake-case-keyword})
                first)
            new_bid_win (or (nil? imb_ib_id) (< curr_max max_bid))
            new_bid (if (and new_bid_win curr_max (< bid curr_max)) curr_max bid)
            upd_curr_bid (and curr_bid (not new_bid_win) (< curr_bid bid))
            composite-id-fn (fn [& ids] (apply str (butlast (interleave ids (repeat "-")))))]

        (xt/submit-tx sut
                      ;; increment number of bids on item
                      (cond->
                          [[:sql "UPDATE item SET i_num_bids = item.i_num_bids + 1 WHERE item.xt$id = ?"
                            [i_id]]]

                        ;; if new bid exceeds old, bump it
                        upd_curr_bid
                        (conj [[:sql "UPDATE item_max_bid SET bid = ? WHERE item_max_bid.imb = ?"
                                [bid imb]]])

                        ;; we exceed the old max, win the bid.
                        (and curr_bid new_bid_win)
                        (conj [[:sql "UPDATE item_max_bid
                                      SET imb_ib_id = ?
                                      imb_ib_i_id = ?
                                      imb_ib_u_id = ?
                                      imb_updated = ?
                                      WHERE item_max_bid.i_id = ? AND item_max_bid.u_id = ?"

                                [new_bid_id i_id u_id now i_id u_id]]])
                        ;; no previous max bid, insert new max bid
                        (nil? imb_ib_id)
                        (conj [:put-docs :item-max-bid {:xt/id (composite-id-fn new_bid_id i_id)
                                                        :imb_i_id i_id
                                                        :imb_u_id u_id
                                                        :imb_ib_id new_bid_id
                                                        :imb_ib_i_id i_id
                                                        :imb_ib_u_id u_id
                                                        :imb_created now
                                                        :imb_updated now}])

                        ;; add new bid
                        :always
                        (conj [:put-docs :item-bid {:xt/id new_bid_id
                                                    :ib_id new_bid_id
                                                    :ib_i_id i_id
                                                    :ib_u_id u_id
                                                    :ib_buyer_id i_buyer_id
                                                    :ib_bid new_bid
                                                    :ib_max_bid max_bid
                                                    :ib_created_at now
                                                    :ib_updated now}])))))))

(defn proc-new-comment [{:keys [sut] :as worker}]
  ;; TODO this is normally queried from closed items
  (let [{:keys [i_id] seller_id :i_u_id} (random-item worker :status :open)
        buyer_id (b/sample-flat worker user-id)
        now (b/current-timestamp worker)
        question (b/random-str worker)
        ic_id (item-comment-id (.getAndIncrement (b/counter worker item-comment-id)))
        ;; TODO move into INSERT STATEMENT see #3325
        #_#_new-item-comment-id (-> (xt/q sut "SELECT COALESCE(MAX(ic.ic_id) + 1, 0) AS new_item_comment_id FROM item_comment AS ic"
                                          {:key-fn :snake-case-keyword})
                                    first
                                    :new_item_comment_id
                                    item-comment-id)]
    (xt/execute-tx sut [[:sql "INSERT INTO item_comment (xt$id, ic_id, ic_i_id, ic_u_id, ic_buyer_id, ic_date, ic_question)
                              VALUES (?, ?, ?, ?, ?, ?, ?)"
                         [ic_id ic_id i_id seller_id buyer_id now question]]])))

(defn proc-new-comment-response [{:keys [sut] :as worker}]
  (let [ic_id (b/sample-flat worker item-comment-id)
        comment (b/random-str worker)
        ;; TODO should probably be moved to the sampling logic
        {item_id :ic_i_id seller_id :ic_u_id} (first (xt/q sut "SELECT * FROM item_comment AS ic WHERE ic.xt$id = ?"
                                                           {:args [ic_id]
                                                            :key-fn :snake-case-keyword}))]
    (xt/execute-tx sut [[:sql "UPDATE item_comment AS ic SET ic_response = ?
                              WHERE ic.xt$id = ? AND ic.ic_i_id = ? AND ic.ic_u_id = ?"
                         [comment ic_id item_id seller_id]]])))

(defn proc-new-purchase [{:keys [sut] :as worker}]
  (let [{:keys [i_id i_u_id]} (random-item worker :status :waiting-for-purchase)
        ;; TODO buyer_id should be used for validation
        {buyer_id :imb_ib_u_id bid_id :imb_ib_id}
        (-> (xt/q sut "SELECT imb.imb_ib_id, imb.imb_ib_u_id FROM item_max_bid AS imb WHERE imb.imb_i_id = ? AND imb.imb_u_id = ?"
                  {:args [i_id i_u_id] :key-fn :snake-case-keyword})
            first)
        ip_id (item-purchase-id (.getAndIncrement (b/counter worker item-purchase-id)))
        now (b/current-timestamp worker)]
    ;; TODO properly test with imb
    (when (and i_id i_u_id #_ buyer_id)
      (xt/submit-tx sut [[:sql "INSERT INTO item_purchase (xt$id, ip_id, ip_ib_id, ip_ib_i_id, ip_ib_u_id, ip_date)
                                VALUES(?, ?, ?, ?, ?, ?)"
                          [ip_id ip_id bid_id i_id i_u_id now]]
                         [:sql "UPDATE item SET i_status = ?
                                WHERE item.xt$id = ?"
                          [:closed i_id]]]))))

(defn proc-new-feedback [{:keys [sut] :as worker}]
  (let [{:keys [i_id i_u_id] :as _item} (random-item worker :status :closed)
        if_id (item-feedback-id (.getAndIncrement (b/counter worker item-feedback-id)))
        {buyer_id :imb_ib_u_id}
        (-> (xt/q sut "SELECT imb.imb_ib_id, imb.imb_ib_u_id FROM item_max_bid AS imb WHERE imb.imb_i_id = ? AND imb.imb_u_id = ?"
                  {:args [i_id i_u_id] :key-fn :snake-case-keyword})
            first)

        rating (b/random-nth worker [-1 0 1])
        comment (b/random-str worker)
        now (b/current-timestamp worker)]
    ;; TODO properly test with imb
    (when (and i_id i_u_id #_buyer_id)
      (xt/submit-tx sut [[:sql "INSERT INTO item_feedback (xt$id, if_id, if_i_id, if_u_id, if_buyer_id, if_rating, if_date, if_comment)
                                VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
                          [if_id if_id i_id i_u_id buyer_id rating now comment]]]))))

(defn proc-get-item [{:keys [sut] :as worker}]
  (let [;; TODO
        ;; the benchbase project uses a profile that keeps item pairs around
        ;; selects only closed items for a particular user profile (they are sampled together)
        ;; right now this is a totally random sample with one less join than we need.
        {:keys [i_id]} (random-item worker :status :open)]
    (xt/q sut "SELECT item.i_id, item.i_u_id, item.i_name, item.i_current_price, item.i_num_bids,
                      item.i_end_date, item.i_status
               FROM item WHERE item.xt$id = ?"
          {:args [i_id] :key-fn :snake-case-keyword})))

;; TODO aborted transactions
(defn proc-update-item [{:keys [sut] :as worker}]
  (let [{:keys [i_id]} (random-item worker :status :open)
        description (b/random-str worker)]
    (xt/submit-tx sut [[:sql "UPDATE item
                              SET i_description = ?
                              WHERE item.xt$id = ?"
                        [description i_id]]])))

(defrecord UserItem [item_id user_id item_status max_bid_id buyer_id])

(defn proc-post-auction [{:keys [sut] :as worker} due-items]
  (let [[items-with-bid items-without-bid] ((juxt filter remove) :max_bid_id due-items)
        now (b/current-timestamp worker)]
    (xt/submit-tx sut
                  (cond-> []
                    (seq items-without-bid)
                    (conj (into [:sql "UPDATE item SET i_status = ? WHERE item.xt$id = ?"]
                                (map #(vector :closed (:item_id %)) items-without-bid)))

                    (seq items-with-bid)
                    (into [(into [:sql "UPDATE item SET i_status = ? WHERE item.xt$id = ?"]
                                 (map #(vector :waiting-for-purchase (:item_id %)) items-with-bid))
                           (into [:sql "INSERT INTO user_item(xt$id, ui_u_id, ui_i_id, ui_i_u_id, ui_created)
                                        VALUES(?, ?, ?, ?, ?)"]
                                 (map #(concat [(UUID/randomUUID)] ((juxt :buyer_id :item_id :user_id) %) [now]) items-with-bid))])))))

(defn proc-check-winning-bids [{:keys [sut] :as worker}]
  (let [now (b/current-timestamp worker)
        now-minus-60s (.minusSeconds now 60)]
    (->> (xt/q sut "SELECT item.i_id AS item_id, item.i_u_id AS user_id, item.i_name, item.i_current_price, item.i_num_bids,
                           item.i_end_date, item.i_status AS item_status
                    FROM item WHERE (item.i_start_date BETWEEN ? AND ?) AND item.i_status = ? ORDER BY item.i_id ASC LIMIT 100"
               {:args [now-minus-60s now :open]
                :key-fn :snake-case-keyword})

         (mapv (comp map->UserItem
                     (fn [{:keys [item_id user_id] :as due-item}]
                       (if-let [{:keys [max_bid_id buyer_id]}
                                (first (xt/q sut "SELECT imb.imb_ib_id AS max_bid_id, ib.ib_buyer_id AS buyer_id
                                           FROM item_max_bid AS imb, item_bid AS ib
                                           WHERE imb.imb_i_id = ? AND imb.imb_u_id = ?
                                           AND ib.xt$id = imb.imb_ib_id AND ib.ib_i_id = imb.imb_i_id AND ib.ib_u_id = imb.imb_u_id"
                                             {:args [item_id user_id]
                                              :key-fn :snake-case-keyword}))]
                         (assoc due-item :max_bid_id max_bid_id :buyer_id buyer_id)
                         due-item))))
         (proc-post-auction worker))))

(defn proc-get-comment [{:keys [sut] :as worker}]
  (let [{:keys [i_u_id]} (random-item worker :status :open)]
    (xt/q sut "SELECT * FROM item_comment AS ic
               WHERE ic.ic_u_id = ? AND ic.ic_response IS NULL"
          {:args [i_u_id]})))

(defn get-user-info [sut u_id seller-items? buyer-items? feedback?]
  (let [user-results (xt/q sut "SELECT user.u_id, user.u_rating, user.u_created, user.u_balance, user.u_sattr0,
                                       user.u_sattr1, user.u_sattr2, user.u_sattr3, user.u_sattr4, region.r_name FROM user, region
                                WHERE user.xt$id = ? AND user.u_r_id = region.xt$id"
                           {:args [u_id] :key-fn :snake-case-keyword})
        item-results (cond seller-items?
                           (xt/q sut "SELECT item.i_id, item.i_u_id, item.i_name, item.i_current_price,
                                             item.i_num_bids, item.i_end_date, item.i_status
                                      FROM item WHERE item.i_u_id = ?
                                      ORDER BY item.i_end_date
                                      DESC LIMIT 20"
                                 {:args [u_id] :key-fn :snake-case-keyword})

                           (and buyer-items? (not seller-items?))
                           (xt/q sut "SELECT item.i_id, item.i_u_id, item.i_name, item.i_current_price,
                                             item.i_num_bids, item.i_end_date, item.i_status
                                      FROM user_item AS ui, item
                                      WHERE ui.ui_u_id = ? AND ui.ui_i_id = item.xt$id AND ui.ui_i_u_id = item.i_u_id
                                      ORDER BY item.i_end_date
                                      DESC LIMIT 20"
                                 {:args [u_id] :key-fn :snake-case-keyword}))
        feedback-results (when feedback?
                           (xt/q sut "SELECT if.if_rating, if.if_comment, if.if_date, item.i_id, item.i_u_id,
                                             item.i_name, item.i_end_date, item.i_status, user.u_id,
                                             user.u_rating, user.u_sattr0, user.u_sattr1
                                      FROM item_feedback AS if, item, user
                                      WHERE if.if_buyer_id = ? AND if.if_i_id = item.xt$id
                                      AND if.if_u_id = item.i_u_id AND if.if_u_id = user.xt$id
                                      ORDER BY if.if_date DESC LIMIT 10"
                                 {:args [u_id] :key-fn :snake-case-keyword}))]
    [user-results item-results feedback-results]))

(defn proc-get-user-info [{:keys [sut] :as worker}]
  (let [u_id (b/sample-flat worker user-id)
        seller-items? (b/random-bool worker)
        buyer-items? (b/random-bool worker)
        feedback? (b/random-bool worker)]
    (get-user-info sut u_id seller-items? buyer-items? feedback?)))

;; represents a probable state of an item that can be sampled randomly
(defrecord ItemSample [i_id, i_u_id, i_status, i_end_date, i_num_bids])

(defn item-status-groups [node]
  (let [items (xt/q node '(from :item [{:xt/id i} i_id i_u_id i_status i_end_date i_num_bids])
                    {:key-fn :snake-case-keyword})
        all (ArrayList.)
        open (ArrayList.)
        waiting-for-purchase (ArrayList.)
        closed (ArrayList.)]
    (doseq [{:keys [i_id i_u_id i_status ^Instant i_end_date i_num_bids]} items
            :let [^ArrayList alist
                  (case i_status
                    :open open
                    :closed closed
                    :waiting-for-purchase waiting-for-purchase
                    ;; TODO debug why this happens
                    nil)

                  item-sample (->ItemSample i_id i_u_id i_status i_end_date i_num_bids)]]

      (.add all item-sample)
      (when alist
        (.add alist item-sample)))
    {:all (vec all)
     :open (vec open)
     :waiting-for-purchase (vec waiting-for-purchase)
     :closed (vec closed)}))

(defn add-item-status [{:keys [^ConcurrentHashMap custom-state]}
                       {:keys [i_status] :as item-sample}]
  (.putAll custom-state {:item-status-groups (-> custom-state :item-status-groups
                                                 (update :all (fnil conj []) item-sample)
                                                 (update i_status (fnil conj []) item-sample))}))

;; do every now and again to provide inputs for item-dependent computations
(defn index-item-status-groups [worker]
  (let [{:keys [sut, ^ConcurrentHashMap custom-state]} worker
        node sut
        res (item-status-groups node)]
    (.putAll custom-state {:item-status-groups res})))

(defn read-category-tsv []
  (let [cat-tsv-rows
        (with-open [rdr (io/reader (io/resource "data/auctionmark/auctionmark-categories.tsv"))]
          (vec (for [line (line-seq rdr)
                     :let [split (str/split line #"\t")
                           cat-parts (butlast split)
                           item-count (last split)
                           parts (remove str/blank? cat-parts)]]
                 {:parts (vec parts)
                  :item-count (parse-long item-count)})))
        extract-cats
        (fn extract-cats [parts]
          (when (seq parts)
            (cons parts (extract-cats (pop parts)))))
        all-paths (into #{} (comp (map :parts) (mapcat extract-cats)) cat-tsv-rows)
        path-i (into {} (map-indexed (fn [i x] [x i])) all-paths)
        trie (reduce #(assoc-in %1 (:parts %2) (:item-count %2)) {} cat-tsv-rows)
        trie-node-item-count (fn trie-node-item-count [path]
                               (let [n (get-in trie path)]
                                 (if (integer? n)
                                   n
                                   (reduce + 0 (map trie-node-item-count (keys n))))))]
    (->> (for [[path i] path-i]
           [(category-id i)
            {:i i
             :xt/id (category-id i)
             :category-name (str/join "/" path)
             :parent (category-id (path-i i))
             :item-count (trie-node-item-count path)}])
         (into {}))))

(defn load-categories-tsv [worker]
  (let [cats (read-category-tsv)
        {:keys [^ConcurrentHashMap custom-state]} worker]
    ;; squirrel these data-structures away for later (see category-generator, sample-category-id)
    (.putAll custom-state {::categories cats
                           ::category-weighting (b/weighted-sample-fn (map (juxt :xt/id :item-count) (vals cats)))})))

(defn generate-region [worker]
  (let [r-id (b/increment worker region-id)]
    {:xt/id r-id
     :r_id r-id
     :r_name (b/random-str worker 6 32)}))

(defn generate-global-attribute-group [worker]
  (let [gag-id (b/increment worker gag-id)
        category-id (b/sample-flat worker category-id)]
    {:xt/id gag-id
     :gag_c_id category-id
     :gag_name (b/random-str worker 6 32)}))

(defn generate-global-attribute-value [worker]
  (let [gav-id (b/increment worker gav-id)
        gag-id (b/sample-flat worker gag-id)]
    {:xt/id gav-id
     :gav_gag_id gag-id
     :gav_name (b/random-str worker 6 32)}))

(defn generate-category [worker]
  (let [{::keys [categories]} (:custom-state worker)
        c-id (b/increment worker category-id)
        {:keys [category-name, parent]} (categories c-id)]
    {:xt/id c-id
     :c_id c-id
     :c_parent_id (when (seq parent) (:xt/id (categories parent)))
     :c_name (or category-name (b/random-str worker 6 32))}))

(defn generate-user-attributes [worker]
  (let [u_id (b/sample-flat worker user-id)
        ua-id (b/increment worker user-attribute-id)]
    (when u_id
      {:xt/id ua-id
       :ua_u_id u_id
       :ua_name (b/random-str worker 5 32)
       :ua_value (b/random-str worker 5 32)
       :u_created (b/current-timestamp worker)})))

(defn generate-item [worker]
  (let [i_id (b/increment worker item-id)
        i_u_id (b/sample-flat worker user-id)
        i_c_id (sample-category-id worker)
        i_start_date (b/current-timestamp worker)
        i_end_date (.plus ^Instant (b/current-timestamp worker) (Duration/ofDays 32))
        i_status (sample-status worker)]
    (add-item-status worker (->ItemSample i_id i_u_id i_status i_end_date 0))
    (when i_u_id
      {:xt/id i_id
       :i_id i_id
       :i_u_id i_u_id
       :i_c_id i_c_id
       :i_name (b/random-str worker 6 32)
       :i_description (b/random-str worker 50 255)
       :i_user_attributes (b/random-str worker 20 255)
       :i_initial_price (random-price worker)
       :i_current_price (random-price worker)
       :i_num_bids 0
       :i_num_images 0
       :i_num_global_attrs 0
       :i_start_date i_start_date
       :i_end_date i_end_date
       :i_status i_status})))

#_{:clj-kondo/ignore [:unused-private-var]}
(defn- wrap-in-logging [f]
  (fn [& args]
    (log/trace (str "Start of " f))
    (let [res (apply f args)]
      (log/trace (str "Finish of " f))
      res)))


(defn largest-id [node table prefix-length]
  (let [id (->> (xt/q node (xt/template (from ~table [{:xt/id id}])))
                (sort-by :id  #(cond (< (count %1) (count %2)) 1
                                     (< (count %2) (count %1)) -1
                                     :else (compare %2 %1)))
                first
                :id)]
    (when id
      (parse-long (subs id prefix-length)))))

(defn load-stats-into-worker [{:keys [sut] :as worker}]
  (index-item-status-groups worker)
  (log/info "query for user")
  (b/set-domain worker user-id (or (largest-id sut :user 2) 0))
  (log/info "query for region")
  (b/set-domain worker region-id (or (largest-id sut :region 2) 0))
  (log/info "query for item")
  (b/set-domain worker item-id (or (largest-id sut :item 2) 0))
  (log/info "query for item-bid")
  (b/set-domain worker item-bid-id (or (largest-id sut :item-bid 3) 0))
  (log/info "query for category")
  (b/set-domain worker category-id (or (largest-id sut :category 2) 0))
  (log/info "query for gag")
  (b/set-domain worker gag-id (or (largest-id sut :gag 4) 0))
  (log/info "query for gav")
  (b/set-domain worker gav-id (or (largest-id sut :gav 4) 0)))

(defn log-stats [worker]
  (log/info "#user " (.get (b/counter worker user-id)))
  (log/info "#region " (.get (b/counter worker region-id)))
  (log/info "#item " (.get (b/counter worker item-id)))
  (log/info "#item-bid " (.get (b/counter worker item-bid-id)))
  (log/info "#category " (.get (b/counter worker category-id)))
  (log/info "#gag " (.get (b/counter worker gag-id)))
  (log/info "#gav " (.get (b/counter worker gav-id))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn benchmark [{:keys [seed,
                         threads,
                         duration
                         scale-factor
                         load-phase
                         sync]
                  :or {seed 0,
                       threads 8,
                       duration "PT30S"
                       scale-factor 0.1
                       load-phase true
                       sync false}}]
  (let [^Duration duration (cond-> duration (string? duration) Duration/parse)
        sf scale-factor]
    (log/trace {:scale-factor scale-factor})
    {:title "Auction Mark OLTP"
     :seed seed
     :tasks
     (into (if load-phase
             [{:t :do
               :stage :load
               :tasks [{:t :call, :f (fn [_] (log/info "start load stage"))}
                       {:t :call, :f load-categories-tsv}
                       {:t :call, :f [bxt/generate :region generate-region 75]}
                       {:t :call, :f [bxt/generate :category generate-category 16908]}
                       {:t :call, :f [bxt/generate :user generate-user (* sf 1e6)]}
                       {:t :call, :f [bxt/generate :user-attribute generate-user-attributes (* sf 1e6 1.3)]}
                       {:t :call, :f [bxt/generate :item generate-item (* sf 1e6 10)]}
                       {:t :call, :f [bxt/generate :gag generate-global-attribute-group 100]}
                       {:t :call, :f [bxt/generate :gav generate-global-attribute-value 1000]}
                       {:t :call, :f (fn [_] (log/info "finished load stage"))}]}]

             [])
           [{:t :do
             :stage :setup-worker
             :tasks [{:t :call, :f (fn [_] (log/info "setting up worker with stats"))}
                     ;; wait for node to catch up
                     {:t :call, :f #(when-not load-phase
                                      ;; otherwise nothing has come through the log yet
                                      (Thread/sleep 1000)
                                      #_(tu/then-await-tx (:sut %)))}
                     {:t :call, :f load-stats-into-worker}
                     {:t :call, :f log-stats}
                     {:t :call, :f (fn [_] (log/info "finished setting up worker with stats"))}]}

            {:t :concurrently
             :stage :oltp
             :duration duration
             :join-wait (Duration/ofSeconds 5)
             :thread-tasks [{:t :pool
                             :duration duration
                             :join-wait (Duration/ofMinutes 5)
                             :thread-count threads
                             :think Duration/ZERO
                             :pooled-task {:t :pick-weighted
                                           :choices [[{:t :call, :transaction :new-user, :f (b/wrap-in-catch proc-new-user)} 5.0]
                                                     [{:t :call, :transaction :new-item, :f (b/wrap-in-catch proc-new-item)} 10.0]
                                                     [{:t :call, :transaction :new-bid,  :f (b/wrap-in-catch proc-new-bid)}  18.0]
                                                     [{:t :call, :transaction :new-comment,
                                                       :f (b/wrap-in-catch proc-new-comment)}  2.0]
                                                     [{:t :call, :transaction :new-comment-response,
                                                       :f (b/wrap-in-catch proc-new-comment-response)}  1.0]
                                                     [{:t :call, :transaction :new-purchase,
                                                       :f (b/wrap-in-catch proc-new-comment-response)}  2.0]
                                                     [{:t :call, :transaction :new-feedback,
                                                       :f (b/wrap-in-catch proc-new-comment-response)}  3.0]
                                                     [{:t :call, :transaction :get-item, :f (b/wrap-in-catch proc-get-item)} 45.0]
                                                     [{:t :call, :transaction :update-item,
                                                       :f (b/wrap-in-catch proc-update-item)} 2.0]
                                                     [{:t :call, :transaction :get-comment,
                                                       :f (b/wrap-in-catch proc-get-comment)} 2.0]
                                                     [{:t :call, :transaction :get-user-info,
                                                       :f (b/wrap-in-catch proc-get-user-info)} 10.0]]}}
                            {:t :freq-job
                             :duration duration
                             :freq (Duration/ofMillis (* 0.2 (.toMillis duration)))
                             :job-task {:t :call, :transaction :index-item-status-groups, :f (b/wrap-in-catch index-item-status-groups)}}]}
            (when sync {:t :call, :f #(tu/then-await-tx (:sut %))})])}))
