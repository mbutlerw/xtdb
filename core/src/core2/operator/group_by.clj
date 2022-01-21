(ns core2.operator.group-by
  (:require [core2.expression :as expr]
            [core2.expression.map :as emap]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import core2.expression.map.IRelationMap
           core2.ICursor
           core2.vector.IIndirectVector
           java.io.Closeable
           [java.util LinkedList List Spliterator]
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BigIntVector Float8Vector IntVector NullVector ValueVector]
           org.apache.arrow.vector.types.pojo.FieldType))

(set! *unchecked-math* :warn-on-boxed)

(definterface IGroupMapper
  (^java.util.Set getColumnNames [])
  (^org.apache.arrow.vector.IntVector groupMapping [^core2.vector.IIndirectRelation inRelation])
  (^java.util.List #_<IIndirectVector> finish []))

(deftype NullGroupMapper [^IntVector group-mapping]
  IGroupMapper
  (getColumnNames [_] #{})
  (groupMapping [_ in-rel]
    (.clear group-mapping)
    (let [row-count (.rowCount in-rel)]
      (.setValueCount group-mapping row-count)
      (dotimes [idx row-count]
        (.set group-mapping idx 0))
      group-mapping))

  (finish [_] [])

  Closeable
  (close [_]
    (.close group-mapping)))

(deftype GroupMapper [^List group-col-names
                      ^IRelationMap rel-map
                      ^IntVector group-mapping]
  IGroupMapper
  (getColumnNames [_] (set group-col-names))
  (groupMapping [_ in-rel]
    (.clear group-mapping)
    (.setValueCount group-mapping (.rowCount in-rel))

    (let [builder (.buildFromRelation rel-map in-rel)]
      (dotimes [idx (.rowCount in-rel)]
        (.set group-mapping idx (emap/inserted-idx (.addIfNotPresent builder idx)))))

    group-mapping)

  (finish [_]
    (seq (.getBuiltRelation rel-map)))

  Closeable
  (close [_]
    (.close group-mapping)
    (util/try-close rel-map)))

(defn- ->group-mapper [^BufferAllocator allocator, group-col-names]
  (let [gm-vec (IntVector. "group-mapping" allocator)]
    (if (seq group-col-names)
      (GroupMapper. group-col-names
                    (emap/->relation-map allocator {:key-col-names group-col-names,
                                                    :store-col-names #{}})
                    gm-vec)
      (NullGroupMapper. gm-vec))))

(definterface IAggregateSpec
  (^String getColumnName [])
  (^void aggregate [^core2.vector.IIndirectRelation inRelation,
                    ^org.apache.arrow.vector.IntVector groupMapping])
  (^core2.vector.IIndirectVector finish []))

(definterface IAggregateSpecFactory
  (^core2.operator.group_by.IAggregateSpec build [^org.apache.arrow.memory.BufferAllocator allocator]))

(defmulti ^core2.operator.group_by.IAggregateSpecFactory ->aggregate-factory
  (fn [f from-name to-name]
    (keyword (name f))))

(defn- emit-agg [from-var from-val-types emit-step]
  (let [acc-sym (gensym 'acc)
        group-mapping-sym (gensym 'group-mapping)
        group-idx-sym (gensym 'group-idx)]
    (eval
     `(fn [~(-> acc-sym (expr/with-tag ValueVector))
           ~(-> from-var (expr/with-tag IIndirectVector))
           ~(-> group-mapping-sym (expr/with-tag IntVector))]

        ~(let [{continue-var :continue} (expr/codegen-expr (expr/form->expr from-var {}) {:var->types {from-var from-val-types}})]
           `(dotimes [~expr/idx-sym (.getValueCount ~from-var)]
              (let [~group-idx-sym (.get ~group-mapping-sym ~expr/idx-sym)]
                (when-not (> (.getValueCount ~acc-sym) ~group-idx-sym)
                  (.setValueCount ~acc-sym (inc ~group-idx-sym)))
                ~(continue-var (fn [var-type val-code]
                                 (emit-step var-type acc-sym group-idx-sym val-code))))))))))

(defn- emit-count-step [var-type acc-sym group-idx-sym _val-code]
  `(let [~(-> acc-sym (expr/with-tag BigIntVector)) ~acc-sym]
     (.setIndexDefined ~acc-sym ~group-idx-sym)
     (when-not ~(= var-type types/null-type)
       (.set ~acc-sym ~group-idx-sym
             (inc (.get ~acc-sym ~group-idx-sym))))))

(defmethod ->aggregate-factory :count [_ ^String from-name, ^String to-name]
  (let [from-var (symbol from-name)]
    (reify IAggregateSpecFactory
      (build [_ al]
        (let [out-vec (BigIntVector. to-name al)]
          (reify
            IAggregateSpec
            (aggregate [_ in-rel group-mapping]
              (let [in-vec (.vectorForName in-rel (name from-var))
                    from-val-types (expr/field->value-types (.getField (.getVector in-vec)))
                    f (emit-agg from-var from-val-types emit-count-step)]
                (f out-vec in-vec group-mapping)))

            (finish [_] (iv/->direct-vec out-vec))

            Closeable
            (close [_] (.close out-vec))))))))

(definterface IPromotableVector
  (^org.apache.arrow.vector.ValueVector getVector [])
  (^org.apache.arrow.vector.ValueVector maybePromote [valueTypes]))

(def ^:private emit-copy-vec
  (let [in-vec-sym (gensym 'in)
        out-vec-sym (gensym 'out)]
    (-> (fn [in-type out-type]
          (eval
           `(fn [~(-> in-vec-sym (expr/with-tag (types/arrow-type->vector-type in-type)))
                 ~(-> out-vec-sym (expr/with-tag (types/arrow-type->vector-type out-type)))]
              (let [row-count# (.getValueCount ~in-vec-sym)]
                (.setValueCount ~out-vec-sym row-count#)
                (dotimes [idx# row-count#]
                  (.set ~out-vec-sym idx# (~(expr/type->cast out-type) (.get ~in-vec-sym idx#))))
                ~out-vec-sym))))
        (memoize))))

(deftype PromotableVector [^BufferAllocator allocator,
                           ^:unsynchronized-mutable ^ValueVector v]
  Closeable
  (close [this] (util/try-close (.getVector this)))

  IPromotableVector
  (getVector [_] v)

  (maybePromote [this from-val-types]
    (let [^ValueVector cur-vec (.getVector this)
          cur-type (.getType (.getField cur-vec))

          new-type (types/least-upper-bound (->> (cond-> from-val-types
                                                   (not (coll? from-val-types)) vector)
                                                 (map #(.getType ^FieldType %))
                                                 (cons cur-type)
                                                 (into [] (filter #(isa? types/arrow-type-hierarchy (class %) ::types/Number)))))

          new-vec (if (= cur-type new-type)
                    cur-vec

                    (let [new-vec (.createVector (types/->field (.getName cur-vec) new-type false) allocator)]
                      (try
                        (when-not (= types/null-type cur-type)
                          (let [copy-vec (emit-copy-vec cur-type new-type)]
                            (copy-vec cur-vec new-vec)))
                        new-vec

                        (catch Throwable e
                          (.close new-vec)
                          (throw e))

                        (finally
                          (.close cur-vec)))))]

      (set! (.v this) new-vec)

      new-vec)))

(defn- promotable-agg-factory [^String from-name, ^String to-name, emit-step]
  (let [from-var (symbol from-name)]
    (reify IAggregateSpecFactory
      (build [_ al]
        (let [out-pvec (PromotableVector. al (NullVector. to-name))]
          (reify
            IAggregateSpec
            (getColumnName [_] to-name)

            (aggregate [_ in-rel group-mapping]
              (when (pos? (.rowCount in-rel))
                (let [in-vec (.vectorForName in-rel (name from-var))
                      from-val-types (expr/field->value-types (.getField (.getVector in-vec)))
                      out-vec (.maybePromote out-pvec from-val-types)
                      f (emit-agg from-var from-val-types
                                  (partial emit-step (.getType (.getField out-vec))))]
                  (f out-vec in-vec group-mapping))))

            (finish [_] (iv/->direct-vec (.getVector out-pvec)))

            Closeable
            (close [_] (util/try-close out-pvec))))))))

(defmethod ->aggregate-factory :sum [_ ^String from-name, ^String to-name]
  (promotable-agg-factory from-name to-name
                          (fn emit-sum-step [acc-type var-type acc-sym group-idx-sym val-code]
                            ;; TODO `DoubleSummaryStatistics` uses 'Kahan's summation algorithm'
                            ;; to compensate for rounding errors - should we?
                            (let [{:keys [continue-call]} (expr/codegen-call {:op :call, :f :+, :arg-types [acc-type var-type]})]
                              `(let [~(-> acc-sym (expr/with-tag (types/arrow-type->vector-type acc-type))) ~acc-sym]
                                 (.setIndexDefined ~acc-sym ~group-idx-sym)
                                 ~(continue-call (fn [arrow-type res-code]
                                                   (expr/set-value-form arrow-type acc-sym group-idx-sym res-code))
                                                 [(expr/get-value-form var-type acc-sym group-idx-sym)
                                                  val-code]))))))

(defmethod ->aggregate-factory :avg [_ ^String from-name, ^String to-name]
  (let [sum-agg (->aggregate-factory :sum from-name "sum")
        count-agg (->aggregate-factory :count from-name "cnt")
        projecter (expr/->expression-projection-spec to-name '(/ (double sum) cnt) {})]
    (reify IAggregateSpecFactory
      (build [_ al]
        (let [sum-agg (.build sum-agg al)
              count-agg (.build count-agg al)
              res-vec (Float8Vector. to-name al)]
          (reify
            IAggregateSpec
            (getColumnName [_] to-name)

            (aggregate [_ in-rel group-mapping]
              (.aggregate sum-agg in-rel group-mapping)
              (.aggregate count-agg in-rel group-mapping))

            (finish [_]
              (let [sum-ivec (.finish sum-agg)
                    count-ivec (.finish count-agg)
                    out-vec (.project projecter al (iv/->indirect-rel [sum-ivec count-ivec]))]
                (if (instance? NullVector (.getVector out-vec))
                  out-vec
                  (do
                    (doto (.makeTransferPair (.getVector out-vec) res-vec)
                      (.transfer))
                    (iv/->direct-vec res-vec)))))

            Closeable
            (close [_]
              (util/try-close res-vec)
              (util/try-close sum-agg)
              (util/try-close count-agg))))))))

(defmethod ->aggregate-factory :variance [_ ^String from-name, ^String to-name]
  (let [avgx-agg (->aggregate-factory :avg from-name "avgx")
        avgx2-agg (->aggregate-factory :avg "x2" "avgx2")
        from-var (symbol from-name)
        x2-projecter (expr/->expression-projection-spec "x2" (list '* from-var from-var) {})
        finish-projecter (expr/->expression-projection-spec to-name '(- avgx2 (* avgx avgx)) {})]
    (reify IAggregateSpecFactory
      (build [_ al]
        (let [avgx-agg (.build avgx-agg al)
              avgx2-agg (.build avgx2-agg al)
              res-vec (Float8Vector. to-name al)]
          (reify
            IAggregateSpec
            (getColumnName [_] to-name)

            (aggregate [_ in-rel group-mapping]
              (with-open [x2 (.project x2-projecter al in-rel)]
                (.aggregate avgx-agg in-rel group-mapping)
                (.aggregate avgx2-agg (iv/->indirect-rel [x2]) group-mapping)))

            (finish [_]
              (let [avgx-ivec (.finish avgx-agg)
                    avgx2-ivec (.finish avgx2-agg)
                    out-ivec (.project finish-projecter al (iv/->indirect-rel [avgx-ivec avgx2-ivec]))]
                (if (instance? NullVector (.getVector out-ivec))
                  out-ivec
                  (do
                    (doto (.makeTransferPair (.getVector out-ivec) res-vec)
                      (.transfer))
                    (iv/->direct-vec res-vec)))))

            Closeable
            (close [_]
              (util/try-close res-vec)
              (util/try-close avgx-agg)
              (util/try-close avgx2-agg))))))))

(defmethod ->aggregate-factory :std-dev [_ ^String from-name, ^String to-name]
  (let [variance-agg (->aggregate-factory :variance from-name "variance")
        finish-projecter (expr/->expression-projection-spec to-name '(sqrt variance) {})]
    (reify IAggregateSpecFactory
      (build [_ al]
        (let [variance-agg (.build variance-agg al)
              res-vec (Float8Vector. to-name al)]
          (reify
            IAggregateSpec
            (getColumnName [_] to-name)

            (aggregate [_ in-rel group-mapping]
              (.aggregate variance-agg in-rel group-mapping))

            (finish [_]
              (let [variance-ivec (.finish variance-agg)
                    out-ivec (.project finish-projecter al (iv/->indirect-rel [variance-ivec]))]
                (if (instance? NullVector (.getVector out-ivec))
                  out-ivec
                  (do
                    (doto (.makeTransferPair (.getVector out-ivec) res-vec)
                      (.transfer))
                    (iv/->direct-vec res-vec)))))

            Closeable
            (close [_]
              (util/try-close res-vec)
              (util/try-close variance-agg))))))))

(defn- min-max-factory
  "update-if-f-kw: update the accumulated value if `(f el acc)`"
  [from-name to-name update-if-f-kw]

  ;; TODO: this still only works for fixed-width values, but it's reasonable to want (e.g.) `(min <string-col>)`
  (promotable-agg-factory from-name to-name
                          (fn emit-min-max-step [acc-type var-type acc-sym group-idx-sym val-code]
                            (let [{:keys [continue-call]} (expr/codegen-call {:op :call, :f update-if-f-kw,
                                                                              :arg-types [var-type acc-type]})
                                  val-sym (gensym 'val)]
                              `(let [~(-> acc-sym (expr/with-tag (types/arrow-type->vector-type acc-type))) ~acc-sym
                                     ~val-sym ~val-code]
                                 (if (.isNull ~acc-sym ~group-idx-sym)
                                   ~(expr/set-value-form acc-type acc-sym group-idx-sym val-sym)

                                   ~(continue-call (fn [_arrow-type res-code]
                                                     `(when ~res-code
                                                        ~(expr/set-value-form acc-type acc-sym group-idx-sym val-sym)))
                                                   [val-sym
                                                    (expr/get-value-form var-type acc-sym group-idx-sym)])))))))

(defmethod ->aggregate-factory :min [_ from-name to-name] (min-max-factory from-name to-name :<))
(defmethod ->aggregate-factory :max [_ from-name to-name] (min-max-factory from-name to-name :>))

(deftype GroupByCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^IGroupMapper group-mapper
                        ^List aggregate-specs]
  ICursor
  (getColumnNames [_]
    (into (.getColumnNames group-mapper)
          (map #(.getColumnName ^IAggregateSpec %))
          aggregate-specs))

  (tryAdvance [_ c]
    (try
      (.forEachRemaining in-cursor
                         (reify Consumer
                           (accept [_ in-rel]
                             (with-open [group-mapping (.groupMapping group-mapper in-rel)]
                               (doseq [^IAggregateSpec agg-spec aggregate-specs]
                                 (.aggregate agg-spec in-rel group-mapping))))))

      (let [out-rel (iv/->indirect-rel (concat (.finish group-mapper)
                                               (map #(.finish ^IAggregateSpec %) aggregate-specs)))]
        (if (pos? (.rowCount out-rel))
          (do
            (.accept c out-rel)
            true)
          false))
      (finally
        (util/try-close group-mapper)
        (run! util/try-close aggregate-specs))))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE))

  (close [_]
    (run! util/try-close aggregate-specs)
    (util/try-close in-cursor)
    (util/try-close group-mapper)))

(defn ->group-by-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor,
                                        ^List #_<String> group-col-names
                                        ^List #_<IAggregateSpecFactory> agg-factories]
  (let [agg-specs (LinkedList.)]
    (try
      (doseq [^IAggregateSpecFactory factory agg-factories]
        (.add agg-specs (.build factory allocator)))

      (GroupByCursor. allocator in-cursor (->group-mapper allocator group-col-names) (vec agg-specs))

      (catch Exception e
        (run! util/try-close agg-specs)
        (throw e)))))