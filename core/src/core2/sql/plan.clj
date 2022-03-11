(ns core2.sql.plan
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.zip :as z]
            [clojure.spec.alpha :as s]
            [core2.logical-plan :as lp]
            [core2.rewrite :as r]
            [core2.sql.analyze :as sem]
            [instaparse.core :as insta])
  (:import clojure.lang.IObj))

;; Attribute grammar for transformation into logical plan.

;; See https://cs.ulb.ac.be/public/_media/teaching/infoh417/sql2alg_eng.pdf

(def ^:private ^:const ^String relation-id-delimiter "__")
(def ^:private ^:const ^String relation-prefix-delimiter "_")

(declare expr)

(defn- maybe-add-ref [z x]
  (if (instance? IObj x)
    (vary-meta x assoc :ref z)
    x))

(defn- id-symbol [table table-id column]
  (with-meta
    (symbol (str table relation-id-delimiter table-id relation-prefix-delimiter column))
    {:column-reference {:table-id table-id
                        :correlation-name table
                        :column column}}))

(defn unqualified-projection-symbol [{:keys [identifier ^long index] :as projection}]
  (symbol (or identifier (str "$column_" (inc index) "$"))))

(defn qualified-projection-symbol [{:keys [qualified-column original-index] :as projection}]
  (let [{derived-column :ref table :table} (meta projection)]
    (if derived-column
      (expr (r/$ derived-column 1))
      (id-symbol (first qualified-column)
                 (:id table)
                 (unqualified-projection-symbol
                  (cond-> projection
                    original-index (assoc :index original-index)))))))

(defn- aggregate-symbol [prefix z]
  (let [query-id (sem/id (sem/scope-element z))]
    (symbol (str "$" prefix "__" query-id "_" (sem/id z) "$"))))

(defn- subquery-projection-symbols [prefix qe]
  (let [subquery-id (sem/id qe)]
    (vec (for [projection (first (sem/projected-columns qe))]
           (id-symbol prefix subquery-id (unqualified-projection-symbol projection))))))

;; Expressions.

(defn expr [z]
  (maybe-add-ref
   z
   (r/zmatch z
     [:column_reference _]
     (let [{:keys [table-id identifiers]} (sem/column-reference z)
           [table column] identifiers]
       (id-symbol table table-id column))

     [:boolean_value_expression ^:z bve _ ^:z bt]
     ;;=>
     (list 'or (expr bve) (expr bt))

     [:boolean_term ^:z bt _ ^:z bf]
     ;;=>
     (list 'and (expr bt) (expr bf))

     [:boolean_factor _ ^:z bt]
     ;;=>
     (list 'not (expr bt))

     [:boolean_test ^:z bp]
     (expr bp)

     [:comparison_predicate ^:z rvp-1 [:comparison_predicate_part_2 [_ co] ^:z rvp-2]]
     ;;=>
     (list (symbol co) (expr rvp-1) (expr rvp-2))

     [:signed_numeric_literal ^:z unl]
     (expr unl)

     [:exact_numeric_literal ^:z ui]
     (expr ui)

     [:unsigned_integer lexeme]
     ;;=>
     (Long/parseLong lexeme)

     [:character_string_literal lexeme]
     (subs lexeme 1 (dec (count lexeme)))

     [:named_columns_join _ _]
     (reduce
      (fn [acc expr]
        (list 'and acc expr))
      (let [{:keys [join-columns] :as env} (sem/named-columns-join-env z)]
        (for [column join-columns]
          (->> (for [side [:lhs :rhs]]
                 (qualified-projection-symbol (first (get-in env [side column]))))
               (apply list '=)))))

     [:aggregate_function _]
     (aggregate-symbol "agg_out" z)

     [:subquery ^:z qe]
     (let [subquery-type (sem/subquery-type z)]
       (case (:type subquery-type)
         :scalar_subquery (first (subquery-projection-symbols "subquery" qe))))

     (throw (IllegalArgumentException. (str "Cannot build expression for: "  (pr-str (z/node z))))))))

;; Logical plan.

(defn- wrap-with-select [sc-expr relation]
  (reduce
   (fn [acc predicate]
     [:select predicate acc])
   relation
   ((fn step [sc-expr]
      (if (and (list? sc-expr)
               (= 'and (first sc-expr)))
        (concat (step (nth sc-expr 1))
                (step (nth sc-expr 2)))
        [sc-expr]))
    sc-expr)))

(defn- needs-group-by? [z]
  (boolean (:grouping-columns (sem/local-env (sem/group-env z)))))

(defn- wrap-with-group-by [te relation]
  (let [projection (first (sem/projected-columns te))
        {:keys [grouping-columns]} (sem/local-env (sem/group-env te))
        grouping-columns (set grouping-columns)
        grouping-columns (vec (for [{:keys [qualified-column] :as projection} projection
                                    :when (contains? grouping-columns qualified-column)
                                    :let [derived-column (:ref (meta projection))]]
                                (expr (r/$ derived-column 1))))
        aggregates (r/collect-stop
                    (fn [z]
                      (r/zcase z
                        :aggregate_function [z]
                        :subquery []
                        nil))
                    (sem/scope-element te))]
    [:group-by (->> (for [aggregate aggregates]
                      (r/zmatch aggregate
                        [:aggregate_function [:general_set_function [:computational_operation sf] _]]
                        {(aggregate-symbol "agg_out" aggregate)
                         (list (symbol (str/lower-case sf)) (aggregate-symbol "agg_in" aggregate))}))
                    (into grouping-columns))
     [:project (->> (for [aggregate aggregates]
                      (r/zmatch aggregate
                        [:aggregate_function [:general_set_function _ ^:z ve]]
                        {(aggregate-symbol "agg_in" aggregate) (expr ve)}))
                    (into grouping-columns))
      relation]]))

(declare expr-symbols)

(defn- wrap-with-order-by [ssl relation]
  (let [projection (first (sem/projected-columns ssl))
        query-id (sem/id (sem/scope-element ssl))
        order-by-specs (r/collect-stop
                        (fn [z]
                          (r/zcase z
                            :sort_specification
                            (let [direction (case (sem/ordering-specification z)
                                              "ASC" :asc
                                              "DESC" :desc
                                              :asc)]
                              [(if-let [idx (sem/order-by-index z)]
                                 {:spec {(unqualified-projection-symbol (nth projection idx)) direction}}
                                 (let [column (symbol (str "$order_by__" query-id  "_" (r/child-idx z) "$"))]
                                   {:spec {column direction}
                                    :projection {column (expr (r/$ z 1))}}))])

                            :subquery
                            []

                            nil))
                        ssl)
        order-by-projection (keep :projection order-by-specs)
        extra-projection (distinct (mapcat (comp expr-symbols vals) order-by-projection))
        base-projection (mapv unqualified-projection-symbol projection)
        relation (if (not-empty extra-projection)
                   (->> (z/vector-zip relation)
                        (r/once-td-tp
                         (r/mono-tp
                          (fn [z]
                            (r/zmatch z
                              [:project projection relation]
                              ;;=>
                              [:project (vec (concat projection extra-projection)) relation]))))
                        (z/node))
                   relation)
        order-by [:order-by (mapv :spec order-by-specs)
                  (if (not-empty order-by-projection)
                    [:project (vec (concat base-projection order-by-projection)) relation]
                    relation)]]
    (if (not-empty order-by-projection)
      [:project base-projection order-by]
      order-by)))

(defn- wrap-with-subquery-apply [z relation]
  (let [subqueries (r/collect-stop
                    (fn [z]
                      (r/zcase z
                        (:subquery
                         :exists_predicate
                         :in_predicate
                         :quantified_comparison_predicate) [z]
                        nil))
                    z)]
    (reduce
     (fn [acc sq]
       (letfn [(build-apply [column->param projected-columns relation subquery-plan]
                 [:apply
                  :cross-join
                  column->param
                  projected-columns
                  relation
                  (w/postwalk-replace column->param subquery-plan)])]
         (r/zmatch sq
           [:subquery ^:z qe]
           (let [subquery-type (sem/subquery-type sq)
                 projected-columns (set (subquery-projection-symbols "subquery" qe))
                 sq-id (symbol (str "subquery__" (sem/id qe)))
                 subquery-plan [:rename sq-id (plan qe)]]
             (build-apply
              {}
              projected-columns
              relation
              (if (= :scalar_subquery (:type subquery-type))
                [:max-1-row subquery-plan]
                subquery-plan))))))
     relation
     subqueries)))

(declare plan)

(defn- build-query-specification [sl te]
  (let [projection (first (sem/projected-columns sl))
        unqualified-rename-map (->> (for [{:keys [qualified-column] :as projection} projection
                                          :when qualified-column]
                                      [(qualified-projection-symbol projection)
                                       (unqualified-projection-symbol projection)])
                                    (into {}))
        qualified-projection (vec (for [{:keys [qualified-column] :as projection} projection
                                        :let [derived-column (:ref (meta projection))]]
                                    (if qualified-column
                                      (qualified-projection-symbol projection)
                                      {(unqualified-projection-symbol projection)
                                       (expr (r/$ derived-column 1))})))
        relation (wrap-with-subquery-apply sl (plan te))
        qualified-project [:project qualified-projection relation]]
    (if (not-empty unqualified-rename-map)
      [:rename unqualified-rename-map qualified-project]
      qualified-project)))

(defn- build-set-op [set-op lhs rhs]
  (let [lhs-unqualified-project (mapv unqualified-projection-symbol (first (sem/projected-columns lhs)))
        rhs-unqualified-project (mapv unqualified-projection-symbol (first (sem/projected-columns rhs)))]
    [set-op (plan lhs)
     (if (= lhs-unqualified-project rhs-unqualified-project)
       (plan rhs)
       [:rename (zipmap rhs-unqualified-project lhs-unqualified-project)
        (plan rhs)])]))

(defn- build-collection-derived-table [tp]
  (let [{:keys [id correlation-name] :as table} (sem/table tp)
        unwind-column (qualified-projection-symbol (ffirst (sem/projected-columns tp)))
        cdt (r/$ tp 1)
        qualified-projection (vec (for [table (vals (sem/local-env-singleton-values (sem/env cdt)))
                                        :let [{:keys [ref]} (meta table)]
                                        projection (first (sem/projected-columns ref))
                                        :let [column (qualified-projection-symbol projection)]]
                                    (if (= unwind-column column)
                                      {unwind-column (expr (r/$ cdt 2))}
                                      column)))]
    [:unwind (with-meta
               unwind-column
               {:table-reference {:table-id id
                                  :correlation-name correlation-name}})
     [:project qualified-projection nil]]))

(defn- build-table-primary [tp]
  (let [{:keys [id correlation-name] :as table} (sem/table tp)
        projection (first (sem/projected-columns tp))]
    [:rename (with-meta
               (symbol (str correlation-name relation-id-delimiter id))
               {:table-reference {:table-id id
                                  :correlation-name correlation-name}})
     (if-let [subquery-ref (:subquery-ref (meta table))]
       (if-let [derived-columns (sem/derived-columns tp)]
         [:rename (zipmap (map unqualified-projection-symbol (first (sem/projected-columns subquery-ref)))
                          (map symbol derived-columns))
          (plan subquery-ref)]
         (plan subquery-ref))
       [:scan (vec (for [{:keys [identifier]} projection]
                     (symbol identifier)))])]))

(defn- build-table-reference-list [trl]
  (reduce
   (fn [acc table]
     (r/zmatch table
       [:unwind cve [:project projection nil]]
       ;;=>
       [:unwind cve [:project projection acc]]

       [:cross-join acc table]))
   (r/collect-stop
    (fn [z]
      (r/zcase z
        (:table_primary
         :qualified_join) [(plan z)]
        :subquery []
        nil))
    trl)))

(defn- plan [z]
  (maybe-add-ref
   z
   (r/zmatch z
     [:directly_executable_statement ^:z dsds]
     (plan dsds)

     [:query_expression ^:z qeb]
     (plan qeb)

     [:query_expression ^:z qeb [:order_by_clause _ _ ^:z ssl]]
     (wrap-with-order-by ssl (plan qeb))

     [:query_expression ^:z qeb [:result_offset_clause _ rorc _]]
     [:top {:skip (expr rorc)} (plan qeb)]

     [:query_expression ^:z qeb [:order_by_clause _ _ ^:z ssl] [:result_offset_clause _ rorc _]]
     [:top {:skip (expr rorc)} (wrap-with-order-by ssl (plan qeb))]

     [:query_expression ^:z qeb [:fetch_first_clause _ _ ffrc _ _]]
     [:top {:limit (expr ffrc)} (plan qeb)]

     [:query_expression ^:z qeb [:order_by_clause _ _ ^:z ssl] [:fetch_first_clause _ _ ffrc _ _]]
     [:top {:limit (expr ffrc)} (wrap-with-order-by ssl (plan qeb))]

     [:query_expression ^:z qeb [:result_offset_clause _ rorc _] [:fetch_first_clause _ _ ffrc _ _]]
     [:top {:skip (expr rorc) :limit (expr ffrc)} (plan qeb)]

     [:query_expression ^:z qeb [:order_by_clause _ _ ^:z ssl] [:result_offset_clause _ rorc _] [:fetch_first_clause _ _ ffrc _ _]]
     [:top {:skip (expr rorc) :limit (expr ffrc)} (wrap-with-order-by ssl (plan qeb))]

     [:query_specification _ ^:z sl ^:z te]
     ;;=>
     (build-query-specification sl te)

     [:query_specification _ [:set_quantifier "ALL"] ^:z sl ^:z te]
     ;;=>
     (build-query-specification sl te)

     [:query_specification _ [:set_quantifier "DISTINCT"] ^:z sl ^:z te]
     ;;=>
     [:distinct (build-query-specification sl te)]

     [:query_expression_body ^:z qeb "UNION" ^:z qt]
     [:distinct (build-set-op :union-all qeb qt)]

     [:query_expression_body ^:z qeb "UNION" "ALL" ^:z qt]
     (build-set-op :union-all qeb qt)

     [:query_expression_body ^:z qeb "EXCEPT" ^:z qt]
     (build-set-op :difference qeb qt)

     [:query_term ^:z qt "INTERSECT" ^:z qp]
     (build-set-op :intersect qt qp)

     [:table_expression ^:z fc]
     ;;=>
     (cond->> (plan fc)
       (needs-group-by? z) (wrap-with-group-by z))

     [:table_expression ^:z fc [:group_by_clause _ _ _]]
     ;;=>
     (wrap-with-group-by z (plan fc))

     [:table_expression ^:z fc [:where_clause _ ^:z sc]]
     ;;=>
     (cond->> (wrap-with-select (expr sc) (plan fc))
       (needs-group-by? z) (wrap-with-group-by z))

     [:table_expression ^:z fc [:where_clause _ ^:z sc] [:group_by_clause _ _ _]]
     ;;=>
     (->> (wrap-with-select (expr sc) (plan fc))
          (wrap-with-group-by z))

     [:table_expression ^:z fc [:where_clause _ ^:z sc] [:group_by_clause _ _ _] [:having_clause _ ^:z hsc]]
     ;;=>
     (->> (wrap-with-select (expr sc) (plan fc))
          (wrap-with-group-by z)
          (wrap-with-select (expr hsc)))

     [:table_expression ^:z fc [:where_clause _ ^:z sc] [:having_clause _ ^:z hsc]]
     ;;=>
     (->> (wrap-with-select (expr sc) (plan fc))
          (wrap-with-group-by z)
          (wrap-with-select (expr hsc)))

     [:table_expression ^:z fc [:group_by_clause _ _ _] [:having_clause _ ^:z hsc]]
     ;;=>
     (->> (wrap-with-group-by z (plan fc))
          (wrap-with-select (expr hsc)))

     [:table_expression ^:z fc [:having_clause _ ^:z hsc]]
     ;;=>
     (->> (wrap-with-group-by z (plan fc))
          (wrap-with-select (expr hsc)))

     [:table_primary [:collection_derived_table _ _] _ _]
     ;;=>
     (build-collection-derived-table z)

     [:table_primary [:collection_derived_table _ _] _ _ _]
     (build-collection-derived-table z)

     [:table_primary _]
     ;;=>
     (build-table-primary z)

     [:table_primary _ _]
     ;;=>
     (build-table-primary z)

     [:table_primary _ _ _]
     ;;=>
     (build-table-primary z)

     [:table_primary _ _ _ _]
     ;;=>
     (build-table-primary z)

     [:qualified_join ^:z lhs _ ^:z rhs [:join_condition _ ^:z sc]]
     ;;=>
     (wrap-with-select (expr sc) [:join {} (plan lhs) (plan rhs)])

     [:qualified_join ^:z lhs ^:z jt _ ^:z rhs [:join_condition _ ^:z sc]]
     ;;=>
     (wrap-with-select (expr sc) (case (sem/join-type jt)
                                   "LEFT" [:left-outer-join {} (plan lhs) (plan rhs)]
                                   "RIGHT" [:left-outer-join {} (plan rhs) (plan lhs)]
                                   "INNER" [:join {} (plan lhs) (plan rhs)]))

     [:qualified_join ^:z lhs _ ^:z rhs ^:z ncj]
     ;;=>
     (wrap-with-select (expr ncj) [:join {} (plan lhs) (plan rhs)])

     [:qualified_join ^:z lhs ^:z jt _ ^:z rhs ^:z ncj]
     ;;=>
     (wrap-with-select (expr ncj) (case (sem/join-type jt)
                                    "LEFT" [:left-outer-join {} (plan lhs) (plan rhs)]
                                    "RIGHT" [:left-outer-join {} (plan rhs) (plan lhs)]
                                    "INNER" [:join {} (plan lhs) (plan rhs)]))

     [:from_clause _ ^:z trl]
     ;;=>
     (build-table-reference-list trl)

     (throw (IllegalArgumentException. (str "Cannot build plan for: "  (pr-str (z/node z))))))))

;; Rewriting of logical plan.

(defn- table-references-in-subtree [op]
  (set (r/collect-stop
        (fn [z]
          (r/zmatch z
            [:rename prefix _]
            (when-let [table-reference (:table-reference (meta prefix))]
              [table-reference])
            [:unwind cve _]
            (when-let [table-reference (:table-reference (meta cve))]
              [table-reference])))
        (z/vector-zip op))))

(defn- table-ids-in-subtree [op]
  (->> (table-references-in-subtree op)
       (map :table-id)
       (set)))

(defn expr-symbols [expr]
  (set (for [x (flatten expr)
             :when (:column-reference (meta x))]
         x)))

(defn- expr-column-references [expr]
  (->> (expr-symbols expr)
       (map (comp :column-reference meta))
       (set)))

(defn- expr-table-ids [expr]
  (->> (expr-column-references expr)
       (map :table-id)
       (set)))

(defn- equals-predicate? [predicate]
  (and (= '= (first predicate))
       (= 3 (count predicate))))

(defn- build-join-map [predicate lhs rhs]
  (when (equals-predicate? predicate)
    (let [[_ x y] predicate
          {x-table-id :table-id} (:column-reference (meta x))
          {y-table-id :table-id} (:column-reference (meta y))
          [lhs-v rhs-v] (for [side [lhs rhs]
                              :let [table-ids (table-ids-in-subtree side)]]
                          (cond
                            (contains? table-ids x-table-id)
                            x
                            (contains? table-ids y-table-id)
                            y))]
      (when (and lhs-v rhs-v)
        {lhs-v rhs-v}))))

(defn- conjunction-clauses [predicate]
  (if (= 'and (first predicate))
    (rest predicate)
    [predicate]))

(defn- merge-conjunctions [predicate-1 predicate-2]
  (let [predicates (->> (concat (conjunction-clauses predicate-1)
                                (conjunction-clauses predicate-2))
                        (distinct)
                        (sort-by str))]
    (if (= 1 (count predicates))
      (first predicates)
      (apply list 'and predicates))))

;; Rewrite rules.

(defn- promote-selection-cross-join-to-join [z]
  (r/zmatch z
    [:select predicate
     [:cross-join lhs rhs]]
    ;;=>
    (when-let [join-map (build-join-map predicate lhs rhs)]
      [:join join-map lhs rhs])))

(defn- promote-selection-to-join [z]
  (r/zmatch z
     [:select predicate
      [join-op {} lhs rhs]]
     ;;=>
     (when-let [join-map (build-join-map predicate lhs rhs)]
       [join-op join-map lhs rhs])))

(defn- push-selection-down-past-join [z]
  (letfn [(push-selection-down [predicate lhs rhs]
            (let [expr-table-ids (expr-table-ids predicate)
                  lhs-table-ids (table-ids-in-subtree lhs)
                  rhs-table-ids (table-ids-in-subtree rhs)
                  on-lhs? (set/subset? expr-table-ids lhs-table-ids)
                  on-rhs? (set/subset? expr-table-ids rhs-table-ids)]
              (cond
                (and on-rhs? (not on-lhs?))
                [lhs [:select predicate rhs]]

                (and on-lhs? (not on-rhs?))
                [[:select predicate lhs] rhs])))]
    (r/zmatch z
      [:select predicate
       [join-op join-map lhs rhs]]
      ;;=>
      (when-let [[lhs rhs] (push-selection-down predicate lhs rhs)]
        [join-op join-map lhs rhs])

      [:select predicate
       [:cross-join lhs rhs]]
      ;;=>
      (when-let [[lhs rhs] (push-selection-down predicate lhs rhs)]
        [:cross-join lhs rhs]))))

(defn- push-selections-with-fewer-variables-down [z]
  (r/zmatch z
    [:select predicate-1
     [:select predicate-2
      relation]]
    ;;=>
    (when (< (count (expr-table-ids predicate-1))
             (count (expr-table-ids predicate-2)))
      [:select predicate-2
       [:select predicate-1
        relation]])))

(defn- push-selections-with-equals-down [z]
  (r/zmatch z
    [:select predicate-1
     [:select predicate-2
      relation]]
    ;;=>
    (when (and (equals-predicate? predicate-1)
               (not (equals-predicate? predicate-2)))
      [:select predicate-2
       [:select predicate-1
        relation]])))

(defn- merge-selections-with-same-variables [z]
  (r/zmatch z
    [:select predicate-1
     [:select predicate-2
      relation]]
    ;;=>
    (when (= (expr-table-ids predicate-1) (expr-table-ids predicate-2))
      [:select (merge-conjunctions predicate-1 predicate-2) relation])))

(defn- merge-renames [z]
  (r/zmatch z
    [:rename columns-1
     [:rename columns-2
      relation]]
    ;;=>
    (when (and (map? columns-1) (map? columns-2))
      (let [rename-map (reduce-kv
                        (fn [acc k v]
                          (assoc acc k (get columns-1 v v)))
                        (apply dissoc columns-1 (vals columns-2))
                        columns-2)]
        [:rename (with-meta rename-map (meta columns-2)) relation]))))

(defn- remove-superseded-projects [z]
  (r/zmatch z
    [:project projections-1
     [:rename prefix-or-columns
      [:project projections-2
       relation]]]
    ;;=>
    (when (every? symbol? projections-2)
      [:project projections-1
       [:rename prefix-or-columns
        relation]])

    [:project projections
     [:rename prefix
      [:scan columns]]]
    ;;=>
    (when (and (every? symbol? projections)
               (symbol? prefix)
               (= (count projections) (count columns)))
      [:rename prefix
       [:scan columns]])))

(defn- add-selection-to-scan-predicate [z]
  (r/zmatch z
    [:select predicate
     [:rename prefix [:scan columns]]]
    ;;=>
    (let [new-columns (reduce
                       (fn [acc predicate]
                         (let [expr-symbols (expr-symbols predicate)]
                           (if-let [single-symbol (when (= 1 (count expr-symbols))
                                                    (first expr-symbols))]
                             (vec (for [column-or-select acc
                                        :let [column (if (map? column-or-select)
                                                       (key (first column-or-select))
                                                       column-or-select)]]
                                    (if (= single-symbol (symbol (str prefix relation-prefix-delimiter column)))
                                      (let [predicate (w/postwalk-replace {single-symbol column} predicate)]
                                        (if (map? column-or-select)
                                          (update column-or-select column (partial merge-conjunctions predicate))
                                          {column predicate}))
                                      column-or-select)))
                             acc)))
                       columns
                       (conjunction-clauses predicate))]
      (when-not (= columns new-columns)
        [:select predicate
         [:rename prefix [:scan new-columns]]]))))

(def optimize-plan
  (some-fn promote-selection-cross-join-to-join
           promote-selection-to-join
           push-selection-down-past-join
           push-selections-with-fewer-variables-down
           push-selections-with-equals-down
           merge-selections-with-same-variables
           merge-renames
           remove-superseded-projects
           add-selection-to-scan-predicate))

;; Logical plan API

(defn plan-query [query]
  (if-let [parse-failure (insta/get-failure query)]
    {:errs [(prn-str parse-failure)]}
    (r/with-memoized-attributes [sem/id
                                 sem/ctei
                                 sem/cteo
                                 sem/cte-env
                                 sem/dcli
                                 sem/dclo
                                 sem/env
                                 sem/group-env
                                 sem/projected-columns
                                 sem/column-reference]
      (let [ag (z/vector-zip query)]
        (if-let [errs (not-empty (sem/errs ag))]
          {:errs errs}
          {:plan (->> (z/vector-zip (plan ag))
                      (r/innermost (r/mono-tp optimize-plan))
                      (z/node)
                      (s/assert ::lp/logical-plan))})))))

;; Building plans using the Apply operator:

;; Aims to digest information from the following in terms of our
;; logical plan:

;; http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf "Orthogonal Optimization of Subqueries and Aggregation"
;; http://www.cse.iitb.ac.in/infolab/Data/Courses/CS632/2010/Papers/subquery-proc-elhemali-sigmod07.pdf "Execution Strategies for SQL Subqueries"
;; https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2000-31.pdf "Parameterized Queries and Nesting Equivalences"

;; Introduction:

;; Each subquery has two parts, an expression replacing it in its
;; parent expression, and a relational algebra tree used in the
;; wrapping Apply operator. The replacement expression is usually a
;; fresh variable/column, but could be a constant, like true (when the
;; mode is anti/semi-join).

;; The Apply operator ends up around the existing tree and below the
;; operator introducing the subquery, so if the subquery sits in a
;; select: [:select (... replaced expression ...) [:apply .... input
;; relation subquery] Apply will map the out columns of the subquery
;; to fresh column/variable(s) and these will be used in the replaced
;; expression. The default Apply mode is left-outer-join, but
;; max-1-row may replace this need, and use cross-join?

;; Usage of Apply for expressions can happen in two relational
;; contexts, in a top-level conjunction select or inside a normal
;; select/project. During top-level select, the subquery in the
;; expression may get replaced with the constant true (which could be
;; optimised away) and the Apply operator mode configured to semi/anti
;; etc. Otherwise, one needs to calculate the actual value of the
;; subquery and bind it to a fresh variable/column so it can be used
;; in the original expression, which may be arbitrary complex.

;; The top-level conjunction context could/should be detected and
;; dealt with as a rewrite instead of being generated directly. The
;; main reason to use different modes is to simplify further rewrites
;; to get rid of the Apply operator, which are out of scope for this
;; note.

;; A few special cases:

;; A table subquery may also occur in the FROM clause (and not in an
;; expression as above), in which case it's dealt with separately. If
;; the subquery isn't a LATERAL derived table, it will simply be
;; translated verbatim without the Apply operator. It may still have
;; correlated variables from an outer query, but that would been dealt
;; with higher up. A LATERAL subquery is executed as an Apply cross
;; join where used columns from tables defined to the left may be
;; among its parameters. If there are no columns to the left used, it
;; doesn't need to use the Apply operator.

;; A row subquery may only appear in a few places, like inside a
;; VALUES or IN expression, in which case it will bind N number of
;; original columns to fresh variables/columns, and use
;; max-1-row. (Alternatively, this could be translated into returning
;; a single struct.) It would expand into a literal row where the
;; elements are these columns. [:table [{:a <fv-1>, :b <fv-2>}]
;; [:apply ... [:max-1-row [:project [{<fv-1> <row-col-1} {fv-2
;; <row-col-2}] ...]]]] This is just an example to illustrate the
;; translation, the keys would normally be determined based on the
;; projected columns of the surrounding query. Often the columns would
;; be anonymous and ordinal only. This is a general problem of how to
;; generate plans for VALUES and not specifically to Apply, so not in
;; scope for this note.

;; Conditional evaluation, like CASE and short-circuiting AND/OR (if
;; the spec enforces this) pose additional challenges, but are not
;; currently dealt with in this note.

;; Scalar subqueries:

;; This is the basic case, they introduce a fresh variable/column in
;; its place in the expression, and then an Apply operator where the
;; parameterised relation is wrapped with max-1-row (why would
;; left-outer-join also be the mode?), and the original output column
;; is renamed to its fresh variable/column. This works the same
;; regardless context. [:select (... <fv> ...) [:apply ... [:project
;; [{<fv> <scalar-column>}] ...]] There are situations where one can
;; avoid wrapping via the max-1-row, like if the parameterised
;; relation is a scalar aggregate.

;; Table subqueries:

;; There are three places these can occur inside expressions: (NOT)
;; IN, ALL/ANY and (NOT) EXISTS. (NOT) IN and ALL/ANY are normalised
;; into (NOT) EXISTS. Exactly how this is translated differs between
;; (top-level) select and usage inside other expressions.

;; ALL is translated into NOT EXISTS, where the left hand side is
;; moved into parameterised relation using the inverse comparison
;; operator, where the right hand side is a reference to the scalar
;; column itself. That is, this can be done via wrapping the
;; parameterised relation with [:select (or (<inv-comp-op> <lhs>
;; <scalar-column>) (nil? <lhs>) (nil? <scalar-column>)) ...]. Moving
;; the left hand side into the parameterised expression will introduce
;; correlation on any column references it contains. Left hand side
;; may be arbitrary complex, and may contain further Apply operators.

;; ANY is translated into EXISTS where the left hand side is moved
;; inside to wrap the parameterised relation. [:select (<comp-op>
;; <lhs> <scalar-column>) ...] Correlation maybe introduced as
;; mentioned above.

;; A potential different translation is to pass in lhs as well as a
;; parameter, avoiding introducing correlation. As above, this
;; parameter might need to be calculated via Apply itself if it's
;; complex. This translation diverges more from the literature, and
;; may require adapted rewrite rules to decorrelate.


;; IN is translated into = ANY and then as above.
;; NOT IN is translated into <> ALL and then as above.
;; IN value lists can be seen as an IN with a VALUES subquery.

;; Inside top-level select, the original expression is replaced with
;; true (or dropped) in all the above cases, and the Apply mode is set
;; to semi-join for EXISTS and anti-join for NOT EXISTS.

;; Otherwise, the expression is a fresh variable/column which boolean
;; value is calculated with an parameterised relation as [:project
;; [{<fv-out> (= <fv-in> <N>)}] [:group-by [{<fv-in> (count ...)}]
;; [:top {:limit 1} ....]  where N is 1 for EXISTS and 0 for NOT
;; EXISTS. The Apply mode here is cross join.

;; Note that for ALL/ANY/IN, the subquery may return more than one
;; column, which can be deal with by returning a struct.

;; Correlated variables:

;; In all cases, the dependent variables/columns necessary to give as
;; a parameters to the Apply operator can be calculated via the
;; attributes, in ways similar to how the scope attribute currently
;; does it. Note that variables may need to be made available and
;; passed down several levels to be used in other Apply operators. In
;; these cases the variable itself will already be in the current
;; parameter scope having been passed down, and simply needs to be
;; passed on. Column references to outer columns will be generated as
;; parameters in the plan, that is, they will be prefixed with a
;; question mark.