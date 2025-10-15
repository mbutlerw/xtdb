(ns xtdb.operator.select
  (:require [clojure.spec.alpha :as s]
            [xtdb.coalesce :as coalesce]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types])
  (:import (xtdb ICursor)
           (xtdb.operator SelectCursor)))

(defmethod lp/ra-expr :select [_]
  (s/cat :op #{:σ :sigma :select}
         :predicate ::lp/expression
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(defmethod lp/emit-expr :select [{:keys [predicate relation]} {:keys [param-fields] :as args}]
  (lp/unary-expr (lp/emit-expr relation args)
    (fn [{inner-fields :fields, inner-stats :stats :as inner-rel}]
      (let [input-types {:col-types (update-vals inner-fields types/field->col-type)
                         :param-types (update-vals param-fields types/field->col-type)}
            selector (expr/->expression-selection-spec (expr/form->expr predicate input-types) input-types)]
        {:op :select
         :stats inner-stats
         :children [inner-rel]
         :explain  {:predicate (pr-str predicate)}
         :fields inner-fields
         :->cursor (fn [{:keys [allocator args schema explain-analyze?]} in-cursor]
                     (cond-> (-> (SelectCursor. allocator in-cursor selector schema args)
                                 (coalesce/->coalescing-cursor allocator))
                       explain-analyze? (ICursor/wrapExplainAnalyze)))}))))
