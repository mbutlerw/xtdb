(ns xtdb.expression.pg
  (:require [xtdb.expression :as expr]
            [clojure.string :as str]
            [xtdb.error :as err]
            [clojure.math :as math]))

(set! *unchecked-math* :warn-on-boxed)

(defn table-paths
  "returns possible table paths based on search path for unqualified names"
  [table]
  (let [parts (str/split table #"\.")]
    (if (= 1 (count parts))
      (mapv #(vector % (first parts)) expr/search-path)
     [parts])))

(defn string-table-name [[schema-name table-name]]
  (if (contains? (set expr/search-path) schema-name)
    table-name
    (str schema-name "." table-name)))

(defn find-matching-table-oid [schema tn]
  (some #(get-in schema (conj (vec %) "_oid")) (table-paths tn)))

(defn find-table-name-by-oid [schema oid]
  (->> schema
       (keep
        (fn [[schema-name tables]]
          (first (keep (fn [[table cols]]
                         (when (= oid (get cols "_oid"))
                           [schema-name table]))
                       tables))))
       (first)))

(defmethod expr/codegen-cast [:utf8 :regclass] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code
   #(do
      `(let [tn# (expr/buf->str ~@%)]
         (if-let [matching-table-oid# (find-matching-table-oid ~expr/schema-sym tn#)]
           matching-table-oid#
           (throw (err/runtime-err ::unknown-relation
                                   {::err/message (format "Relation %s does not exist" tn#)})))))})

(defmethod expr/codegen-cast [:regclass :utf8] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code
   #(do
      `(let [oid# ~@%]
         (if-let [tn# (find-table-name-by-oid ~expr/schema-sym oid#)]
          (expr/resolve-utf8-buf (string-table-name tn#))
          (expr/resolve-utf8-buf (str oid#)))))})

(defmethod expr/codegen-cast [:regclass :int] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code #(do `(do ~@%))})

(defmethod expr/codegen-cast [:int :regclass] [{:keys [target-type]}]
  {:return-type target-type
   :->call-code #(do `(do ~@%))})
