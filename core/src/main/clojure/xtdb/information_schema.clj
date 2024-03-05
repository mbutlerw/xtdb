(ns xtdb.information-schema
  (:require xtdb.metadata
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import
   (org.apache.arrow.vector VectorSchemaRoot)
   (org.apache.arrow.vector.types.pojo Schema Field)
   xtdb.operator.IRelationSelector
   (xtdb.vector RelationReader IVectorWriter)
   (xtdb ICursor)
   (xtdb.metadata IMetadataManager)
   (xtdb.watermark Watermark)))

;;TODO add temporal cols

(defn schema-info->col-rows [schema-info]
  (for [table-entry schema-info
        [idx col] (map-indexed #(vector %1 %2) (val table-entry))
        :let [table (key table-entry)
              name (key col)
              ^Field col-field (val col)
              col-types (or (seq (.getChildren col-field)) [col-field])]]

    {:idx idx
     :table table
     :name name
     :type (map #(types/arrow-type->col-type (.getType ^Field %)) col-types)}))

(def info-tables {"tables" {"table_catalog" (types/col-type->field "table_catalog" :utf8)
                            "table_schema" (types/col-type->field "table_schema" :utf8)
                            "table_name" (types/col-type->field "table_name" :utf8)
                            "table_type" (types/col-type->field "table_type" :utf8)}
                  "pg_tables" {"schemaname" (types/col-type->field "schemaname" :utf8)
                               "tablename" (types/col-type->field "tablename" :utf8)
                               "tableowner" (types/col-type->field "tableowner" :utf8)
                               "tablespace" (types/col-type->field "tablespace" :null)}
                  "pg_views" {"schemaname" (types/col-type->field "schemaname" :utf8)
                              "viewname" (types/col-type->field "viewname" :utf8)
                              "viewowner" (types/col-type->field "viewowner" :utf8)}
                  "pg_matviews" {"schemaname" (types/col-type->field "schemaname" :utf8)
                                 "matviewname" (types/col-type->field "matviewname" :utf8)
                                 "matviewowner" (types/col-type->field "matviewowner" :utf8)}
                  "columns" {"table_catalog" (types/col-type->field "table_catalog" :utf8)
                             "table_schema" (types/col-type->field "table_schema" :utf8)
                             "table_name" (types/col-type->field "table_name" :utf8)
                             "column_name" (types/col-type->field "column_name" :utf8)
                             "data_type" (types/col-type->field "data_type" [:list :utf8])}
                  "pg_attribute" {"attrelid" (types/col-type->field "attrelid" :i32)
                                  "attname" (types/col-type->field "attname" :utf8)
                                  "atttypid" (types/col-type->field "atttypid" :i32)
                                  "attlen" (types/col-type->field "attlen" :i16)
                                  "attnum" (types/col-type->field "attnum" :i16)}
                  "schemata" {"catalog_name" (types/col-type->field "catalog_name" :utf8)
                              "schema_name" (types/col-type->field "schema_name" :utf8)
                              "schema_owner" (types/col-type->field "schema_owner" :utf8)}
                  "pg_namespace" {"oid" (types/col-type->field "oid" :i32)
                                  "nspname" (types/col-type->field "nspname" :utf8)
                                  "nspowner" (types/col-type->field "nspowner" :i32)
                                  "nspacl" (types/col-type->field "nspacl" :null)}})

(def info-table-cols (update-vals info-tables (comp set keys)))

(def schemas [{"catalog_name" "default"
               "schema_name" "pg_catalog"
               "schema_owner" "default"}
              {"catalog_name" "default"
               "schema_name" "public"
               "schema_owner" "default"}
              {"catalog_name" "default"
               "schema_name" "information_schema"
               "schema_owner" "default"}])

(def pg-namespaces (map (fn [{:strs [schema_owner schema_name] :as schema}]
                          (-> schema
                              (assoc "nspowner" (hash schema_owner))
                              (assoc "oid" (hash schema_name))))
                        schemas))

(defn empty-rel [allocator relation-definition col-names]
  (let [schema (Schema. (or (vals (select-keys relation-definition col-names)) []))]
    (util/with-close-on-catch [root (VectorSchemaRoot/create schema allocator)]
      (let [rel-wtr (vw/root->writer root)]
        {:rel (vw/rel-wtr->rdr rel-wtr)
         :vsr root}))))

(defn tables [allocator schema-info col-names]
  (let [schema (Schema. (or (vals (select-keys (get info-tables "tables") col-names)) []))]
    (util/with-close-on-catch [root (VectorSchemaRoot/create schema allocator)]
      (let [rel-wtr (vw/root->writer root)]
        (doseq [table (keys schema-info)]
          (.startRow rel-wtr)
          (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
            (case col
              "table_catalog" (.writeObject col-wtr "default")
              "table_name" (.writeObject col-wtr table)
              "table_schema" (.writeObject col-wtr "public")
              "table_type" (.writeObject col-wtr "BASE TABLE")))
          (.endRow rel-wtr))
        (.syncRowCount rel-wtr)
        {:rel (vw/rel-wtr->rdr rel-wtr)
         :vsr root}))))

(defn pg-tables [allocator schema-info col-names]
  (let [schema (Schema. (or (vals (select-keys (get info-tables "pg_tables") col-names)) []))]
    (util/with-close-on-catch [root (VectorSchemaRoot/create schema allocator)]
      (let [rel-wtr (vw/root->writer root)]
        (doseq [table (keys schema-info)]
          (.startRow rel-wtr)
          (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
            (case col
              "schemaname" (.writeObject col-wtr "public")
              "tablename" (.writeObject col-wtr table)
              "tableowner" (.writeObject col-wtr "default")
              "tablespace" (.writeObject col-wtr nil)))
          (.endRow rel-wtr))
        (.syncRowCount rel-wtr)
        {:rel (vw/rel-wtr->rdr rel-wtr)
         :vsr root}))))

(defn columns [allocator col-rows col-names]
  (let [schema (Schema. (or (vals (select-keys (get info-tables "columns") col-names)) []))]
    (util/with-close-on-catch [root (VectorSchemaRoot/create schema allocator)]
      (let [rel-wtr (vw/root->writer root)]
        (doseq [{:keys [table name type]} col-rows]
          (.startRow rel-wtr)
          (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
            (case col
              "table_catalog" (.writeObject col-wtr "default")
              "table_name" (.writeObject col-wtr table)
              "table_schema" (.writeObject col-wtr "public")
              "column_name" (.writeObject col-wtr name)
              "data_type" (let [el-wtr (.listElementWriter col-wtr)]
                            (.startList col-wtr)
                            (doseq [type-el type]
                              (.writeObject el-wtr type-el))
                            (.endList col-wtr))))
          (.endRow rel-wtr)
          (.syncRowCount rel-wtr))
        {:rel (vw/rel-wtr->rdr rel-wtr)
         :vsr root}))))

(defn pg-attribute [allocator col-rows col-names]
  (let [schema (Schema. (or (vals (select-keys (get info-tables "pg_attribute") col-names)) []))]
    (util/with-close-on-catch [root (VectorSchemaRoot/create schema allocator)]
      (let [rel-wtr (vw/root->writer root)]
        (doseq [{:keys [idx table name _type]} col-rows]
          (.startRow rel-wtr)
          (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
            (case col
              "attrelid" (.writeInt col-wtr (hash table))
              "attname" (.writeObject col-wtr name)
              "atttypid" (.writeInt col-wtr 114) ;; = json - avoiding circular dep on pgwire.clj
              "attlen" (.writeShort col-wtr -1)
              "attnum" (.writeShort col-wtr idx)))
          (.endRow rel-wtr)
          (.syncRowCount rel-wtr))
        {:rel (vw/rel-wtr->rdr rel-wtr)
         :vsr root}))))

(defn schemata [allocator col-names]
  (let [schema (Schema. (or (vals (select-keys (get info-tables "schemata") col-names)) []))]
    (util/with-close-on-catch [root (VectorSchemaRoot/create schema allocator)]
      (let [rel-wtr (vw/root->writer root)]
        (doseq [{:strs [catalog_name schema_name schema_owner]} schemas]
          (.startRow rel-wtr)
          (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
            (case col
              "catalog_name" (.writeObject col-wtr catalog_name)
              "schema_name" (.writeObject col-wtr schema_name)
              "schema_owner" (.writeObject col-wtr schema_owner)))
          (.endRow rel-wtr)
          (.syncRowCount rel-wtr))
        {:rel (vw/rel-wtr->rdr rel-wtr)
         :vsr root}))))

(defn pg-namespace [allocator col-names]
  (let [schema (Schema. (or (vals (select-keys (get info-tables "pg_namespace") col-names)) []))]
    (util/with-close-on-catch [root (VectorSchemaRoot/create schema allocator)]
      (let [rel-wtr (vw/root->writer root)]
        (doseq [{:strs [oid schema_name nspowner]} pg-namespaces]
          (.startRow rel-wtr)
          (doseq [[col ^IVectorWriter col-wtr] rel-wtr]
            (case col
              "oid" (.writeInt col-wtr oid)
              "nspname" (.writeObject col-wtr schema_name)
              "nspowner" (.writeInt col-wtr nspowner)
              "nspacl" (.writeObject col-wtr nil)))
          (.endRow rel-wtr)
          (.syncRowCount rel-wtr))
        {:rel (vw/rel-wtr->rdr rel-wtr)
         :vsr root}))))

(deftype InformationSchemaCursor [^:unsynchronized-mutable ^RelationReader out-rel vsr]
  ICursor
  (tryAdvance [this c]
    (boolean
     (when-let [out-rel out-rel]
       (try
         (set! (.out-rel this) nil)
         (.accept c out-rel)
         true
         (finally
           (util/close vsr)
           (.close out-rel))))))

  (close [_]
    (util/close vsr)
    (some-> out-rel .close)))


(defn ->cursor [allocator _schema table col-names col-preds params ^IMetadataManager metadata-mgr ^Watermark wm]
  (let [schema-info (merge-with merge
                                (.allColumnFields metadata-mgr)
                                (some-> (.liveIndex wm)
                                        (.allColumnFields)))
        {:keys [rel vsr]}
        (case table
          "tables" (tables allocator schema-info col-names)
          "pg_tables" (pg-tables allocator schema-info col-names)
          "pg_views" (empty-rel allocator (get info-tables "pg_views") col-names)
          "pg_matviews" (empty-rel allocator (get info-tables "pg_matviews") col-names)
          "columns" (columns allocator (schema-info->col-rows schema-info) col-names)
          "pg_attribute" (pg-attribute allocator (schema-info->col-rows schema-info) col-names)
          "schemata" (schemata allocator col-names)
          "pg_namespace" (pg-namespace allocator col-names)
          (throw (UnsupportedOperationException. (str "Information Schema table does not exist: " table))))]

    ;;TODO reuse relation selector code from tri cursor
    (InformationSchemaCursor. (reduce (fn [^RelationReader rel ^IRelationSelector col-pred]
                                        (.select rel (.select col-pred allocator rel params)))
                                      (-> rel
                                          (vr/with-absent-cols allocator col-names))
                                      (vals col-preds)) vsr)))
