(ns xtdb.transit
  (:require [clojure.edn :as edn]
            [cognitect.transit :as transit]
            [time-literals.read-write :as time-literals.rw]
            [xtdb.api.protocols :as xtp]
            [xtdb.edn :as xt-edn]
            [xtdb.error :as err]
            [xtdb.types :as types])
  (:import (com.cognitect.transit TransitFactory)
           (java.time DayOfWeek Duration Instant LocalDate LocalDateTime LocalTime Month MonthDay OffsetDateTime OffsetTime Period Year YearMonth ZonedDateTime ZoneId)
           (org.apache.arrow.vector.types.pojo ArrowType Field FieldType)
           (xtdb.api.protocols TransactionInstant)
           (xtdb.types ClojureForm IntervalDayTime IntervalMonthDayNano IntervalYearMonth)))

(def tj-read-handlers
  (merge (-> time-literals.rw/tags
             (update-keys str)
             (update-vals transit/read-handler))
         {"xtdb/clj-form" (transit/read-handler xtp/->ClojureForm)
          "xtdb/tx-key" (transit/read-handler xtp/map->TransactionInstant)
          "xtdb/illegal-arg" (transit/read-handler err/-iae-reader)
          "xtdb/runtime-err" (transit/read-handler err/-runtime-err-reader)
          "xtdb/exception-info" (transit/read-handler #(ex-info (first %) (second %)))
          "xtdb/period-duration" xt-edn/period-duration-reader
          "xtdb.interval/year-month" xt-edn/interval-ym-reader
          "xtdb.interval/day-time" xt-edn/interval-dt-reader
          "xtdb.interval/month-day-nano" xt-edn/interval-mdn-reader
          "xtdb/list" (transit/read-handler edn/read-string)
          "xtdb/arrow-type" (transit/read-handler types/->arrow-type)
          "xtdb/field-type" (transit/read-handler (fn [[arrow-type nullable?]]
                                                    (if nullable?
                                                      (FieldType/nullable arrow-type)
                                                      (FieldType/notNullable arrow-type))))
          "xtdb/field" (transit/read-handler (fn [[name field-type children]]
                                               (Field. name field-type children)))}))


(def tj-write-handlers
  (merge (-> {Period "time/period"
              LocalDate "time/date"
              LocalDateTime "time/date-time"
              ZonedDateTime "time/zoned-date-time"
              OffsetTime "time/offset-time"
              Instant "time/instant"
              OffsetDateTime "time/offset-date-time"
              ZoneId "time/zone"
              DayOfWeek "time/day-of-week"
              LocalTime "time/time"
              Month "time/month"
              Duration "time/duration"
              Year "time/year"
              YearMonth "time/year-month"
              MonthDay "time/month-day"}
             (update-vals #(transit/write-handler % str)))
         {TransactionInstant (transit/write-handler "xtdb/tx-key" #(select-keys % [:tx-id :system-time]))
          xtdb.IllegalArgumentException (transit/write-handler "xtdb/illegal-arg" ex-data)
          xtdb.RuntimeException (transit/write-handler "xtdb/runtime-err" ex-data)
          clojure.lang.ExceptionInfo (transit/write-handler "xtdb/exception-info" #(vector (ex-message %) (ex-data %)))

          ClojureForm (transit/write-handler "xtdb/clj-form" #(.form ^ClojureForm %))

          IntervalYearMonth (transit/write-handler "xtdb.interval/year-month" #(str (.period ^IntervalYearMonth %)))

          IntervalDayTime (transit/write-handler "xtdb.interval/day-time"
                                                 #(vector (str (.period ^IntervalDayTime %))
                                                          (str (.duration ^IntervalDayTime %))))

          IntervalMonthDayNano (transit/write-handler "xtdb.interval/month-day-nano"
                                                      #(vector (str (.period ^IntervalMonthDayNano %))
                                                               (str (.duration ^IntervalMonthDayNano %))))
          clojure.lang.PersistentList (transit/write-handler "xtdb/list" #(pr-str %))
          ArrowType (transit/write-handler "xtdb/arrow-type" #(types/<-arrow-type %))
          ;; beware that this currently ignores dictionary encoding and metadata of FieldType's
          FieldType (transit/write-handler "xtdb/field-type"
                                           (fn [^FieldType field-type]
                                             (TransitFactory/taggedValue "array" [(.getType field-type) (.isNullable field-type)])))
          Field (transit/write-handler "xtdb/field"
                                       (fn [^Field field]
                                         (TransitFactory/taggedValue "array" [(.getName field) (.getFieldType field) (.getChildren field)])))}))