(ns xtdb.sql-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.logical-plan :as lp]
            [xtdb.serde :as serde]
            [xtdb.sql :as sql]
            [xtdb.sql.plan2 :as plan]
            [xtdb.test-util :as tu])

  (:import (java.time.zone ZoneRulesException)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(defn plan-sql
  ([sql opts] (sql/compile-query sql opts))
  ([sql] (plan-sql sql {})))

(def regen-expected-files? false) ;; <<no-commit>>

(defmethod t/assert-expr '=plan-file [msg form]
  `(let [exp-plan-file-name# ~(nth form 1)
         exp-plan-file-path# (format "xtdb/sql/plan_test_expectations/%s.edn" exp-plan-file-name#)
         actual-plan# ~(nth form 2)]
     (binding [*print-namespace-maps* false]
       (if-let [exp-plan-file# (io/resource exp-plan-file-path#)]
         (let [exp-plan# (read-string (slurp exp-plan-file#))
               result# (= exp-plan# actual-plan#)]
           (if result#
             (t/do-report {:type :pass
                           :message ~msg
                           :expected (list '~'= exp-plan-file-name# actual-plan#)
                           :actual (list '~'= exp-plan# actual-plan#)})
             (do
               (when regen-expected-files?
                 (spit (io/resource exp-plan-file-path#) (with-out-str (clojure.pprint/pprint actual-plan#))))
               (t/do-report {:type :fail
                             :message ~msg
                             :expected (list '~'= exp-plan-file-name# actual-plan#)
                             :actual (list '~'not (list '~'= exp-plan# actual-plan#))})))
           result#)
         (if regen-expected-files?
           (do
             (spit
              (str (io/resource "xtdb/sql/plan_test_expectations/") exp-plan-file-name# ".edn")
              (with-out-str (clojure.pprint/pprint actual-plan#))))
           (t/do-report
            {:type :error, :message "Missing Expectation File"
             :expected exp-plan-file-path#  :actual (Exception. "Missing Expectation File")}))))))

(defn plan-expr-with-foo [expr]
  (plan/plan-expr expr {:scopes 
                        (list (plan/->Scope {"foo" (plan/->BaseTable nil nil "foo" "foo" #{"a" "b"} (atom #{}))}))}))

(deftest test-basic-queries
  (t/is (=plan-file
          "basic-query-1"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate = 1960"
                    {:table-info {"stars_in" #{"movie_title" "star_name" "year"}
                                  "movie_star" #{"name" "birthdate"}}})))

  (t/is (=plan-file
          "basic-query-2"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.birthdate > 1950"
                    {:table-info {"stars_in" #{"movie_title" "star_name" "year"}
                                  "movie_star" #{"name" "birthdate"}}})))

  (t/is (=plan-file
          "basic-query-3"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, MovieStar AS ms WHERE si.starName = ms.name AND ms.birthdate < 1960 AND ms.name = 'Foo'"
                    {:table-info {"stars_in" #{"movie_title" "star_name" "year"}
                                  "movie_star" #{"name" "birthdate"}}})))

  (t/is (=plan-file
          "basic-query-4"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si, (SELECT ms.name FROM MovieStar AS ms WHERE ms.birthdate = 1960) AS m WHERE si.starName = m.name"
                    {:table-info {"stars_in" #{"movie_title" "star_name" "year"}
                                  "movie_star" #{"name" "birthdate"}}})))

  (t/is (=plan-file
          "basic-query-5"
          (plan-sql "SELECT si.movieTitle FROM Movie AS m JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear"
                    {:table-info {"movie" #{"title" "movie_year"}
                                  "stars_in" #{"movie_title" "year"}}})))

  (t/is (=plan-file
          "basic-query-6"
          (plan-sql "SELECT si.movieTitle FROM Movie AS m LEFT JOIN StarsIn AS si ON m.title = si.movieTitle AND si.year = m.movieYear"
                    {:table-info {"movie" #{"title" "movie_year"}
                                  "stars_in" #{"movie_title" "year"}}})))

  (t/is (=plan-file
          "basic-query-7"
          (plan-sql "SELECT si.title FROM Movie AS m JOIN StarsIn AS si USING (title)"
                    {:table-info {"movie" #{"title"}
                                  "stars_in" #{"title"}}})))

  (t/is (=plan-file
          "basic-query-8"
          (plan-sql "SELECT si.title FROM Movie AS m RIGHT OUTER JOIN StarsIn AS si USING (title)"
                    {:table-info {"movie" #{"title"}
                                  "stars_in" #{"title"}}})))

  (t/is (=plan-file
          "basic-query-9"
          (plan-sql "SELECT me.name, SUM(m.length) FROM MovieExec AS me, Movie AS m WHERE me.cert = m.producer GROUP BY me.name HAVING MIN(m.year) < 1930"
                    {:table-info {"movie_exec" #{"name" "cert"}
                                  "movie" #{"producer" "year"}}})))

  (t/is (=plan-file
          "basic-query-10"
          (plan-sql "SELECT SUM(m.length) FROM Movie AS m"
                    {:table-info {"movie" #{"length"}}})))

  (t/is (=plan-file
          "basic-query-11"
          (plan-sql "SELECT * FROM StarsIn AS si(name)"
                    {:table-info {"stars_in" #{"name"}}})))

  (t/is (=plan-file
          "basic-query-11"
          (plan-sql "FROM StarsIn AS si(name)"
                    {:table-info {"stars_in" #{"name"}}}))
        "implicit SELECT *")

  (t/is (=plan-file
          "basic-query-12"
          (plan-sql "SELECT * FROM (SELECT si.name FROM StarsIn AS si) AS foo(bar)"
                    {:table-info {"stars_in" #{"name"}}})))

  (t/is (=plan-file
          "basic-query-12"
          (plan-sql "FROM (SELECT si.name FROM StarsIn AS si) AS foo(bar)"
                    {:table-info {"stars_in" #{"name"}}}))
        "implicit SELECT *")

  (t/is (=plan-file
         "basic-query-13"
         (plan-sql "SELECT si.* FROM StarsIn AS si WHERE si.name = si.lastname"
                   {:table-info {"stars_in" #{"name" "lastname"}}})))

  (t/is (=plan-file
          "basic-query-14"
           (plan-sql "SELECT DISTINCT si.movieTitle FROM StarsIn AS si"
                     {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-15"
          (plan-sql "SELECT si.name FROM StarsIn AS si EXCEPT SELECT si.name FROM StarsIn AS si"
                    {:table-info {"stars_in" #{"name"}}})))

  (t/is (=plan-file
          "basic-query-16"
           (plan-sql "SELECT si.name FROM StarsIn AS si UNION ALL SELECT si.name FROM StarsIn AS si"
                     {:table-info {"stars_in" #{"name"}}})))

  (t/is (=plan-file
          "basic-query-17"
          (plan-sql "SELECT si.name FROM StarsIn AS si INTERSECT SELECT si.name FROM StarsIn AS si"
                    {:table-info {"stars_in" #{"name"}}})))

  (t/is (=plan-file
          "basic-query-18"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si"
                    {:table-info {"stars_in" #{"movie_title" "name"}}})))

  (t/is (=plan-file
          "basic-query-19"
          (plan-sql "SELECT si.name FROM StarsIn AS si UNION SELECT si.name FROM StarsIn AS si ORDER BY name"
                    {:table-info {"stars_in" #{"name"}}})))

  (t/is (=plan-file
          "basic-query-20"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si FETCH FIRST 10 ROWS ONLY"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-21"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 ROWS"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-22"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si OFFSET 5 LIMIT 10"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-23"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-24"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.movieTitle OFFSET 100 ROWS"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-25"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY movieTitle DESC"
                    {:table-info {"stars_in" #{"movie_title"}}})))

  (t/is (=plan-file
          "basic-query-26"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year = 'foo' DESC, movieTitle"
                    {:table-info {"stars_in" #{"movie_title" "year"}}})))

  (t/is (=plan-file
          "basic-query-27"
          (plan-sql "SELECT si.movieTitle FROM StarsIn AS si ORDER BY si.year"
                    {:table-info {"stars_in" #{"movie_title" "year"}}})))

  (t/is (=plan-file
          "basic-query-28"
           (plan-sql "SELECT si.year = 'foo' FROM StarsIn AS si ORDER BY si.year = 'foo'"
                     {:table-info {"stars_in" #{"year"}}})))

  (t/is (=plan-file
          "basic-query-29"
          (plan-sql "SELECT film.name FROM StarsIn AS si, UNNEST(si.films) AS film(name)"
                    {:table-info {"stars_in" #{"films"}}})))

  (t/is (=plan-file
          "basic-query-30"
          (plan-sql "SELECT * FROM StarsIn AS si(films), UNNEST(si.films) AS film"
                    {:table-info {"stars_in" #{"films"}}})))

  (t/is (=plan-file
          "basic-query-30"
          (plan-sql "FROM StarsIn, UNNEST(films) AS film"
                    {:table-info {"stars_in" #{"films"}}}))

        "implicit SELECT *")

  (t/is (=plan-file
          "basic-query-31"
          (plan-sql "SELECT * FROM StarsIn AS si, UNNEST(si.films) WITH ORDINALITY AS film"
                    {:table-info {"stars_in" #{"films"}}})))

  (t/is (=plan-file
          "basic-query-31"
          (plan-sql "FROM StarsIn AS si, UNNEST(si.films) WITH ORDINALITY AS film"
                    {:table-info {"stars_in" #{"films"}}}))
        "implicit SELECT *")

  (t/is (=plan-file
          "basic-query-32"
          (plan-sql "VALUES (1, 2), (3, 4)")))

  (t/is (=plan-file
          "basic-query-33"
           (plan-sql "VALUES 1, 2")))

  (t/is (=plan-file
          "basic-query-34"
          (plan-sql "SELECT CASE t1.a + 1 WHEN t1.b THEN 111 WHEN t1.c THEN 222 WHEN t1.d THEN 333 WHEN t1.e THEN 444 ELSE 555 END,
                    CASE WHEN t1.a < t1.b - 3 THEN 111 WHEN t1.a <= t1.b THEN 222 WHEN t1.a < t1.b+3 THEN 333 ELSE 444 END,
                    CASE t1.a + 1 WHEN t1.b, t1.c THEN 222 WHEN t1.d, t1.e + 1 THEN 444 ELSE 555 END FROM t1"
                    {:table-info {"t1" #{"a" "b" "c" "d" "e"}}})))

  (t/is (=plan-file
          "basic-query-35"
          (plan-sql "SELECT * FROM t1 AS t1(a) WHERE t1.a IS NULL"
                    {:table-info {"t1" #{"a"}}})))

  (t/is (=plan-file
          "basic-query-35"
          (plan-sql "FROM t1 AS t1(a) WHERE t1.a IS NULL"
                    {:table-info {"t1" #{"a"}}}))
        "implicit SELECT *")

  (t/is (=plan-file
          "basic-query-36"
          (plan-sql "SELECT * FROM t1 WHERE t1.a IS NOT NULL"
                    {:table-info {"t1" #{"a"}}})))

  (t/is (=plan-file
          "basic-query-36"
          (plan-sql "FROM t1 WHERE t1.a IS NOT NULL"
                    {:table-info {"t1" #{"a"}}}))
        "implicit SELECT *")

  (t/is (=plan-file
          "basic-query-37"
          (plan-sql "SELECT NULLIF(t1.a, t1.b) FROM t1"
                    {:table-info {"t1" #{"a" "b"}}}))))

;; TODO: sanity check semantic analysis for correlation both inside
;; and outside MAX, gives errors in both cases, are these correct?
;; SELECT MAX(foo.bar) FROM foo

(deftest test-subqueries
  (t/testing "Scalar subquery in SELECT"
    (t/is (=plan-file
            "scalar-subquery-in-select"
            (plan-sql "SELECT (1 = (SELECT MAX(foo.bar) FROM foo)) AS some_column FROM x WHERE x.y = 1"
                      {:table-info {"x" #{"y"}
                                    "foo" #{"bar"}}}))))

  (t/testing "Scalar subquery in WHERE"
    (t/is (=plan-file
            "scalar-subquery-in-where"
            (plan-sql "SELECT x.y AS some_column FROM x WHERE x.y = (SELECT MAX(foo.bar) FROM foo)"
                      {:table-info {"x" #{"y"}
                                    "foo" #{"bar"}}}))))

  (t/testing "Correlated scalar subquery in SELECT"
    (t/is (=plan-file
            "correlated-scalar-subquery-in-select"
            (plan-sql "SELECT (1 = (SELECT bar = z FROM foo)) AS some_column FROM x WHERE y = 1"
                      {:table-info {"x" #{"y" "z"}
                                    "foo" #{"bar"}}}))))

  (t/testing "EXISTS in WHERE"
    (t/is (=plan-file
            "exists-in-where"
             (plan-sql "SELECT x.y FROM x WHERE EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10.0"
                       {:table-info {"x" #{"y" "z"}
                                     "y" #{"z"}}}))))

  (t/testing "EXISTS as expression in SELECT"
    (t/is (=plan-file
            "exists-as-expression-in-select"
            (plan-sql "SELECT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) FROM x WHERE x.z = 10"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}}))))

  (t/testing "NOT EXISTS in WHERE"
    (t/is (=plan-file
            "not-exists-in-where"
            (plan-sql "SELECT x.y FROM x WHERE NOT EXISTS (SELECT y.z FROM y WHERE y.z = x.y) AND x.z = 10"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}}))))

  (t/testing "IN in WHERE"
    (t/is (=plan-file
            "in-in-where-select"
            (plan-sql "SELECT x.y FROM x WHERE x.z IN (SELECT y.z FROM y)"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}})))

    (t/is (=plan-file
            "in-in-where-set"
            (plan-sql "SELECT x.y FROM x WHERE x.z IN (1, 2)"
                      {:table-info {"x" #{"y" "z"}}}))))

  (t/testing "NOT IN in WHERE"
    (t/is (=plan-file
            "not-in-in-where"
            (plan-sql "SELECT x.y FROM x WHERE x.z NOT IN (SELECT y.z FROM y)"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}}))))

  (t/testing "ALL in WHERE"
    (t/is (=plan-file
            "all-in-where"
            (plan-sql "SELECT x.y FROM x WHERE x.z > ALL (SELECT y.z FROM y)"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}}))))

  (t/testing "ANY in WHERE"
    (t/is (=plan-file
            "any-in-where"
            (plan-sql "SELECT x.y FROM x WHERE (x.z = 1) > ANY (SELECT y.z FROM y)"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}}))))

  (t/testing "ALL as expression in SELECT"
    (t/is (=plan-file
            "all-as-expression-in-select"
            (plan-sql "SELECT x.z <= ALL (SELECT y.z FROM y) FROM x"
                      {:table-info {"x" #{"y" "z"}
                                    "y" #{"z"}}}))))

  (t/testing "LATERAL derived table"
    (t/is (=plan-file
            "lateral-derived-table-1"
            (plan-sql "SELECT x.y, y.z FROM x, LATERAL (SELECT z.z FROM z WHERE z.z = x.y) AS y"
                      {:table-info {"x" #{"y"}
                                    "z" #{"z"}}})))

    (t/is (=plan-file
            "lateral-derived-table-2"
            (plan-sql "SELECT y.z FROM LATERAL (SELECT z.z FROM z WHERE z.z = 1) AS y"
                      {:table-info {"z" #{"z"}}}))))

  (t/testing "decorrelation"
    ;; http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf "Orthogonal Optimization of Subqueries and Aggregation"
    (t/is (=plan-file
            "decorrelation-1"
            (plan-sql "SELECT c.custkey FROM customer c
                      WHERE 1000000 < (SELECT SUM(o.totalprice) FROM orders o WHERE o.custkey = c.custkey)"
                      {:table-info {"customer" #{"custkey"}
                                    "orders" #{"custkey" "totalprice"}}})))

    ;; https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2000-31.pdf "Parameterized Queries and Nesting Equivalences"
    (t/is (=plan-file
            "decorrelation-2"
            (plan-sql "SELECT * FROM customers AS customers(country, custno)
                      WHERE customers.country = 'Mexico' AND
                      EXISTS (SELECT * FROM orders AS orders(custno) WHERE customers.custno = orders.custno)"
                      {:table-info {"customers" #{"country" "custno"}
                                    "orders" #{"custno"}}})))

    ;; NOTE: these below simply check what's currently being produced,
    ;; not necessarily what should be produced.
    (t/is (=plan-file
            "decorrelation-3"
            (plan-sql "SELECT customers.name, (SELECT COUNT(*) FROM orders WHERE customers.custno = orders.custno)
                      FROM customers WHERE customers.country <> ALL (SELECT salesp.country FROM salesp)"
                      {:table-info {"customers" #{"name" "custno" "country"}
                                    "orders" #{"custno"}
                                    "salesp" #{"country"}}})))

    ;; https://subs.emis.de/LNI/Proceedings/Proceedings241/383.pdf "Unnesting Arbitrary Queries"
    (t/is (=plan-file
            "decorrelation-4"
            (plan-sql "SELECT s.name, e.course
                      FROM students s, exams e
                      WHERE s.id = e.sid AND
                      e.grade = (SELECT MIN(e2.grade)
                      FROM exams e2
                      WHERE s.id = e2.sid)"
                      {:table-info {"students" #{"id"}
                                    "exams" #{"sid" "grade" "course"}}})))

    (t/is (=plan-file
            "decorrelation-5"
            (plan-sql
              "SELECT s.name, e.course
              FROM students s, exams e
              WHERE s.id = e.sid AND
              (s.major = 'CS' OR s.major = 'Games Eng') AND
              e.grade >= (SELECT AVG(e2.grade) + 1
              FROM exams e2
              WHERE s.id = e2.sid OR
              (e2.curriculum = s.major AND
              s.year > e2.date))"
              {:table-info {"students" #{"id" "major" "name" "year"}
                            "exams" #{"sid" "grade" "course" "curriculum" "date"}}})))


    (t/testing "Subqueries in join conditions"

      (->> "uncorrelated subquery"
           (t/is (=plan-file
                   "subquery-in-join-uncorrelated-subquery"
                   (plan-sql "select foo.a from foo join bar on bar.c = (select foo.b from foo)"))))

      (->> "correlated subquery"
           (t/is (=plan-file
                   "subquery-in-join-correlated-subquery"
                   (plan-sql "select foo.a from foo join bar on bar.c in (select foo.b from foo where foo.a = bar.b)"))))

      ;; TODO unable to decorr, need to be able to pull the select over the max-1-row
      ;; although should be able to do this now, no such thing as max-1-row any more
      (->> "correlated equalty subquery"
           (t/is (=plan-file
                   "subquery-in-join-correlated-equality-subquery"
                    (plan-sql "select foo.a from foo join bar on bar.c = (select foo.b from foo where foo.a = bar.b)")))))))

(t/deftest parameters-referenced-in-relation-test
  (t/are [expected plan apply-columns]
         (= expected (lp/parameters-referenced-in-relation? plan (vals apply-columns)))
         true '[:table [{x6 ?x8}]] '{x2 ?x8}
         false '[:table [{x6 ?x4}]] '{x2 ?x8}))

(deftest non-semi-join-subquery-optimizations-test
  (t/is (=plan-file
          "non-semi-join-subquery-optimizations-test-1"
          (plan-sql "select f.a from foo f where f.a in (1,2) or f.b = 42"))
        "should not be decorrelated")
  (t/is (=plan-file
          "non-semi-join-subquery-optimizations-test-2"
          (plan-sql "select f.a from foo f where true = (EXISTS (SELECT foo.c from foo))"))
        "should be decorrelated as a cross join, not a semi/anti join"))

(deftest multiple-ins-in-where-clause
  (t/is (=plan-file
          "multiple-ins-in-where-clause"
          (plan-sql "select f.a from foo f where f.a in (1,2) AND f.a = 42 AND f.b in (3,4)"))))

#_ ; FIXME broken
(deftest deeply-nested-correlated-query
  (t/is (=plan-file
          "deeply-nested-correlated-query"
          (plan-sql "SELECT R1.A, R1.B
                    FROM R R1, S
                    WHERE EXISTS
                    (SELECT R2.A, R2.B
                    FROM R R2
                    WHERE R2.A = R1.B AND EXISTS
                    (SELECT R3.A, R3.B
                    FROM R R3
                    WHERE R3.A = R2.B AND R3.B = S.C))"))))

(t/deftest test-array-element-reference-107
  (t/is (=plan-file
          "test-array-element-reference-107-1"
          (plan-sql "SELECT u.a[1] AS first_el FROM u"
                    {:table-info {"u" #{"a"}}})))

  (t/is (=plan-file
          "test-array-element-reference-107-2"
          (plan-sql "SELECT u.b[u.a[1]] AS dyn_idx FROM u"
                    {:table-info {"u" #{"a" "b"}}}))))

(t/deftest test-current-time-111
  (t/is (=plan-file
          "test-current-time-111"
          (plan-sql "
                    SELECT u.a,
                    CURRENT_TIME, CURRENT_TIME(2),
                    CURRENT_DATE,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(4),
                    LOCALTIME, LOCALTIME(6),
                    LOCALTIMESTAMP, LOCALTIMESTAMP(9),
                    END_OF_TIME, END_OF_TIME()
                    FROM u"
                    {:table-info {"u" #{"a"}}}))))

(t/deftest test-dynamic-parameters-103
  (t/is (=plan-file
         "test-dynamic-parameters-103-1"
         (plan-sql "SELECT foo.a FROM foo WHERE foo.b = ? AND foo.c = ?"
                   {:table-info {"foo" #{"a" "b" "c"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-2"
         (plan-sql "SELECT foo.a
                    FROM foo, (SELECT bar.b FROM bar WHERE bar.c = ?) bar (b)
                    WHERE foo.b = ? AND foo.c = ?"
                   {:table-info {"foo" #{"a" "b" "c"}
                                 "bar" #{"b" "c"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-subquery-project"
         (plan-sql "SELECT t1.col1, (SELECT ? FROM bar WHERE bar.col1 = 4) FROM t1"
                   {:table-info {"t1" #{"col1"}
                                 "bar" #{"col1"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-top-level-project"
         (plan-sql "SELECT t1.col1, ? FROM t1"
                   {:table-info {"t1" #{"col1"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-update-set-value"
         (plan-sql "UPDATE t1 SET col1 = ?"
                   {:table-info {"t1" #{"col1"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-table-values"
         (plan-sql "SELECT bar.foo FROM (VALUES (?)) AS bar(foo)"
                   {:table-info {"bar" #{"foo"}}})))

  (t/is (=plan-file
         "test-dynamic-parameters-103-update-app-time"
         (plan-sql "UPDATE users FOR PORTION OF VALID_TIME FROM ? TO ? AS u SET first_name = ? WHERE u.id = ?"
                   {:table-info {"users" #{"first_name" "id"}}}))))

(t/deftest test-dynamic-temporal-filters-3068
  (t/testing "AS OF"
    (t/is
     (=plan-file
      "test-dynamic-parameters-temporal-filters-3068-as-of"
      (plan-sql "SELECT bar FROM foo FOR VALID_TIME AS OF ?"
                {:table-info {"foo" #{"bar"}}}))))

  (t/testing "FROM A to B"
    (t/is
     (=plan-file
      "test-dynamic-parameters-temporal-filters-3068-from-to"
      (plan-sql "SELECT bar FROM foo FOR VALID_TIME FROM ? TO ?"
                {:table-info {"foo" #{"bar"}}}))))

  (t/testing "BETWEEN A AND B"
    (t/is
     (=plan-file
      "test-dynamic-parameters-temporal-filters-3068-between"
      (plan-sql "SELECT bar FROM foo FOR VALID_TIME BETWEEN ? AND ?"
                {:table-info {"foo" #{"bar"}}}))))
  
  (t/testing "AS OF SYSTEM TIME"
    (t/is
     (=plan-file
      "test-dynamic-parameters-temporal-filters-3068-as-of-system-time"
      (plan-sql "SELECT bar FROM foo FOR SYSTEM_TIME AS OF ?"
                {:table-info {"foo" #{"bar"}}}))))

  (t/testing "using dynamic AS OF in a query"
    (xt/submit-tx tu/*node* [[:put-docs {:into :docs, :valid-from #inst "2015"}
                              {:xt/id :matthew}]
                             [:put-docs {:into :docs, :valid-from #inst "2018"}
                              {:xt/id :mark}]])
    (t/is
     (= [{:xt/id :matthew}]
        (xt/q tu/*node* "SELECT docs.xt$id FROM docs FOR VALID_TIME AS OF ?" {:args [#inst "2016"]})))))

(t/deftest test-order-by-null-handling-159
  (t/is (=plan-file
         "test-order-by-null-handling-159-1"
         (plan-sql "SELECT a FROM foo ORDER BY a NULLS FIRST"
                   {:table-info {"foo" #{"a"}}})))

  (t/is (=plan-file
         "test-order-by-null-handling-159-2"
         (plan-sql "SELECT a FROM foo ORDER BY a NULLS LAST"
                   {:table-info {"foo" #{"a"}}}))))

(t/deftest test-arrow-table
  (t/is (=plan-file
          "test-arrow-table-1"
          (plan-sql "SELECT foo.a FROM ARROW_TABLE('test.arrow') AS foo")))

  (t/is (=plan-file
          "test-arrow-table-2"
          (plan-sql "SELECT * FROM ARROW_TABLE('test.arrow') AS foo (a, b)"))))

(t/deftest test-trim-expr
  (t/are [sql expected]
    (= expected (plan-expr-with-foo sql))

    "TRIM(foo.a)" '(trim a " ")

    "TRIM(LEADING FROM foo.a)" '(trim-leading a " ")
    "TRIM(LEADING '$' FROM foo.a)" '(trim-leading a "$")
    "TRIM(LEADING foo.b FROM foo.a)" '(trim-leading a b)

    "TRIM(TRAILING FROM foo.a)" '(trim-trailing a " ")
    "TRIM(TRAILING '$' FROM foo.a)" '(trim-trailing a "$")
    "TRIM(TRAILING foo.b FROM foo.a)" '(trim-trailing a b)

    "TRIM(BOTH FROM foo.a)" '(trim a " ")
    "TRIM(BOTH '$' FROM foo.a)" '(trim a "$")
    "TRIM(BOTH foo.b FROM foo.a)" '(trim a b)

    "TRIM(BOTH '😎' FROM foo.a)" '(trim a "😎")

    "TRIM('$' FROM foo.a)" '(trim a "$")))

(t/deftest test-like-expr
  (t/are [sql expected]
    (= expected (plan-expr-with-foo sql))

    "foo.a LIKE ''" '(like a "")
    "foo.a LIKE foo.b" '(like a b)
    "foo.a LIKE 'foo%'" '(like a "foo%")

    "foo.a NOT LIKE ''" '(not (like a ""))
    "foo.a NOT LIKE foo.b" '(not (like a b))
    "foo.a NOT LIKE 'foo%'" '(not (like a "foo%"))

    ;; no support for ESCAPE (or default escapes), see #157
    ))

(t/deftest test-like-regex-expr
  (t/are [sql expected]
    (= expected (plan-expr-with-foo sql))

    "foo.a LIKE_REGEX foo.b" '(like-regex a b "")
    "foo.a LIKE_REGEX foo.b FLAG 'i'" '(like-regex a b "i")

    "foo.a NOT LIKE_REGEX foo.b" '(not (like-regex a b ""))
    "foo.a NOT LIKE_REGEX foo.b FLAG 'i'" '(not (like-regex a b "i"))))

(t/deftest test-like-regex-query-case-insensitive
  (t/is (= [{:match false}]
           (xt/q tu/*node* "SELECT ('ABC' LIKE_REGEX 'a') as match")))

  (t/is (= [{:match true}]
           (xt/q tu/*node* "SELECT ('ABC' LIKE_REGEX 'a' FLAG 'i') as match"))))

(t/deftest test-postgres-regex-expr
  (t/are [sql expected] (= expected (plan-expr-with-foo sql))

    "foo.a ~ foo.b" '(like-regex a b "")
    "foo.a ~* foo.b" '(like-regex a b "i")

    "foo.a !~ foo.b" '(not (like-regex a b ""))
    "foo.a !~* foo.b" '(not (like-regex a b "i"))))

(defn pg-regex-query [val op pattern]
  (let [query (format "SELECT ('%s' %s '%s') as match" val op pattern)
        [{:keys [match]}] (xt/q tu/*node* query)]
    match))

(t/deftest test-postgres-regex-query
  (t/testing "postgres case sensitive match regex"
    (t/are [expected val op pattern] (= expected (pg-regex-query val op pattern))
      true "abcd" "~" "abcd"
      true "abcd" "~" "a.c"
      false "abcd" "~" "x"
      false "abcd" "~" "A"
      false "ABCD" "~" "a"))

  (t/testing "postgres case insensitive match regex"
    (t/are [expected val op pattern] (= expected (pg-regex-query val op pattern))
      true "abcd" "~*" "abcd"
      true "abcd" "~*" "a.c"
      false "abcd" "~*" "x"
      true "abcd" "~*" "A"
      true "ABCD" "~*" "a"))

  (t/testing "postgres case sensitive not match regex"
    (t/are [expected val op pattern] (= expected (pg-regex-query val op pattern))
      false "abcd" "!~" "abcd"
      false "abcd" "!~" "a.c"
      true "abcd" "!~" "x"
      true "abcd" "!~" "A"
      true "ABCD" "!~" "a"))

  (t/testing "postgres case insensitive not match regex"
    (t/are [expected val op pattern] (= expected (pg-regex-query val op pattern))
      false "abcd" "!~*" "abcd"
      false "abcd" "!~*" "a.c"
      true "abcd" "!~*" "x"
      false "abcd" "!~*" "A"
      false "ABCD" "!~*" "a")))

(t/deftest test-upper-expr
  (t/is (= '(upper a) (plan-expr-with-foo "UPPER(foo.a)"))))

(t/deftest test-lower-expr
  (t/is (= '(lower a) (plan-expr-with-foo "LOWER(foo.a)"))))

(t/deftest test-substring-expr
  (t/are
   [sql expected] (= expected (plan-expr-with-foo sql))
    "SUBSTRING(foo.a FROM 1)" '(substring a 1)
    "SUBSTRING(foo.a FROM 1 FOR 2)" '(substring a 1 2)
    "SUBSTRING(foo.a FROM 1 USING 'CHARACTERS')" '(substring a 1)))

(t/deftest test-concat-expr
  (t/is (= '(concat a b) (plan-expr-with-foo "foo.a || foo.b")))
  (t/is (= '(concat "a" b) (plan-expr-with-foo "'a' || foo.b")))
  (t/is (= '(concat (concat a "a") "b") (plan-expr-with-foo "foo.a || 'a' || 'b'"))))

(t/deftest test-character-length-expr
  (t/is (= '(character-length a) (plan-expr-with-foo "CHARACTER_LENGTH(foo.a)")))
  (t/is (= '(character-length a) (plan-expr-with-foo "CHARACTER_LENGTH(foo.a USING CHARACTERS)")))
  (t/is (= '(octet-length a) (plan-expr-with-foo "CHARACTER_LENGTH(foo.a USING OCTETS)"))))

(t/deftest test-char-length-alias
  (t/is (= '(character-length a) (plan-expr-with-foo "CHAR_LENGTH(foo.a)")) "CHAR_LENGTH alias works")
  (t/is (= '(character-length a) (plan-expr-with-foo "CHAR_LENGTH(foo.a USING CHARACTERS)")) "CHAR_LENGTH alias works")
  (t/is (= '(octet-length a) (plan-expr-with-foo "CHAR_LENGTH(foo.a USING OCTETS)")) "CHAR_LENGTH alias works"))

(t/deftest test-octet-length-expr
  (t/is (= '(octet-length a) (plan-expr-with-foo "OCTET_LENGTH(foo.a)"))))

(t/deftest test-position-expr
  (t/is (= '(position a b) (plan-expr-with-foo "POSITION(foo.a IN foo.b)")))
  (t/is (= '(position a b) (plan-expr-with-foo "POSITION(foo.a IN foo.b USING CHARACTERS)")))
  (t/is (= '(octet-position a b) (plan-expr-with-foo "POSITION(foo.a IN foo.b USING OCTETS)"))))

(t/deftest test-length-expr
  (t/is (= '(length x1) (plan/plan-expr "LENGTH(foo.a)")))
  (t/is (= '(length "abc") (plan/plan-expr "LENGTH('abc')")))
  (t/is (= '(length [1 2 3]) (plan/plan-expr "LENGTH([1, 2, 3])"))))

(t/deftest test-length-query
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 
                                             :string "abcdef"
                                             :list [1 2 3 4 5]
                                             :map {:a 1 :b 2} 
                                             :set #{1 2 3}
                                             :varbinary (byte-array [11 22])}]])
  
  (t/is (= [{:len 3}] (xt/q tu/*node* "SELECT LENGTH('abc') as len FROM docs")))
  (t/is (= [{:len 6}] (xt/q tu/*node* "SELECT LENGTH(docs.string) as len FROM docs")))
  (t/is (= [{:len 4}] (xt/q tu/*node* "SELECT LENGTH([1,2,3,4]) as len FROM docs"))) 
  (t/is (= [{:len 5}] (xt/q tu/*node* "SELECT LENGTH(docs.list) as len FROM docs")))
  (t/is (= [{:len 2}] (xt/q tu/*node* "SELECT LENGTH(docs.map) as len FROM docs"))) 
  (t/is (= [{:len 3}] (xt/q tu/*node* "SELECT LENGTH(docs.set) as len FROM docs"))) 
  (t/is (= [{:len 2}] (xt/q tu/*node* "SELECT LENGTH(docs.varbinary) as len FROM docs"))))

(t/deftest test-numerical-fn-exprs
  (t/are [expr expected]
    (= expected (plan-expr-with-foo expr))
    "CARDINALITY(foo.a)" '(cardinality a)
    "ABS(foo.a)" '(abs a)
    "MOD(foo.a, foo.b)" '(mod a b)
    "SIN(foo.a)" '(sin a)
    "COS(foo.a)" '(cos a)
    "TAN(foo.a)" '(tan a)
    "LOG(foo.a, 3)" '(log a 3)
    "LOG10(foo.a)" '(log10 a)
    "LN(foo.a)" '(ln a)
    "EXP(foo.a)" '(exp a)
    "POWER(foo.a, 3)" '(power a 3)
    "SQRT(foo.a)" '(sqrt a)
    "FLOOR(foo.a)" '(floor a)
    "CEIL(foo.a)" '(ceil a)
    "LEAST(foo.a, foo.b)" '(least a b)
    "GREATEST(foo.a, foo.b)" '(greatest a b)))

(t/deftest test-boolean-predicate-exprs
  (t/are [expr expected]
         (= expected (plan-expr-with-foo expr))
    "1 > 2" '(> 1 2)
    "1 >= 2" '(>= 1 2)
    "1 < 2" '(< 1 2)
    "1 <= 2" '(<= 1 2)
    "1 = 2" '(= 1 2)
    "1 != 2" '(!= 1 2)
    "1 <> 2" '(<> 1 2)
    "2 BETWEEN 1 AND 3" '(between 2 1 3)
    "2 BETWEEN ASYMMETRIC 1 AND 3" '(between 2 1 3)
    "2 BETWEEN SYMMETRIC 1 AND 3" '(between-symmetric 2 1 3)
    "2 NOT BETWEEN 1 AND 3" '(not (between 2 1 3))))
  
(t/deftest test-overlay-expr
  (t/are [sql expected]
    (= expected (plan-expr-with-foo sql))
    "OVERLAY(foo.a PLACING foo.b FROM 1 for 4)" '(overlay a b 1 4)
    "OVERLAY(foo.a PLACING foo.b FROM 1)" '(overlay a b 1 (default-overlay-length b))))

(t/deftest test-bool-test-expr
  (t/are [sql expected]
    (= expected (plan-expr-with-foo sql))

    "foo.a IS true" '(true? a)
    "foo.a IS NOT true" '(not (true? a))

    "foo.a IS false" '(false? a)
    "foo.a IS NOT false" '(not (false? a))

    "foo.a IS UNKNOWN" '(nil? a)
    "foo.a IS NOT UNKNOWN" '(not (nil? a))

    "foo.a IS NULL" '(nil? a)
    "foo.a IS NOT NULL" '(not (nil? a))))

(deftest test-projects-that-matter-are-maintained
  (t/is (=plan-file
          "projects-that-matter-are-maintained"
          (plan-sql
            "SELECT customers.id
            FROM customers
            UNION
            SELECT o.id
            FROM
            (SELECT orders.id, orders.product
            FROM orders) AS o"))))

(deftest test-semi-and-anti-joins-are-pushed-down
  (t/is (=plan-file
          "test-semi-and-anti-joins-are-pushed-down"
          (plan-sql
            "SELECT t1.a1
            FROM t1, t2, t3
            WHERE t1.b1 in (532,593)
            AND t2.b1 in (808,662)
            AND t3.c1 in (792,14)
            AND t1.a1 = t2.a2"))))

(deftest test-group-by-with-projected-column-in-expr
  (t/is (=plan-file
          "test-group-by-with-projected-column-in-expr"
          (plan-sql
            "SELECT foo.a - 4 AS bar
            FROM foo
            GROUP BY foo.a"))))

(deftest test-interval-expr
  (t/are [sql expected]
         (= expected (plan-expr-with-foo sql))

    "1 YEAR" '(single-field-interval 1 "YEAR" 2 6)
    "1 YEAR + 3 MONTH + 4 DAY" '(+ (+ (single-field-interval 1 "YEAR" 2 6)
                                      (single-field-interval 3 "MONTH" 2 6))
                                   (single-field-interval 4 "DAY" 2 6))

      ;; todo investigate, these expr are entirely ambiguous
      ;; I think these should not be parsed, but they are! ...
    #_#_"1 YEAR + 3" '()
    #_#_"1 YEAR - 3" '()

      ;; scaling is not ambiguous like add/sub is
    "INTERVAL '1' YEAR * 3" '(* (single-field-interval "1" "YEAR" 2 6) 3)
    "3 * INTERVAL '1' YEAR" '(* 3 (single-field-interval "1" "YEAR" 2 6))

      ;; division is allowed in spec, but provides some ambiguity
      ;; as we do not allow fractional components (other than seconds)
      ;; we therefore throw at runtime for arrow vectors that cannot be cleanly truncated / rond
    "1 YEAR / 3" '(/ (single-field-interval 1 "YEAR" 2 6) 3)

    "foo.a YEAR" '(single-field-interval a "YEAR" 2 6)
    "foo.a MONTH" '(single-field-interval a "MONTH" 2 6)
    "foo.a DAY" '(single-field-interval a "DAY" 2 6)
    "foo.a HOUR" '(single-field-interval a "HOUR" 2 6)
    "foo.a MINUTE" '(single-field-interval a "MINUTE" 2 6)
    "foo.a SECOND" '(single-field-interval a "SECOND" 2 6)

    "- foo.a SECOND" '(- (single-field-interval a "SECOND" 2 6))
    "+ foo.a SECOND" '(single-field-interval a "SECOND" 2 6)

    "foo.a YEAR + foo.b YEAR" '(+ (single-field-interval a "YEAR" 2 6)
                                  (single-field-interval b "YEAR" 2 6))
    "foo.a YEAR + foo.b MONTH" '(+ (single-field-interval a "YEAR" 2 6)
                                   (single-field-interval b "MONTH" 2 6))
    "foo.a YEAR - foo.b MONTH" '(- (single-field-interval a "YEAR" 2 6)
                                   (single-field-interval b "MONTH" 2 6))

    "foo.a YEAR + 1 MONTH" '(+ (single-field-interval a "YEAR" 2 6) (single-field-interval 1 "MONTH" 2 6))
    "foo.a YEAR + 1 MONTH + 2 DAY" '(+ (+ (single-field-interval a "YEAR" 2 6)
                                          (single-field-interval 1 "MONTH" 2 6))
                                       (single-field-interval 2 "DAY" 2 6))
    "foo.a YEAR + 1 MONTH - 2 DAY" '(- (+ (single-field-interval a "YEAR" 2 6)
                                          (single-field-interval 1 "MONTH" 2 6))
                                       (single-field-interval 2 "DAY" 2 6))

    "foo.a + 2 MONTH" '(+ a (single-field-interval 2 "MONTH" 2 6))
    "foo.a + + 1 MONTH" '(+ a (single-field-interval 1 "MONTH" 2 6))
    "foo.a + - 1 MONTH" '(+ a (- (single-field-interval 1 "MONTH" 2 6)))

    "foo.a YEAR TO MONTH" '(multi-field-interval a "YEAR" 2 "MONTH" 6)
    "foo.a DAY TO SECOND" '(multi-field-interval a "DAY" 2 "SECOND" 6)

    "INTERVAL '3' YEAR" '(single-field-interval "3" "YEAR" 2 6)
    "INTERVAL '-3' YEAR" '(single-field-interval "-3" "YEAR" 2 6)
    "INTERVAL '+3' YEAR" '(single-field-interval "+3" "YEAR" 2 6)

    "INTERVAL '3' MONTH" '(single-field-interval "3" "MONTH" 2 6)
    "INTERVAL '-3' MONTH" '(single-field-interval "-3" "MONTH" 2 6)
    "INTERVAL '+3' MONTH" '(single-field-interval "+3" "MONTH" 2 6)

    "INTERVAL '3' DAY" '(single-field-interval "3" "DAY" 2 6)
    "INTERVAL '-3' DAY" '(single-field-interval "-3" "DAY" 2 6)
    "INTERVAL '+3' DAY" '(single-field-interval "+3" "DAY" 2 6)
    "INTERVAL '333' DAY(3)" '(single-field-interval "333" "DAY" 2 6)

    "INTERVAL '3' HOUR" '(single-field-interval "3" "HOUR" 2 6)
    "INTERVAL '-3' HOUR" '(single-field-interval "-3" "HOUR" 2 6)
    "INTERVAL '+3' HOUR" '(single-field-interval "+3" "HOUR" 2 6)

    "INTERVAL '3' MINUTE" '(single-field-interval "3" "MINUTE" 2 6)
    "INTERVAL '-3' MINUTE" '(single-field-interval "-3" "MINUTE" 2 6)
    "INTERVAL '+3' MINUTE" '(single-field-interval "+3" "MINUTE" 2 6)

    "INTERVAL '3' SECOND" '(single-field-interval "3" "SECOND" 2 6)
    "INTERVAL '-3' SECOND" '(single-field-interval "-3" "SECOND" 2 6)
    "INTERVAL '+3' SECOND" '(single-field-interval "+3" "SECOND" 2 6)
    "INTERVAL '333' SECOND(3)" '(single-field-interval "333" "SECOND" 2 3)

    "INTERVAL '3-4' YEAR TO MONTH" '(multi-field-interval "3-4" "YEAR" 2 "MONTH" 6)
    "INTERVAL '-3-4' YEAR TO MONTH" '(multi-field-interval "-3-4" "YEAR" 2 "MONTH" 6)
    "INTERVAL '+3-4' YEAR TO MONTH" '(multi-field-interval "+3-4" "YEAR" 2 "MONTH" 6)

    "INTERVAL '3 4' DAY TO HOUR" '(multi-field-interval "3 4" "DAY" 2 "HOUR" 6)
    "INTERVAL '3 04' DAY TO HOUR" '(multi-field-interval "3 04" "DAY" 2 "HOUR" 6)
    "INTERVAL '3 04:20' DAY TO MINUTE" '(multi-field-interval "3 04:20" "DAY" 2 "MINUTE" 6)
    "INTERVAL '3 04:20:34' DAY TO SECOND" '(multi-field-interval "3 04:20:34" "DAY" 2 "SECOND" 6)
    "INTERVAL '3 04:20:34' DAY TO SECOND(4)" '(multi-field-interval "3 04:20:34" "DAY" 2 "SECOND" 4)

    "INTERVAL '04:20' HOUR TO MINUTE" '(multi-field-interval "04:20" "HOUR" 2 "MINUTE" 6)
    "INTERVAL '04:20:34' HOUR TO SECOND" '(multi-field-interval "04:20:34" "HOUR" 2 "SECOND" 6)

    "INTERVAL '20:34' MINUTE TO SECOND" '(multi-field-interval "20:34" "MINUTE" 2 "SECOND" 6)
    
    "INTERVAL -'3' YEAR" '(- (single-field-interval "3" "YEAR" 2 6))
    "INTERVAL -'3-10' YEAR TO MONTH" '(- (multi-field-interval "3-10" "YEAR" 2 "MONTH" 6))
    "INTERVAL -'3 10' DAY TO HOUR" '(- (multi-field-interval "3 10" "DAY" 2 "HOUR" 6))))

(deftest test-interval-abs
  (t/are [sql expected]
    (= expected (plan-expr-with-foo sql))

    "ABS(foo.a)" '(abs a)
    "ABS(1 YEAR)" '(abs (single-field-interval 1 "YEAR" 2 6))))

(t/deftest test-interval-comparison
  (t/is (= [{:gt true}]
           (xt/q tu/*node* "SELECT (INTERVAL '3 4' DAY TO HOUR > INTERVAL '3 1' DAY TO HOUR) as gt")))

  (t/is (= [{:lt false}]
           (xt/q tu/*node* "SELECT (INTERVAL '3 4' DAY TO HOUR < INTERVAL '3 1' DAY TO HOUR) as lt")))

  (t/is (= [{:gte true}]
           (xt/q tu/*node* "SELECT (INTERVAL '3 4' DAY TO HOUR >= INTERVAL '3 1' DAY TO HOUR) as gte")))

  (t/is (= [{:lte false}]
           (xt/q tu/*node* "SELECT (INTERVAL '3 4' DAY TO HOUR <= INTERVAL '3 1' DAY TO HOUR) as lte")))

  (t/is (= [{:eq false}]
           (xt/q tu/*node* "SELECT (INTERVAL '3 4' DAY TO HOUR = INTERVAL '3 1' DAY TO HOUR) as eq")))

  (t/is (= [{:eq true}]
           (xt/q tu/*node* "SELECT (INTERVAL '3 1' DAY TO HOUR = INTERVAL '3 1' DAY TO HOUR) as eq"))))

(deftest test-array-construction
  (t/are [sql expected]
    (= expected (plan/plan-expr sql))

    "ARRAY []" []

    "ARRAY [1]" [1]
    "ARRAY [NULL]" [nil]
    "ARRAY [ARRAY [1]]" [[1]]

    "ARRAY [foo.x, foo.y + 1]" '[x1 (+ x2 1)]

    "ARRAY [1, 42]" [1 42]
    "ARRAY [1, NULL]" [1 nil]
    "ARRAY [1, 1.2, '42!']" [1 1.2 "42!"]

    "[]" []

    "[1]" [1]
    "[NULL]" [nil]
    "[[1]]" [[1]]

    "[foo.x, foo.y + 1]" '[x1 (+ x2 1)]

    "[1, 42]" [1 42]
    "[1, NULL]" [1 nil]
    "[1, 1.2, '42!']" [1 1.2 "42!"]))

(deftest test-object-construction
  (t/are [sql expected]
    (= expected (plan/plan-expr sql))

    "OBJECT ()" {}
    "OBJECT ('foo': 2)" {:foo 2}
    "OBJECT ('foo': 2, 'bar': true)" {:foo 2 :bar true}
    "OBJECT ('foo': 2, 'bar': ARRAY [true, 1])" {:foo 2 :bar [true 1]}
    "OBJECT ('foo': 2, 'bar': OBJECT('baz': ARRAY [true, 1]))" {:foo 2 :bar {:baz [true 1]}}

    "{}" {}
    "{'foo': 2}" {:foo 2}
    "{'foo': 2, 'bar': true}" {:foo 2 :bar true}
    "{'foo': 2, 'bar': [true, 1]}" {:foo 2 :bar [true 1]}
    "{'foo': 2, 'bar': {'baz': [true, 1]}}" {:foo 2 :bar {:baz [true 1]}}))

(deftest test-object-field-access
  (t/are [sql expected]
    (= expected (plan/plan-expr sql))

    "OBJECT('foo': 2).foo" '(. {:foo 2} :foo)
    "{'foo': 2}.foo" '(. {:foo 2} :foo)
    "{'foo': 2}.foo.bar" '(. (. {:foo 2} :foo) :bar)

    "foo.a.b" '(. x1 :b)
    "foo.a.b.c" '(. (. x1 :b) :c)))

(deftest test-array-subqueries
  (t/are [file q]
    (=plan-file file (plan-sql q))

    "test-array-subquery1" "SELECT ARRAY(select b.b1 from b where b.b2 = 42) FROM a where a.a = 42"
    "test-array-subquery2" "SELECT ARRAY(select b.b1 from b where b.b2 = a.b) FROM a where a.a = 42"))

(t/deftest test-array-expressions
  (t/are [sql expected]
         (= expected (plan-expr-with-foo sql))
  
    "[1,2]" [1 2]
    "[1,2] || foo.a" '(concat [1 2] a)
    "[1,2] || [2,3]" '(concat [1 2] [2 3])))

(t/deftest test-array-trim
  (t/are [sql expected]
    (= expected (plan-expr-with-foo sql))

    "TRIM_ARRAY(NULL, 2)" '(trim-array nil 2)
    "TRIM_ARRAY(foo.a, 2)" '(trim-array a 2)
    "TRIM_ARRAY(ARRAY [42, 43], 1)" '(trim-array [42, 43] 1)
    "TRIM_ARRAY(foo.a, foo.b)" '(trim-array a b)))

(t/deftest test-cast
  (t/are [sql expected]
    (= expected (plan-expr-with-foo sql))

    "CAST(NULL AS INT)" (list 'cast nil :i32)
    "CAST(NULL AS INTEGER)" (list 'cast nil :i32)
    "CAST(NULL AS BIGINT)" (list 'cast nil :i64)
    "CAST(NULL AS SMALLINT)" (list 'cast nil :i16)
    "CAST(NULL AS FLOAT)" (list 'cast nil :f32)
    "CAST(NULL AS REAL)" (list 'cast nil :f32)
    "CAST(NULL AS DOUBLE PRECISION)" (list 'cast nil :f64)

    "CAST(foo.a AS INT)" (list 'cast 'a :i32)
    "CAST(42.0 AS INT)" (list 'cast 42.0 :i32)))

(t/deftest test-cast-string-to-temporal
  (t/is (= [{:timestamp-tz #time/zoned-date-time "2021-10-21T12:34:00Z"}]
           (xt/q tu/*node* "SELECT CAST('2021-10-21T12:34:00Z' AS TIMESTAMP WITH TIME ZONE) as timestamp_tz")))

  (t/is (= [{:timestamp #time/date-time "2021-10-21T12:34:00"}]
           (xt/q tu/*node* "SELECT CAST('2021-10-21T12:34:00' AS TIMESTAMP) as \"timestamp\"")))

  (t/is (= [{:timestamp #time/date-time "2021-10-21T12:34:00"}]
           (xt/q tu/*node* "SELECT CAST('2021-10-21T12:34:00' AS TIMESTAMP WITHOUT TIME ZONE) as \"timestamp\"")))

  (t/is (= [{:duration #time/date "2021-10-21"}]
           (xt/q tu/*node* "SELECT CAST('2021-10-21' AS DATE) as \"duration\"")))

  (t/is (= [{:time #time/time "12:00:01"}]
           (xt/q tu/*node* "SELECT CAST('12:00:01' AS TIME) as \"time\"")))
  
  (t/is (= [{:duration #time/duration "PT13M56.123456S"}]
           (xt/q tu/*node* "SELECT CAST('PT13M56.123456789S' AS DURATION) as \"duration\"")))
  
  (t/is (= [{:duration #time/duration "PT13M56.123456789S"}]
           (xt/q tu/*node* "SELECT CAST('PT13M56.123456789S' AS DURATION(9)) as \"duration\"")))

  (t/is (= [{:time #time/time "12:00:01.1234"}]
           (xt/q tu/*node* "SELECT CAST('12:00:01.123456' AS TIME(4)) as \"time\"")))
  
  (t/is (= [{:timestamp #time/date-time "2021-10-21T12:34:00.1234567"}]
           (xt/q tu/*node* "SELECT CAST('2021-10-21T12:34:00.123456789' AS TIMESTAMP(7)) as \"timestamp\"")))
  
  (t/is (= [{:timestamp-tz #time/zoned-date-time "2021-10-21T12:34:00.12Z"}]
           (xt/q tu/*node* "SELECT CAST('2021-10-21T12:34:00.123Z' AS TIMESTAMP(2) WITH TIME ZONE) as timestamp_tz")))

  (t/is (thrown-with-msg?
         RuntimeException
         #"String '2021-10-21T12' has invalid format for type timestamp with timezone"
         (xt/q tu/*node* "SELECT CAST('2021-10-21T12' AS TIMESTAMP WITH TIME ZONE) as timestamp_tz"))))

(t/deftest test-cast-temporal-to-string
  (t/is (= [{:string "2021-10-21T12:34:01Z"}]
           (xt/q tu/*node* "SELECT CAST(TIMESTAMP '2021-10-21T12:34:01Z' AS VARCHAR) as string")))

  (t/is (= [{:string "2021-10-21T12:34:01"}]
           (xt/q tu/*node* "SELECT CAST(TIMESTAMP '2021-10-21T12:34:01' AS VARCHAR) as string")))

  (t/is (= [{:string "2021-10-21"}]
           (xt/q tu/*node* "SELECT CAST(DATE '2021-10-21' AS VARCHAR) as string")))

  (t/is (= [{:string "12:00:01"}]
           (xt/q tu/*node* "SELECT CAST(TIME '12:00:01' AS VARCHAR) as string")))

  (t/is (= [{:string "PT13M56.123S"}]
           (xt/q tu/*node* "SELECT CAST(DURATION 'PT13M56.123S' AS VARCHAR) as string"))))

(t/deftest test-cast-interval-to-duration
  (t/is (= [{:duration #time/duration "PT13M56S"}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '13:56' MINUTE TO SECOND AS DURATION) as \"duration\"")))

  (t/is (= [{:duration #time/duration "PT13M56.123456789S"}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '13:56.123456789' MINUTE TO SECOND AS DURATION(9)) as \"duration\""))))

(t/deftest test-cast-duration-to-interval
  (t/testing "without interval qualifier"
    (t/is (= [{:itvl #xt/interval-mdn ["P0D" "PT26H13M56.111111S"]}]
             (xt/q tu/*node* "SELECT CAST(DURATION 'PT26H13M56.111111S' AS INTERVAL) as itvl")))

    (t/is (= [{:itvl #xt/interval-mdn ["P0D" "PT122H"]}]
             (xt/q tu/*node* "SELECT CAST((TIMESTAMP '2021-10-26T14:00:00' - TIMESTAMP '2021-10-21T12:00:00') AS INTERVAL) as itvl")))
    
    (t/is (= [{:itvl #xt/interval-mdn  ["P0D" "PT8882H"]}]
             (xt/q tu/*node* "SELECT CAST((TIMESTAMP '2021-10-26T14:00:00' - TIMESTAMP '2020-10-21T12:00:00') AS INTERVAL) as itvl"))))
  
  (t/testing "with interval qualifier"
    (t/is (= [{:itvl #xt/interval-mdn ["P1D" "PT0S"]}]
             (xt/q tu/*node* "SELECT CAST(DURATION 'PT26H13M56.111111S' AS INTERVAL DAY) as itvl")))

    (t/is (= [{:itvl #xt/interval-mdn ["P1D" "PT2H"]}]
             (xt/q tu/*node* "SELECT CAST(DURATION 'PT26H13M56.111111S' AS INTERVAL DAY TO HOUR) as itvl")))

    (t/is (= [{:itvl #xt/interval-mdn ["P1D" "PT2H13M"]}]
             (xt/q tu/*node* "SELECT CAST(DURATION 'PT26H13M56.111111S' AS INTERVAL DAY TO MINUTE) as itvl")))

    (t/is (= [{:itvl #xt/interval-mdn ["P1D" "PT2H13M56.111111S"]}]
             (xt/q tu/*node* "SELECT CAST(DURATION 'PT26H13M56.111111S' AS INTERVAL DAY TO SECOND) as itvl")))

    (t/is (= [{:itvl #xt/interval-mdn ["P1D" "PT2H13M56.111S"]}]
             (xt/q tu/*node* "SELECT CAST(DURATION 'PT26H13M56.111111S' AS INTERVAL DAY TO SECOND(3)) as itvl")))))

(t/deftest test-cast-interval-to-interval
  (t/is (= [{:itvl #xt/interval-ym "P22M"}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '1-10' YEAR TO MONTH AS INTERVAL) as itvl")))

  (t/is (= [{:itvl #xt/interval-ym "P12M"}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '1-10' YEAR TO MONTH AS INTERVAL YEAR) as itvl")))

  (t/is (= [{:itvl #xt/interval-mdn ["P1D" "PT0S"]}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '1 11:11:11.111' DAY TO SECOND AS INTERVAL DAY) as itvl")))

  (t/is (= [{:itvl #xt/interval-mdn ["P1D" "PT11H"]}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '1 11:11:11.111' DAY TO SECOND AS INTERVAL DAY TO HOUR) as itvl")))

  (t/is (= [{:itvl #xt/interval-mdn ["P1D" "PT11H11M"]}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '1 11:11:11.111' DAY TO SECOND AS INTERVAL DAY TO MINUTE) as itvl")))

  (t/is (= [{:itvl #xt/interval-mdn ["P1D" "PT11H11M11.111S"]}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '1 11:11:11.111' DAY TO SECOND AS INTERVAL DAY TO SECOND) as itvl")))

  (t/is (= [{:itvl #xt/interval-mdn ["P1D" "PT11H11M11S"]}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '1 11:11:11.111' DAY TO SECOND AS INTERVAL DAY TO SECOND(0)) as itvl")))
  
  (t/is (= [{:itvl #xt/interval-mdn ["P0D" "PT35H"]}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '1 11:11:11.111' DAY TO SECOND AS INTERVAL HOUR) as itvl"))))

(t/deftest test-cast-int-to-interval
  (t/is (= [{:itvl #xt/interval-mdn ["P3D" "PT0S"]}]
           (xt/q tu/*node* "SELECT CAST(3 AS INTERVAL DAY) as itvl")))
  
  (t/is (= [{:itvl #xt/interval-ym "P24M"}]
           (xt/q tu/*node* "SELECT CAST(2 AS INTERVAL YEAR) as itvl")))
  
  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"Cannot cast integer to a multi field interval"
         (xt/q tu/*node* "SELECT CAST(2 AS INTERVAL YEAR TO MONTH) as itvl"))))

(t/deftest test-cast-string-to-interval-with-qualifier
  (t/is (= [{:itvl #xt/interval-mdn ["P3D" "PT11H10M"]}]
           (xt/q tu/*node* "SELECT CAST('3 11:10' AS INTERVAL DAY TO MINUTE) as itvl")))
  
  (t/is (= [{:itvl #xt/interval-ym "P24M"}]
           (xt/q tu/*node* "SELECT CAST('2' AS INTERVAL YEAR) as itvl")))
  
  (t/is (= [{:itvl #xt/interval-ym "P22M"}]
           (xt/q tu/*node* "SELECT CAST('1-10' AS INTERVAL YEAR TO MONTH) as itvl")))
  
  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"Interval end field must have less significance than the start field."
         (xt/q tu/*node* "SELECT CAST('11:10' AS INTERVAL MINUTE TO HOUR) as itvl"))))

(t/deftest test-cast-string-to-interval-without-qualifier
  (t/is (= [{:itvl #xt/interval-mdn ["P3D" "PT11H10M"]}]
           (xt/q tu/*node* "SELECT CAST('P3DT11H10M' AS INTERVAL) as itvl")))

  (t/is (= [{:itvl #xt/interval-mdn ["P24M" "PT0S"]}]
           (xt/q tu/*node* "SELECT CAST('P2Y' AS INTERVAL) as itvl")))

  (t/is (= [{:itvl #xt/interval-mdn ["P22M" "PT0S"]}]
           (xt/q tu/*node* "SELECT CAST('P1Y10M' AS INTERVAL) as itvl")))
  
  (t/is (= [{:itvl #xt/interval-mdn ["P1M1D" "PT1H1M1.11111S"]}]
           (xt/q tu/*node* "SELECT CAST('P1M1DT1H1M1.11111S' AS INTERVAL) as itvl")))
  
  (t/is (= [{:itvl #xt/interval-mdn ["P0D" "PT-1H-1M"]}]
           (xt/q tu/*node* "SELECT CAST('PT-1H-1M' AS INTERVAL) as itvl"))))

(t/deftest test-cast-interval-to-string
  (t/is (= [{:string "P2YT0S"}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '2' YEAR AS VARCHAR) as string")))

  (t/is (= [{:string "P22MT0S"}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '1-10' YEAR TO MONTH AS VARCHAR) as string")))

  (t/is (= [{:string "P-22MT0S"}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '-1-10' YEAR TO MONTH AS VARCHAR) as string")))

  (t/is (= [{:string "P1DT0S"}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '1' DAY AS VARCHAR) as string")))

  (t/is (= [{:string "P1DT10H10M10S"}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '1 10:10:10' DAY TO SECOND AS VARCHAR) as string")))
  
  (t/is (= [{:string "P0DT10M10.111111111S"}]
           (xt/q tu/*node* "SELECT CAST(INTERVAL '10:10.111111111' MINUTE TO SECOND(9) AS VARCHAR) as string"))))

(t/deftest test-expr-in-equi-join
  (t/is
    (=plan-file
      "test-expr-in-equi-join-1"
      (plan-sql "SELECT a.a FROM a JOIN bar b ON a.a+1 = b.b+1")))
  (t/is
    (=plan-file
      "test-expr-in-equi-join-2"
      (plan-sql "SELECT a.a FROM a JOIN bar b ON a.a = b.b+1"))))

(deftest push-semi-and-anti-joins-down-test
;;semi-join was previously been pushed down below the cross join
;;where the cols it required weren't in scope
(t/is
  (=plan-file
    "push-semi-and-anti-joins-down"
    (plan-sql "SELECT
              x.foo
              FROM
              x,
              y
              WHERE
              EXISTS (
              SELECT z.bar
              FROM z
              WHERE
              z.bar = x.foo
              AND
              z.baz = y.biz
              )"))))

(deftest test-timestamp-literal
  (t/are
    [sql expected]
    (= expected (plan/plan-expr sql))
    "TIMESTAMP '3000-03-15 20:40:31'" #time/date-time "3000-03-15T20:40:31"
    "TIMESTAMP '3000-03-15 20:40:31.11'" #time/date-time "3000-03-15T20:40:31.11"
    "TIMESTAMP '3000-03-15 20:40:31.2222'" #time/date-time "3000-03-15T20:40:31.2222"
    "TIMESTAMP '3000-03-15 20:40:31.44444444'" #time/date-time "3000-03-15T20:40:31.44444444"
    "TIMESTAMP '3000-03-15 20:40:31+03:44'" #time/zoned-date-time "3000-03-15T20:40:31+03:44"
    "TIMESTAMP '3000-03-15 20:40:31.12345678+13:12'" #time/zoned-date-time "3000-03-15T20:40:31.123456780+13:12"
    "TIMESTAMP '3000-03-15 20:40:31.12345678-14:00'" #time/zoned-date-time"3000-03-15T20:40:31.123456780-14:00"
    "TIMESTAMP '3000-03-15 20:40:31.12345678+14:00'" #time/zoned-date-time"3000-03-15T20:40:31.123456780+14:00"
    "TIMESTAMP '3000-03-15 20:40:31-11:44'" #time/zoned-date-time "3000-03-15T20:40:31-11:44"
    "TIMESTAMP '3000-03-15T20:40:31-11:44'" #time/zoned-date-time "3000-03-15T20:40:31-11:44"
    "TIMESTAMP '3000-03-15T20:40:31Z'" #time/zoned-date-time "3000-03-15T20:40:31Z"

    "TIMESTAMP '3000-04-15T20:40:31+01:00[Europe/London]'" #time/zoned-date-time "3000-04-15T20:40:31+01:00[Europe/London]"
    ;; corrects the offset to the zone's offset
    "TIMESTAMP '3000-04-15T20:40:31+05:00[Europe/London]'" #time/zoned-date-time "3000-04-15T20:40:31+01:00[Europe/London]"
    ;; provides the correct offset for the zone
    "TIMESTAMP '3000-04-15T20:40:31[Europe/London]'" #time/zoned-date-time "3000-04-15T20:40:31+01:00[Europe/London]"))

(deftest test-time-literal
  (t/are
    [sql expected]
    (= expected (plan/plan-expr sql))
    "TIME '20:40:31'" #time/time "20:40:31"
    "TIME '20:40:31.467'" #time/time "20:40:31.467"
    "TIME '20:40:31.932254'" #time/time "20:40:31.932254"
    "TIME '20:40:31-03:44'" #time/offset-time "20:40:31-03:44"
    "TIME '20:40:31+03:44'" #time/offset-time "20:40:31+03:44"
    "TIME '20:40:31.467+14:00'" #time/offset-time "20:40:31.467+14:00"))

(deftest date-literal
  (t/are
    [sql expected]
    (= expected (plan/plan-expr sql))
    "DATE '3000-03-15'" #time/date "3000-03-15"))

(t/deftest interval-literal
  (t/are [sql expected] (= expected (plan/plan-expr sql))
    "INTERVAL 'P1Y'" #xt/interval-mdn ["P1Y" "PT0S"]
    "INTERVAL 'P1Y-2M3D'" #xt/interval-mdn ["P1Y-2M3D" "PT0S"]
    "INTERVAL 'PT5H6M12.912S'" #xt/interval-mdn ["P0D" "PT5H6M12.912S"]
    "INTERVAL 'PT5H-6M-12.912S'" #xt/interval-mdn ["P0D" "PT4H53M47.088S"]
    "INTERVAL 'P1Y3DT12H52S'" #xt/interval-mdn ["P1Y3D" "PT12H52S"]
    "INTERVAL 'P1Y10M3DT12H52S'" #xt/interval-mdn ["P1Y10M3D" "PT12H52S"]))

(t/deftest interval-literal-query
  (t/is (= [{:interval #xt/interval-mdn ["P12M" "PT0S"]}]
           (xt/q tu/*node* "SELECT INTERVAL 'P1Y' as itvl")))

  (t/is (= [{:interval #xt/interval-mdn ["P10M3D" "PT0S"]}]
           (xt/q tu/*node* "SELECT INTERVAL 'P1Y-2M3D' as itvl")))

  (t/is (= [{:interval #xt/interval-mdn ["P0D" "PT5H6M12.912S"]}]
           (xt/q tu/*node* "SELECT INTERVAL 'PT5H6M12.912S' as itvl")))

  (t/is (= [{:interval #xt/interval-mdn ["P22M3D" "PT4H53M47.088S"]}]
           (xt/q tu/*node* "SELECT INTERVAL 'P1Y10M3DT5H-6M-12.912S' as itvl"))))

(t/deftest duration-literal
  (t/are [sql expected] (= expected (plan/plan-expr sql))
    "DURATION 'P1D'" #time/duration "PT24H"
    "DURATION 'PT1H'" #time/duration "PT1H"
    "DURATION 'PT1M'" #time/duration "PT1M"
    "DURATION 'PT1H1M1.111111S'" #time/duration "PT1H1M1.111111S"
    "DURATION 'P1DT1H'" #time/duration "PT25H"
    "DURATION 'P1DT10H1M1.111111S'" #time/duration "PT34H1M1.111111S"
    "DURATION 'PT-1H'" #time/duration "PT-1H"
    "DURATION 'P-1DT2H'" #time/duration "PT-22H"
    "DURATION 'P-1DT-10H-1M'" #time/duration "PT-34H-1M"))

(t/deftest duration-literal-query
  (t/is (= [{:duration #time/duration "PT24H"}]
           (xt/q tu/*node* "SELECT DURATION 'P1D' as duration")))
  
  (t/is (= [{:duration #time/duration "PT1H"}]
           (xt/q tu/*node* "SELECT DURATION 'PT1H' as duration")))
  
  (t/is (= [{:duration #time/duration "PT26H"}]
           (xt/q tu/*node* "SELECT DURATION 'P1DT2H' as duration")))
  
  (t/is (= [{:duration #time/duration "PT-22H"}]
           (xt/q tu/*node* "SELECT DURATION 'P-1DT2H' as duration"))))

(deftest test-date-trunc-plan
  (t/testing "TIMESTAMP behaviour"
    (t/are
     [sql expected]
     (= expected (plan/plan-expr sql))
      "DATE_TRUNC('MICROSECOND', TIMESTAMP '2021-10-21T12:34:56')" '(date_trunc "MICROSECOND" #time/date-time "2021-10-21T12:34:56")
      "DATE_TRUNC('MILLISECOND', TIMESTAMP '2021-10-21T12:34:56')" '(date_trunc "MILLISECOND" #time/date-time "2021-10-21T12:34:56")
      "date_trunc('second', timestamp '2021-10-21T12:34:56')" '(date_trunc "SECOND" #time/date-time "2021-10-21T12:34:56")
      "DATE_TRUNC('MINUTE', TIMESTAMP '2021-10-21T12:34:56')" '(date_trunc "MINUTE" #time/date-time "2021-10-21T12:34:56")
      "DATE_TRUNC('HOUR', TIMESTAMP '2021-10-21T12:34:56')" '(date_trunc "HOUR" #time/date-time "2021-10-21T12:34:56")
      "DATE_TRUNC('DAY', TIMESTAMP '2021-10-21T12:34:56')" '(date_trunc "DAY" #time/date-time "2021-10-21T12:34:56")
      "DATE_TRUNC('WEEK', TIMESTAMP '2021-10-21T12:34:56')" '(date_trunc "WEEK" #time/date-time "2021-10-21T12:34:56")
      "DATE_TRUNC('QUARTER', TIMESTAMP '2021-10-21T12:34:56')" '(date_trunc "QUARTER" #time/date-time "2021-10-21T12:34:56")
      "DATE_TRUNC('MONTH', TIMESTAMP '2021-10-21T12:34:56')" '(date_trunc "MONTH" #time/date-time "2021-10-21T12:34:56")
      "DATE_TRUNC('YEAR', TIMESTAMP '2021-10-21T12:34:56')" '(date_trunc "YEAR" #time/date-time "2021-10-21T12:34:56")
      "DATE_TRUNC('DECADE', TIMESTAMP '2021-10-21T12:34:56')" '(date_trunc "DECADE" #time/date-time "2021-10-21T12:34:56")
      "DATE_TRUNC('CENTURY', TIMESTAMP '2021-10-21T12:34:56')" '(date_trunc "CENTURY" #time/date-time "2021-10-21T12:34:56")
      "DATE_TRUNC('MILLENNIUM', TIMESTAMP '2021-10-21T12:34:56')" '(date_trunc "MILLENNIUM" #time/date-time "2021-10-21T12:34:56")))
  
  (t/testing "INTERVAL behaviour"
    (t/are
     [sql expected]
     (= expected (plan/plan-expr sql))
      "DATE_TRUNC('DAY', INTERVAL '5' DAY)" '(date_trunc "DAY" (single-field-interval "5" "DAY" 2 6))
      "date_trunc('hour', interval '3 02:47:33' day to second)" '(date_trunc "HOUR" (multi-field-interval "3 02:47:33" "DAY" 2 "SECOND" 6)))))

(deftest test-datetime-functions-plan
  (t/are
   [sql expected]
   (= expected (plan/plan-expr sql))
    "CURRENT_DATE" '(current-date)
    "CURRENT_DATE()" '(current-date)
    "CURRENT_TIME" '(current-time)
    "CURRENT_TIME()" '(current-time)
    "CURRENT_TIME(6)" '(current-time 6)
    "CURRENT_TIMESTAMP" '(current-timestamp)
    "CURRENT_TIMESTAMP()" '(current-timestamp)
    "CURRENT_TIMESTAMP(6)" '(current-timestamp 6)
    "LOCALTIME" '(local-time)
    "LOCALTIME()" '(local-time)
    "LOCALTIME(6)" '(local-time 6)
    "LOCALTIMESTAMP" '(local-timestamp)
    "LOCALTIMESTAMP()" '(local-timestamp)
    "LOCALTIMESTAMP(6)" '(local-timestamp 6)
    "END_OF_TIME" 'xtdb/end-of-time
    "END_OF_TIME()" 'xtdb/end-of-time))

(deftest test-date-trunc-query
  (t/is (= [{:timestamp #time/zoned-date-time "2021-10-21T12:34:00Z"}]
           (xt/q tu/*node* "SELECT DATE_TRUNC('MINUTE', TIMESTAMP '2021-10-21T12:34:56Z') as timestamp")))
  
  (t/is (= [{:timestamp #time/zoned-date-time "2021-10-21T12:00:00Z"}]
           (xt/q tu/*node* "select date_trunc('hour', timestamp '2021-10-21T12:34:56Z') as timestamp")))
  
  (t/is (= [{:timestamp #time/date "2001-01-01"}]
           (xt/q tu/*node* "select date_trunc('year', DATE '2001-11-27') as timestamp")))
  
  (t/is (= [{:timestamp #time/date-time "2021-10-21T12:00:00"}]
           (xt/q tu/*node* "select date_trunc('hour', timestamp '2021-10-21T12:34:56') as timestamp"))))

(deftest test-date-trunc-with-timezone-query
  (t/is (= [{:timestamp #time/zoned-date-time "2001-02-16T08:00-05:00"}]
           (xt/q tu/*node* "select date_trunc('day', TIMESTAMP '2001-02-16 15:38:11-05:00', 'Australia/Sydney') as timestamp")))
  
  (t/is (thrown-with-msg?
         ZoneRulesException
         #"Unknown time-zone ID: NotRealRegion"
         (xt/q tu/*node* "select date_trunc('hour', TIMESTAMP '2000-01-02 00:43:11+00:00', 'NotRealRegion') as timestamp"))))

(deftest test-date-trunc-with-interval-query 
  (t/is (= [{:interval #xt/interval-ym "P36M"}]
           (xt/q tu/*node* "SELECT DATE_TRUNC('YEAR', 3 YEAR + 3 MONTH) as itvl")))

  (t/is (= [{:interval #xt/interval-mdn ["P3M4D" "PT2S"]}]
           (xt/q tu/*node* "SELECT DATE_TRUNC('SECOND', 3 MONTH + 4 DAY + 2 SECOND) as itvl")))

  (t/is (= [{:interval #xt/interval-mdn ["P3M4D" "PT0S"]}]
           (xt/q tu/*node* "SELECT DATE_TRUNC('DAY', 3 MONTH + 4 DAY + 2 SECOND) as itvl")))

  (t/is (= [{:interval #xt/interval-mdn ["P3M" "PT0S"]}]
           (xt/q tu/*node* "SELECT DATE_TRUNC('MONTH', 3 MONTH + 4 DAY + 2 SECOND) as itvl"))))

(deftest test-extract-plan
  (t/testing "TIMESTAMP behaviour"
    (t/are
     [sql expected]
     (= expected (plan/plan-expr sql))
      "extract(second from timestamp '2021-10-21T12:34:56')" '(extract "SECOND" #time/date-time "2021-10-21T12:34:56")
      "EXTRACT(MINUTE FROM TIMESTAMP '2021-10-21T12:34:56')" '(extract "MINUTE" #time/date-time "2021-10-21T12:34:56")
      "EXTRACT(HOUR FROM TIMESTAMP '2021-10-21T12:34:56')" '(extract "HOUR" #time/date-time "2021-10-21T12:34:56")
      "EXTRACT(DAY FROM TIMESTAMP '2021-10-21T12:34:56')" '(extract "DAY" #time/date-time "2021-10-21T12:34:56")
      "EXTRACT(MONTH FROM TIMESTAMP '2021-10-21T12:34:56')" '(extract "MONTH" #time/date-time "2021-10-21T12:34:56")
      "EXTRACT(YEAR FROM TIMESTAMP '2021-10-21T12:34:56')" '(extract "YEAR" #time/date-time "2021-10-21T12:34:56")
      "EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2021-10-21T12:34:56')" '(extract "TIMEZONE_MINUTE" #time/date-time "2021-10-21T12:34:56")
      "EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2021-10-21T12:34:56')" '(extract "TIMEZONE_HOUR" #time/date-time "2021-10-21T12:34:56")))

  (t/testing "INTERVAL behaviour"
    (t/are
     [sql expected]
     (= expected (plan/plan-expr sql))
      "EXTRACT(second from interval '3 02:47:33' day to second)" '(extract "SECOND" (multi-field-interval "3 02:47:33" "DAY" 2 "SECOND" 6))
      "EXTRACT(MINUTE FROM INTERVAL '5' DAY)" '(extract "MINUTE" (single-field-interval "5" "DAY" 2 6))))
  
  (t/testing "TIME behaviour"
    (t/are
     [sql expected]
     (= expected (plan/plan-expr sql))
      "EXTRACT(second from time '11:11:11')" '(extract "SECOND" #time/time "11:11:11")
      "EXTRACT(MINUTE FROM TIME '11:11:11')" '(extract "MINUTE" #time/time "11:11:11"))))

(deftest test-extract-query
  (t/testing "timestamp behavior"
    (t/is (= [{:x 34}]
             (xt/q tu/*node* "SELECT EXTRACT(MINUTE FROM TIMESTAMP '2021-10-21T12:34:56') as x")))

    (t/is (= [{:x 2021}]
             (xt/q tu/*node* "SELECT EXTRACT(YEAR FROM TIMESTAMP '2021-10-21T12:34:56') as x")))

    (t/is (thrown-with-msg?
           UnsupportedOperationException
           #"Extract \"TIMEZONE_HOUR\" not supported for type timestamp without timezone"
           (xt/q tu/*node* "SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2021-10-21T12:34:56') as x"))))

  (t/testing "timestamp with timezone behavior"
    (t/is (= [{:x 34}]
             (xt/q tu/*node* "SELECT EXTRACT(MINUTE FROM TIMESTAMP '2021-10-21T12:34:56+05:00') as x")))

    (t/is (= [{:x 5}]
             (xt/q tu/*node* "SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2021-10-21T12:34:56+05:00') as x"))))

  (t/testing "date behavior"
    (t/is (= [{:x 3}]
             (xt/q tu/*node* "SELECT EXTRACT(MONTH FROM DATE '2001-03-11') as x")))

    (t/is (thrown-with-msg?
           UnsupportedOperationException
           #"Extract \"TIMEZONE_HOUR\" not supported for type date"
           (xt/q tu/*node* "SELECT EXTRACT(TIMEZONE_HOUR FROM DATE '2001-03-11') as x"))))

  (t/testing "time behavior"
    (t/is (= [{:x 34}]
             (xt/q tu/*node* "SELECT EXTRACT(MINUTE FROM TIME '12:34:56') as x")))

    (t/is (= [{:x 12}]
             (xt/q tu/*node* "SELECT EXTRACT(HOUR FROM TIME '12:34:56') as x")))

    (t/is (thrown-with-msg?
           UnsupportedOperationException
           #"Extract \"TIMEZONE_HOUR\" not supported for type timestamp without timezone"
           (xt/q tu/*node* "SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2021-10-21T12:34:56') as x"))))

  (t/testing "interval behavior"
    (t/is (= [{:x 3}]
             (xt/q tu/*node* "SELECT EXTRACT(DAY FROM INTERVAL '3 02:47:33' DAY TO SECOND) as x")))

    (t/is (= [{:x 47}]
             (xt/q tu/*node* "SELECT EXTRACT(MINUTE FROM INTERVAL '3 02:47:33' DAY TO SECOND) as x")))

    (t/is (thrown-with-msg?
           UnsupportedOperationException
           #"Extract \"TIMEZONE_HOUR\" not supported for type interval"
           (xt/q tu/*node* "SELECT EXTRACT(TIMEZONE_HOUR FROM INTERVAL '3 02:47:33' DAY TO SECOND) as x")))))

(deftest test-age-function
  (t/testing "testing AGE with timestamps"
    (t/is (= [{:itvl #xt/interval-mdn ["P0D" "PT2H"]}]
             (xt/q tu/*node* "SELECT AGE(TIMESTAMP '2022-05-02T01:00:00', TIMESTAMP '2022-05-01T23:00:00') as itvl")))
    (t/is (= [{:itvl #xt/interval-mdn ["P6M" "PT0S"]}]
             (xt/q tu/*node* "SELECT AGE(TIMESTAMP '2022-11-01T00:00:00', TIMESTAMP '2022-05-01T00:00:00') as itvl")))
    (t/is (= [{:itvl #xt/interval-mdn ["P0D" "PT1H"]}]
             (xt/q tu/*node* "SELECT AGE(TIMESTAMP '2023-01-01T01:00:00', TIMESTAMP '2023-01-01T00:00:00') as itvl"))))

  (t/testing "testing AGE with timestamp with timezone"
    (t/is (= [{:itvl #xt/interval-mdn ["P0D" "PT1H"]}]
             (xt/q tu/*node* "SELECT AGE(TIMESTAMP '2023-06-01T11:00:00+01:00[Europe/London]', TIMESTAMP '2023-06-01T11:00:00+02:00[Europe/Berlin]') as itvl")))
    (t/is (= [{:itvl #xt/interval-mdn ["P0D" "PT2H"]}]
             (xt/q tu/*node* "SELECT AGE(TIMESTAMP '2023-06-01T09:00:00-05:00[America/Chicago]', TIMESTAMP '2023-06-01T12:00:00') as itvl"))))

  (t/testing "testing AGE with date"
    (t/is (= [{:itvl #xt/interval-mdn ["P1D" "PT0S"]}]
             (xt/q tu/*node* "SELECT AGE(DATE '2023-01-02', DATE '2023-01-01') as itvl")))
    (t/is (= [{:itvl #xt/interval-mdn ["P-12M" "PT0S"]}]
             (xt/q tu/*node* "SELECT AGE(DATE '2023-01-01', DATE '2024-01-01') as itvl"))))

  (t/testing "test with mixed types"
    (t/is (= [{:itvl #xt/interval-mdn ["P1D" "PT0S"]}]
             (xt/q tu/*node* "SELECT AGE(DATE '2023-01-02', TIMESTAMP '2023-01-01T00:00:00') as itvl")))
    (t/is (= [{:itvl #xt/interval-mdn ["P-6M" "PT0S"]}]
             (xt/q tu/*node* "SELECT AGE(TIMESTAMP '2022-05-01T00:00:00', TIMESTAMP '2022-11-01T00:00:00+00:00[Europe/London]') as itvl")))
    (t/is (= [{:itvl #xt/interval-mdn ["P0D" "PT2H0.001S"]}]
             (xt/q tu/*node* "SELECT AGE(TIMESTAMP '2023-07-01T12:00:30.501', TIMESTAMP '2023-07-01T12:00:30.500+02:00[Europe/Berlin]') as itvl")))
    (t/is (= [{:itvl #xt/interval-mdn ["P0D" "PT-2H-0.001S"]}]
             (xt/q tu/*node* "SELECT AGE(TIMESTAMP '2023-07-01T12:00:30.499+02:00[Europe/Berlin]', TIMESTAMP '2023-07-01T12:00:30.500') as itvl")))))

(deftest test-system-time-queries
  (t/testing "AS OF"
    (t/is
      (=plan-file
        "system-time-as-of"
        (plan-sql "SELECT foo.bar FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00'"))))

  (t/testing "FROM A to B"

    (t/is
      (=plan-file
        "system-time-from-a-to-b"
        (plan-sql "SELECT foo.bar FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00'"))))


  (t/testing "BETWEEN A AND B"
    (t/is
      (=plan-file
        "system-time-between-subquery"
        (plan-sql "SELECT (SELECT 4 FROM t1 FOR SYSTEM_TIME BETWEEN DATE '3001-01-01' AND TIMESTAMP '3002-01-01 00:00:00+00:00') FROM t2")))

    (t/is
      (=plan-file
        "system-time-between-lateraly-derived-table"
        (plan-sql "SELECT x.y, y.z FROM x FOR SYSTEM_TIME AS OF DATE '3001-01-01',
                  LATERAL (SELECT z.z FROM z FOR SYSTEM_TIME FROM DATE '3001-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00' WHERE z.z = x.y) AS y")))))

(deftest test-valid-time-period-spec-queries

  (t/testing "AS OF"
    (t/is
     (=plan-file
      "valid-time-period-spec-as-of"
      (plan-sql "SELECT foo.bar FROM foo FOR VALID_TIME AS OF TIMESTAMP '2999-01-01 00:00:00'"))))

  (t/testing "FROM A to B"
    (t/is
     (=plan-file
      "valid-time-period-spec-from-to"
      (plan-sql "SELECT foo.bar FROM foo FOR VALID_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00'"))))

  (t/testing "BETWEEN A AND B"
    (t/is
     (=plan-file
      "valid-time-period-spec-between"
      (plan-sql "SELECT 4 FROM t1 FOR VALID_TIME BETWEEN TIMESTAMP '3000-01-01 00:00:00+00:00' AND DATE '3001-01-01'")))))

(deftest test-valid-and-system-time-period-spec-queries
  (t/testing "BETWEEN A AND B"
    (t/is
     (=plan-file
      "valid-and-system-time-period-spec-between"
      (plan-sql "SELECT 4 FROM t1
                  FOR SYSTEM_TIME BETWEEN DATE '2000-01-01' AND DATE '2001-01-01'
                  FOR VALID_TIME BETWEEN TIMESTAMP '3001-01-01 00:00:00+00:00' AND DATE '3000-01-01'")))))

;; x1 = app-time-start x2 = app-time-end

(deftest test-period-predicates
  (t/are [expected sql] (= expected (plan/plan-expr sql))
    '(and (<= x1 #time/zoned-date-time "2000-01-01T00:00Z")
          (>= x2 #time/zoned-date-time "2001-01-01T00:00Z"))
    "foo.VALID_TIME CONTAINS PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"

    '(and (<= x1 #time/zoned-date-time "2000-01-01T00:00Z")
          (>= x2 #time/zoned-date-time "2000-01-01T00:00Z"))
    "foo.VALID_TIME CONTAINS TIMESTAMP '2000-01-01 00:00:00+00:00'"

    ;; also testing all period-predicate permutations
    '(and (< x1 #time/zoned-date-time "2001-01-01T00:00Z")
          (> x2 #time/zoned-date-time "2000-01-01T00:00Z"))
    "foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"

    '(and (< x1 x2) (> x2 x1))
    "foo.VALID_TIME OVERLAPS foo.VALID_TIME"

    '(and (< #time/zoned-date-time "2000-01-01T00:00Z" #time/zoned-date-time "2003-01-01T00:00Z")
          (> #time/zoned-date-time "2001-01-01T00:00Z" #time/zoned-date-time "2002-01-01T00:00Z"))
    "PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')
    OVERLAPS PERIOD (TIMESTAMP '2002-01-01 00:00:00+00:00', TIMESTAMP '2003-01-01 00:00:00+00:00')"

    '(and (= x1 #time/zoned-date-time "2000-01-01T00:00Z")
          (= x2 #time/zoned-date-time "2001-01-01T00:00Z"))
    "foo.SYSTEM_TIME EQUALS PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"

    '(<= x2 #time/zoned-date-time "2000-01-01T00:00Z")
    "foo.VALID_TIME PRECEDES PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"

    '(>= x1 #time/zoned-date-time "2001-01-01T00:00Z")
    "foo.SYSTEM_TIME SUCCEEDS PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"

    '(= x2 #time/zoned-date-time "2000-01-01T00:00Z")
    "foo.VALID_TIME IMMEDIATELY PRECEDES PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"

    '(= x1 #time/zoned-date-time "2001-01-01T00:00Z")
    "foo.VALID_TIME IMMEDIATELY SUCCEEDS PERIOD (TIMESTAMP '2000-01-01 00:00:00+00:00', TIMESTAMP '2001-01-01 00:00:00+00:00')"))

(deftest test-period-predicates-point-in-time
  (t/are [expected sql] (= expected (plan/plan-expr sql))

    '(and (<= x1 x3) (>= x2 x3))
    "foo.valid_time CONTAINS foo.other_column"

    '(and
      (<= x1 #time/zoned-date-time "2010-01-01T11:10:11Z")
      (>= x2 #time/zoned-date-time "2010-01-01T11:10:11Z"))
    "foo.valid_time CONTAINS TIMESTAMP '2010-01-01T11:10:11Z'"

    '(and (<= x1 x3) (>= x2 x3))
    "foo.valid_time CONTAINS PERIOD(foo.baz, foo.baz)"

    '(and (<= x1 x3) (>= x2 x3))
    "foo.valid_time CONTAINS foo.xt$system_from"))

(deftest test-period-predicates-point-in-time-erros

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"Error Planning SQL: f.other_column is not a Period"
         (plan-sql "SELECT f.foo FROM foo f WHERE f.valid_time OVERLAPS f.other_column")))

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"Invalid SQL query: Parse error at line 1, column 63:"
         (plan-sql
          "SELECT f.foo FROM foo f WHERE f.valid_time OVERLAPS TIMESTAMP '2010-01-01T11:10:11Z'"))))

(deftest test-min-long-value-275
  (t/is (= Long/MIN_VALUE (plan/plan-expr "-9223372036854775808"))))

(deftest test-multiple-references-to-temporal-cols
  (t/is
   (=plan-file
    "multiple-references-to-temporal-cols"
    (plan-sql "SELECT foo.xt$valid_from, foo.xt$valid_to, foo.xt$system_from, foo.xt$system_to
                FROM foo FOR SYSTEM_TIME FROM DATE '2001-01-01' TO DATE '2002-01-01'
                WHERE foo.xt$valid_from = 4 AND foo.xt$valid_to > 10
                AND foo.xt$system_from = 20 AND foo.xt$system_to <= 23
                AND foo.VALID_TIME OVERLAPS PERIOD (DATE '2000-01-01', DATE '2004-01-01')"))))

(deftest test-sql-insert-plan
  (t/is (= '[:insert
             {:table "users"}
             [:rename
              {x1 xt$id, x2 name, x5 xt$valid_from}
              [:project
               [x1 x2 {x5 (cast-tstz x3)}]
               [:table [x1 x2 x3] [{x1 ?_0, x2 ?_1, x3 ?_2}]]]]]
           (plan-sql "INSERT INTO users (xt$id, name, xt$valid_from) VALUES (?, ?, ?)")
           (plan-sql
            "INSERT INTO users
             SELECT bar.xt$id, bar.name, bar.xt$valid_from
             FROM (VALUES (?, ?, ?)) AS bar(xt$id, name, xt$valid_from)")
           (plan-sql
            "INSERT INTO users (xt$id, name, xt$valid_from)
             SELECT bar.xt$id, bar.name, bar.xt$valid_from
             FROM (VALUES (?, ?, ?)) AS bar(xt$id, name, xt$valid_from)")))

  (t/is (=plan-file "test-sql-insert-plan-309"
                    (plan-sql "INSERT INTO customer (xt$id, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"))
        "#309")

  (t/is (=plan-file "test-sql-insert-plan-398"
                    (plan-sql "INSERT INTO foo (xt$id, xt$valid_from) VALUES ('foo', DATE '2018-01-01')"))))

(deftest test-sql-delete-plan
  (t/is (=plan-file "test-sql-delete-plan"
                    (plan-sql "DELETE FROM users FOR PORTION OF VALID_TIME FROM DATE '2020-05-01' TO END_OF_TIME AS u WHERE u.id = ?"))))

(deftest test-sql-update-plan
  (t/is (=plan-file "test-sql-update-plan"
                    (plan-sql "UPDATE users FOR PORTION OF VALID_TIME FROM DATE '2021-07-01' TO END_OF_TIME AS u SET first_name = 'Sue' WHERE u.id = ?")))

  (t/is (=plan-file "test-sql-update-plan-with-column-references"
                    (plan-sql "UPDATE foo SET bar = foo.baz"
                              {:default-all-valid-time? true})))

  (t/is (=plan-file "test-sql-update-plan-with-period-references"
                    (plan-sql "UPDATE foo SET bar = (foo.SYSTEM_TIME OVERLAPS foo.VALID_TIME)"
                              {:default-all-valid-time? true}))))

(deftest dml-target-table-alises
  (t/is (= (plan-sql "UPDATE t1 AS u SET col1 = 30")
           (plan-sql "UPDATE t1 AS SET SET col1 = 30")
           (plan-sql "UPDATE t1 u SET col1 = 30")
           (plan-sql "UPDATE t1 SET col1 = 30"))
        "UPDATE")

  (t/is (= (plan-sql "DELETE FROM t1 AS u WHERE u.col1 = 30")
           (plan-sql "DELETE FROM t1 u WHERE u.col1 = 30")
           (plan-sql "DELETE FROM t1 WHERE t1.col1 = 30"))
        "DELETE"))

(deftest test-system-time-period-predicate
  (t/is
    (=plan-file
      "test-system-time-period-predicate-full-plan"
      (plan-sql
        "SELECT foo.name, bar.name
        FROM foo, bar
        WHERE foo.SYSTEM_TIME OVERLAPS bar.SYSTEM_TIME"))))

(deftest test-valid-time-correlated-subquery
  (t/is
   (=plan-file
    "test-valid-time-correlated-subquery-where"
    (plan-sql
     "SELECT (SELECT foo.name
        FROM foo
        WHERE foo.VALID_TIME OVERLAPS bar.VALID_TIME) FROM bar")))

  (t/is
   (=plan-file
    "test-valid-time-correlated-subquery-projection"
    (plan-sql
     "SELECT (SELECT (foo.VALID_TIME OVERLAPS bar.VALID_TIME) FROM foo)
        FROM bar"))))

(deftest test-derived-columns-with-periods
  (t/is
   (=plan-file
    "test-derived-columns-with-periods-period-predicate"
    (plan-sql
     "SELECT f.VALID_TIME OVERLAPS f.SYSTEM_TIME
        FROM foo
        AS f (xt$system_from, xt$system_to, xt$valid_from, xt$valid_to)")))

  (t/is
   (=plan-file
    "test-derived-columns-with-periods-period-specs"
    (plan-sql
     "SELECT f.bar
        FROM foo
        FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP
        FOR VALID_TIME AS OF CURRENT_TIMESTAMP
        AS f (bar)"))))

(deftest test-for-all-valid-time-387
  (t/is
   (=plan-file
    "test-for-all-valid-time-387-query"
    (plan-sql
     "SELECT foo.bar
        FROM foo
        FOR ALL VALID_TIME")))

  (t/is
   (=plan-file
    "test-for-all-valid-time-387-query"
    (plan-sql
     "SELECT foo.bar
        FROM foo
        FOR VALID_TIME ALL")))

  (t/is
   (=plan-file
    "test-for-all-valid-time-387-update"
    (plan-sql
     "UPDATE users FOR ALL VALID_TIME SET first_name = 'Sue'")))

  (t/is
   (=plan-file
    "test-for-all-valid-time-387-delete"
    (plan-sql
     "DELETE FROM users FOR ALL VALID_TIME")))

  (t/is
   (=plan-file
    "test-for-all-valid-time-387-delete"
    (plan-sql
     "DELETE FROM users FOR VALID_TIME ALL"))))

(deftest test-for-all-system-time-404
  (t/is
   (=plan-file
    "test-for-all-system-time-404"
    (plan-sql
     "SELECT foo.bar
        FROM foo
        FOR ALL SYSTEM_TIME")))

  (t/is
   (=plan-file
    "test-for-all-system-time-404"
    (plan-sql
     "SELECT foo.bar
        FROM foo
        FOR SYSTEM_TIME ALL"))))

(deftest test-period-specs-with-subqueries-407

  (t/is
   (=plan-file
    "test-period-specs-with-subqueries-407-system-time"
    (plan-sql
     "SELECT 1 FROM (select foo.bar from foo FOR ALL SYSTEM_TIME) as tmp")))

  (t/is
   (=plan-file
    "test-period-specs-with-subqueries-407-app-time"
    (plan-sql
     "SELECT 1 FROM (select foo.bar from foo FOR VALID_TIME AS OF CURRENT_TIMESTAMP) as tmp")))

  (t/is
   (=plan-file
    "test-period-specs-with-dml-subqueries-and-defaults-407" ;;also #424
    (plan-sql "INSERT INTO prop_owner (xt$id, customer_number, property_number, xt$valid_from, xt$valid_to)
                SELECT 1,
                145,
                7797, DATE '1998-01-03', tmp.app_start
                FROM
                (SELECT MIN(Prop_Owner.xt$system_from) AS app_start
                FROM Prop_Owner
                FOR ALL SYSTEM_TIME
                WHERE Prop_Owner.id = 1) AS tmp"
              {:default-all-valid-time? false}))))

(deftest parenthesized-joined-tables-are-unboxed-502
  (t/is (= (plan-sql "SELECT 1 FROM ( tab0 JOIN tab2 ON TRUE )")
           (plan-sql "SELECT 1 FROM tab0 JOIN tab2 ON TRUE"))))

(deftest test-with-clause
  (t/is (=plan-file
          "test-with-clause"
          (plan-sql "WITH foo AS (SELECT bar.id FROM bar WHERE bar.id = 5)
                    SELECT foo.id, baz.id
                    FROM foo, foo AS baz"))))

(deftest test-delimited-identifiers-in-insert-column-list-2549
  (t/is (=plan-file
          "test-delimited-identifiers-in-insert-column-list-2549"
          (plan-sql
            "INSERT INTO posts (\"xt$id\", \"user_id\") VALUES (1234, 5678)"))))

(deftest test-table-period-specification-ordering-2260
  (let [opts {:table-info {"foo" #{"bar"}}}
        v-s (plan-sql
              "SELECT foo.bar
               FROM foo
                 FOR ALL VALID_TIME
                 FOR ALL SYSTEM_TIME"
              opts)
        s-v (plan-sql
              "SELECT foo.bar
               FROM foo
                 FOR ALL SYSTEM_TIME
                 FOR ALL VALID_TIME"
              opts)]

    (t/is (=plan-file "test-table-period-specification-ordering-2260-v-s" v-s))

    (t/is (=plan-file "test-table-period-specification-ordering-2260-s-v" s-v))

    (t/is (= v-s s-v))))

(deftest array-agg-decorrelation

  (t/testing "ARRAY_AGG is not decorrelated using rule-9 as array_agg(null) =/= array_agg(empty-rel)"

    (t/is (=plan-file
           "array-agg-decorrelation-1"
           (plan-sql "SELECT (SELECT sum(x.y) FROM (VALUES (1), (2), (3), (tab0.z)) AS x(y)) FROM tab0")))
    (t/is (=plan-file
           "array-agg-decorrelation-2"
           (plan-sql "SELECT (SELECT ARRAY_AGG(x.y) FROM (VALUES (1), (2), (3), (tab0.z)) AS x(y)) FROM tab0")))))

(deftest test-order-by-3065
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :x 3}]
                           [:put-docs :docs {:xt/id 2 :x 2}]
                           [:put-docs :docs {:xt/id 3 :x 1}]])

  (t/is (= [{:x 1, :xt/id 3} {:x 2, :xt/id 2} {:x 3, :xt/id 1}]
           (xt/q tu/*node* "SELECT * FROM docs ORDER BY docs.x")))

  (t/is (= [{:x 1, :xt/id 3} {:x 2, :xt/id 2} {:x 3, :xt/id 1}]
           (xt/q tu/*node* "SELECT * FROM docs ORDER BY docs.x + 1")))

  (t/is (= #{{:x 1, :xt/id 3} {:x 2, :xt/id 2} {:x 3, :xt/id 1}}
           (set (xt/q tu/*node* "SELECT * FROM docs ORDER BY 1 + 1"))))

  (t/is (= [{:xt/id 3} {:xt/id 2} {:xt/id 1}]
           (xt/q tu/*node* "SELECT docs.xt$id FROM docs ORDER BY docs.x"))))

(deftest test-order-by-unqualified-derived-column-refs
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :x 3}]
                           [:put-docs :docs {:xt/id 2 :x 2}]
                           [:put-docs :docs {:xt/id 3 :x 1}]])

  (t/is (= [{:b 2} {:b 3} {:b 4}]
             (xt/q tu/*node* "SELECT (docs.x + 1) AS b FROM docs ORDER BY b")))
  (t/is (= [{:b 1} {:b 2} {:b 3}]
           (xt/q tu/*node* "SELECT docs.x AS b FROM docs ORDER BY b")))
  (t/is (= [{:x 1} {:x 2} {:x 3}]
           (xt/q tu/*node* "SELECT y.x FROM docs AS y ORDER BY x")))

  ;; Postgres doesn't allow deliminated col refs in order-by exprs but mysql/sqlite do
  #_(t/is (= [{:b 1} {:b 2} {:b 3}]
           (xt/q tu/*node* "SELECT docs.x AS b FROM docs ORDER BY (b + 2)"))))

(deftest test-select-star-projections
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :x 3 :y "a"}]
                           [:put-docs :docs {:xt/id 2 :x 2 :y "b"}]
                           [:put-docs :docs {:xt/id 3 :x 1 :y "c"}]])

  (t/is (= [{:y "b"} {:y "a"} {:y "c"}]
           (xt/q tu/*node* "SELECT docs.y FROM docs")))

  (t/is (= #{{:x 2, :y "b", :bar "b", :xt/id 2}
             {:x 3, :y "a", :bar "a", :xt/id 1}
             {:x 1, :y "c", :bar "c", :xt/id 3}}
           (set (xt/q tu/*node* "SELECT docs.*, docs.y AS bar FROM docs"))))

  (t/is (= #{{:x 2, :y "b", :xt/id 2}
             {:x 3, :y "a", :xt/id 1}
             {:x 1, :y "c", :xt/id 3}}
           (set (xt/q tu/*node* "SELECT docs.*, docs.y FROM docs"))))

  (t/is (= #{{:xt/valid-from #time/zoned-date-time "2020-01-01T00:00Z[UTC]",
              :x 2,
              :y "b",
              :xt/id 2}
             {:xt/valid-from #time/zoned-date-time "2020-01-01T00:00Z[UTC]",
              :x 3,
              :y "a",
              :xt/id 1}
             {:xt/valid-from #time/zoned-date-time "2020-01-01T00:00Z[UTC]",
              :x 1,
              :y "c",
              :xt/id 3}}
           (set (xt/q tu/*node* "SELECT docs.*, docs.xt$valid_from FROM docs"))))

  (t/is (= #{{:x 2,
              :y "b",
              :xt/id 2,
              :xt/valid-from #time/zoned-date-time "2020-01-01T00:00Z[UTC]"}
             {:x 3,
              :y "a",
              :xt/id 1,
              :xt/valid-from #time/zoned-date-time "2020-01-01T00:00Z[UTC]"}
             {:x 1,
              :y "c",
              :xt/id 3,
              :xt/valid-from #time/zoned-date-time "2020-01-01T00:00Z[UTC]"}}
             (set (xt/q tu/*node* "SELECT docs.*, docs.xt$valid_from FROM docs WHERE docs.xt$system_to = docs.xt$valid_to")))))

(deftest test-select-star-qualified-join
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1}]
                           [:put-docs :bar {:xt/id 2 :a "one"}]])

  (t/is (= [{:xt/id 1, :a "one", :xt/id:1 2}]
           (xt/q tu/*node* "SELECT * FROM foo JOIN bar ON true")))

  (t/is (= [{:xt/id 1, :a "one", :xt/id:1 2}]
           (xt/q tu/*node* "FROM foo JOIN bar ON true"))))

(deftest test-expand-asterisk-parenthesized-joined-table
  ;;Parens in the place of a table primary creates a parenthesized_joined_table
  ;;which is a qualified join, but nested inside a table primary. This test checks
  ;;that the expand asterisk attribute continues to traverse through the outer table
  ;;primary to find the nested table primaries.

  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1}]
                           [:put-docs :bar {:xt/id 2 :a "one"}]])

  (t/is (= [{:xt/id 1, :a "one", :xt/id:1 2}]

           (xt/q tu/*node*
                 "SELECT * FROM ( foo LEFT JOIN bar ON true )"))))

(deftest test-select-star-lateral-join
  (xt/submit-tx tu/*node* [[:put-docs :y {:xt/id 1 :b "one"}]])

  (t/is (= [{:b "one", :b:1 "one"}]
           (xt/q tu/*node*
                 "SELECT * FROM (SELECT y.b FROM y) AS x, LATERAL (SELECT y.b FROM y) AS z"))))

(deftest test-select-star-subquery
  (xt/submit-tx tu/*node* [[:put-docs :y {:xt/id 1 :b "one" :a 2}]])

  (t/is (= [{:b "one"}]
           (xt/q tu/*node*
                 "SELECT * FROM (SELECT y.b FROM y) AS x"))))

(deftest test-sql-over-scanning
  (t/is
   (=plan-file
    "test-sql-over-scanning-col-ref"
    (plan-sql "SELECT foo.name FROM foo"
              {:table-info {"foo" #{"name" "lastname"}}}))

   "Tests only those columns required by the query are scanned for, rather than all those present on the base table")

  (t/is
   (=plan-file
    "test-sql-over-scanning-qualified-asterisk"
    (plan-sql "SELECT foo.*, bar.jame FROM foo, bar"
              {:table-info {"foo" #{"name" "lastname"}
                            "bar" #{"jame" "lastjame"}}})))

  (t/is
   (=plan-file
    "test-sql-over-scanning-asterisk"
    (plan-sql "SELECT * FROM foo, bar"
              {:table-info {"foo" #{"name" "lastname"}
                            "bar" #{"jame" "lastjame"}}})))

  (t/is
   (=plan-file
    "test-sql-over-scanning-asterisk-subquery"
    (plan-sql "SELECT foo.*, (SELECT * FROM baz) FROM foo, bar"
              {:table-info {"foo" #{"name" "lastname"}
                            "bar" #{"jame" "lastjame"}
                            "baz" #{"frame"}}})))
  (t/is
   (=plan-file
    "test-sql-over-scanning-asterisk-from-subquery"
    (plan-sql "SELECT bar.* FROM (SELECT foo.a, foo.b FROM foo) AS bar"
              {:table-info {"foo" #{"a" "b"}}}))))

(deftest test-schema-qualified-names
  (t/is
   (=plan-file
    "test-schema-qualified-names-fully-qualified"
    (plan-sql "SELECT information_schema.columns.column_name FROM information_schema.columns")))

  (t/is
   (=plan-file
    "test-schema-qualified-names-aliased-table"
    (plan-sql "SELECT f.column_name FROM information_schema.columns AS f")))
  (t/is
   (=plan-file
    "test-schema-qualified-names-implict-pg_catalog"
    (plan-sql "SELECT pg_attribute.attname FROM pg_attribute")))

  (t/is
   (=plan-file
    "test-schema-qualified-names-unqualified-col-ref"
    (plan-sql "SELECT pg_attribute.attname FROM pg_catalog.pg_attribute")))

  (t/is
   (=plan-file
    "test-schema-qualified-names-qualified-col-ref"
    (plan-sql "SELECT pg_catalog.pg_attribute.attname FROM pg_attribute")))

  (t/is
   (=plan-file
    "test-schema-qualified-names-field"
    (plan-sql "SELECT information_schema.columns.column_name.my_field FROM information_schema.columns")))

  ;;errors
  ;;
  (t/testing "Invalid Queries"
    (t/is
     (thrown-with-msg?
      IllegalArgumentException
      #"PG_CATALOG.columns.column_name is an invalid reference to columns, schema name does not match"
      (plan-sql "SELECT pg_catalog.columns.column_name FROM information_schema.columns")))

    (t/is
     (thrown-with-msg?
      IllegalArgumentException
      #"INFORMATION_SCHEMA.f.column_name is an invalid reference to f, schema name does not match"
      (plan-sql "SELECT information_schema.f.column_name FROM information_schema.columns AS f")))

    (t/is
     (thrown-with-msg?
      IllegalArgumentException
      #"PG_CATALOG.f.column_name is an invalid reference to f, schema name does not match"
      (plan-sql "SELECT pg_catalog.f.column_name FROM information_schema.columns AS f")))))

(t/deftest test-generated-column-names
  (t/is (= [{:xt/column-1 1, :xt/column-2 3}]
           (xt/q tu/*node* "SELECT LEAST(1,2), LEAST(3,4) FROM (VALUES (1)) x")))
  
  (t/testing "Aggregates"
    (t/is (= [{:xt/column-1 1}]
             (xt/q tu/*node* "SELECT COUNT(*) FROM (VALUES (1)) x"))))
  
  (t/testing "ARRAY()"
   (xt/submit-tx tu/*node* [[:put-docs :a {:xt/id 1 :a 42}]
                            [:put-docs :b {:xt/id 2 :b1 "one" :b2 42}]])

    (t/is (= [{:xt/column-1 ["one"]}]
             (xt/q tu/*node* "SELECT ARRAY(select b.b1 from b where b.b2 = 42) FROM a where a.a = 42")))))

(t/deftest test-postgres-session-variables
  ;; These currently return hard-coded values.
  (t/is (= [{:xt/column-1 "xtdb"}]
           (xt/q tu/*node* "SELECT current_user")))
  
  (t/is (= [{:xt/column-1 "xtdb"}]
           (xt/q tu/*node* "SELECT current_database")))
  
  (t/is (= [{:xt/column-1 "public"}]
           (xt/q tu/*node* "SELECT current_schema"))))

(t/deftest test-postgres-access-control-functions
  ;; These current functions should always should return true
  (t/are [sql expected] (= expected (plan/plan-expr sql))
    "has_table_privilege('xtdb','docs', 'select')" true
    "has_table_privilege('docs', 'select')" true
    "pg_catalog.has_table_privilege('docs', 'select')" true

    "has_schema_privilege('xtdb', 'public', 'select')" true
    "has_schema_privilege('public', 'select')" true
    "pg_catalog.has_schema_privilege('public', 'select')" true

    "has_table_privilege(current_user, 'docs', 'select')" true
    "has_schema_privilege(current_user, 'public', 'select')" true)

  (t/testing "example SQL query"
    (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :x 3}]])

    (t/is (= [{:x 3}]
             (xt/q tu/*node* "SELECT docs.x FROM docs WHERE has_table_privilege('docs', 'select') ")))))

(t/deftest test-select-without-from
  (t/is (= [{:xt/column-1 1}]
           (xt/q tu/*node* "SELECT 1")))
  
  (t/is (= [{:xt/column-1 1 :xt/column-2 2}]
           (xt/q tu/*node* "SELECT 1, 2")))
  
  (t/is (= [{:xt/column-1 "xtdb"}]
           (xt/q tu/*node* "SELECT current_user"))))

(t/deftest test-nest
  (t/is (=plan-file "test-nest-one"
          (plan-sql "SELECT o.xt$id AS order_id, o.value,
                            NEST_ONE(SELECT c.name FROM customers c WHERE c.xt$id = o.customer_id) AS customer
                     FROM orders o")))

  (t/is (=plan-file "test-nest-many"
          (plan-sql "SELECT c.xt$id AS customer_id, c.name,
                            NEST_MANY(SELECT o.xt$id AS order_id, o.value
                                      FROM orders o
                                      WHERE o.customer_id = c.xt$id)
                              AS orders
                     FROM customers c")))

  (xt/submit-tx tu/*node* [[:put-docs :customers {:xt/id 0, :name "bob"}]
                           [:put-docs :customers {:xt/id 1, :name "alice"}]
                           [:put-docs :orders {:xt/id 0, :customer-id 0, :value 26.20}]
                           [:put-docs :orders {:xt/id 1, :customer-id 0, :value 8.99}]
                           [:put-docs :orders {:xt/id 2, :customer-id 1, :value 12.34}]])


  (t/is (= #{{:customer {:name "bob"}, :order-id 0, :value 26.20}
             {:customer {:name "bob"}, :order-id 1, :value 8.99}
             {:customer {:name "alice"}, :order-id 2, :value 12.34}}
           (set (xt/q tu/*node*
                      "SELECT o.xt$id AS order_id, o.value,
                              NEST_ONE(SELECT c.name FROM customers c WHERE c.xt$id = o.customer_id) AS customer
                       FROM orders o"))))

  (t/is (= #{{:orders [{:order-id 1, :value 8.99} {:order-id 0, :value 26.20}], :name "bob", :customer-id 0}
             {:orders [{:order-id 2, :value 12.34}], :name "alice", :customer-id 1}}
           (set (xt/q tu/*node*
                      "SELECT c.xt$id AS customer_id, c.name,
                              NEST_MANY(SELECT o.xt$id AS order_id, o.value
                                        FROM orders o
                                        WHERE o.customer_id = c.xt$id)
                                AS orders
                       FROM customers c")))))

(deftest test-invalid-xt-id-in-query-3324
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 0 :name "bob"}]])
  (t/is (= [] (xt/q tu/*node* "SELECT foo.name FROM foo WHERE foo.xt$id = NULL")))
  (t/is (= [] (xt/q tu/*node* "SELECT foo.name FROM foo WHERE foo.xt$id = ?" {:args [nil]}))))

(deftest test-random-fn
  (t/is (= true (-> (xt/q tu/*node* "SELECT 0.0 <= random() AS greater") first :greater)))
  (t/is (= true (-> (xt/q tu/*node* "SELECT random() < 1.0 AS smaller ") first :smaller))))

(t/deftest test-tx-ops-sql-params
  (t/testing "correct number of args"
    (t/is (= (serde/->tx-committed 0 #time/instant "2020-01-01T00:00:00Z")
             (xt/execute-tx tu/*node* [[:sql "INSERT INTO users(xt$id, u_name) VALUES (?, ?)" [1 "dan"] [2 "james"]]])))

    (t/is (= [{:u-name "dan", :xt/id 1}
              {:u-name "james", :xt/id 2}]
             (xt/q tu/*node* "SELECT users.xt$id, users.u_name FROM users ORDER BY xt$id"))))

  (t/testing "no arg rows provided when args expected"
    (t/is (= (serde/->tx-aborted 1
                                 #time/instant "2020-01-02T00:00:00Z"
                                 #xt/runtime-err [:xtdb.indexer/missing-sql-args "Arguments list was expected but not provided" {:param-count 2}])
             (xt/execute-tx tu/*node* [[:sql "INSERT INTO users(xt$id, u_name) VALUES (?, ?)"]]))))

  (t/testing "incorrect number of args on all arg-row"
    (t/is (= (serde/->tx-aborted 2
                                 #time/instant "2020-01-03T00:00:00Z"
                                 #xt/runtime-err [:xtdb.indexer/incorrect-sql-arg-count "1 arguments were provided and 2 arguments were provided" {:param-count 2, :arg-count 1}])
             (xt/execute-tx tu/*node* [[:sql "INSERT INTO users(xt$id, u_name) VALUES (?, ?)" [3] [4]]]))))

  (t/testing "incorrect number of args on one row"
    (t/is (thrown-with-msg?
           IllegalArgumentException
           #"All SQL arg-rows must have the same number of columns"
           (xt/submit-tx tu/*node* [[:sql "INSERT INTO users(xt$id, u_name) VALUES (?, ?)" [3 "finn"] [4]]])))))

(t/deftest test-case-sensitivity-in-delimited-cols
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO T1(xt$id, col1, col2) VALUES(1,'fish',1000)"]])
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO t1(xt$id, COL1, COL2) VALUES(2,'dog',2000)"]])

  (t/is (= [{:xt/id 2, :col1 "dog", :col2 2000}
            {:xt/id 1, :col1 "fish", :col2 1000}]
           (xt/q tu/*node* "SELECT t1.XT$ID, T1.col1, t1.COL2 FROM T1")
           (xt/q tu/*node* "SELECT \"t1\".xt$id, T1.\"col1\", t1.COL2 FROM t1")))

  (xt/submit-tx tu/*node* [[:sql
                            "INSERT INTO \"T1\"(xt$id, \"CoL1\", \"col2\") VALUES(3,'cat',3000)"]])

  (t/is (= [{:xt/id 3, :CoL1 "cat", :col2 3000}]
           (xt/q tu/*node*
                 "SELECT \"T1\".xt$id, \"T1\".\"CoL1\", \"T1\".COL2 FROM \"T1\"")))

  (t/is (= [{:xt/id 3, :col2 3000}]
           (xt/q tu/*node*
                 "SELECT \"T1\".xt$id, \"T1\".col1, \"T1\".COL2 FROM \"T1\""))
        "can't refer to it as `col1`, have to quote")

  (xt/submit-tx tu/*node* [[:sql "UPDATE T1 SET Col1 = 'cat' WHERE t1.COL2 IN (313, 2000)"]])

  (t/is (= [{:xt/id 2, :col1 "cat", :col2 2000}
            {:xt/id 1, :col1 "fish", :col2 1000}]
           (xt/q tu/*node* "SELECT t1.xt$id, T1.col1, \"t1\".col2 FROM t1")))

  (t/is (= [{:col1 "cat", :avg 2000.0} {:col1 "fish", :avg 1000.0}]
           (xt/q tu/*node* "SELECT T1.col1, AVG(t1.col2) avg FROM t1 GROUP BY T1.col1")))

  (t/is (= [{:col2 3000}]
           (xt/q tu/*node*
                 "SELECT \"TEEONE\".col2 FROM \"T1\" AS \"TEEONE\" WHERE \"TEEONE\".\"CoL1\" IN ( SELECT t1.\"col1\" FROM T1 WHERE T1.col1 = \"TEEONE\".\"CoL1\" ) ORDER BY \"TEEONE\".col2")))

  (xt/submit-tx tu/*node* [[:sql "DELETE FROM T1 WHERE t1.Col1 = 'fish'"]])

  (t/is (= [{:xt/id 2}]
           (xt/q tu/*node* "SELECT t1.XT$ID FROM T1")))

  (xt/submit-tx tu/*node* [[:sql
                            "DELETE FROM \"T1\" WHERE \"T1\".\"col2\" IN (2000, 3000)"]])

  (t/is (= [] (xt/q tu/*node* "SELECT \"T1\".xt$id FROM \"T1\""))))

(t/deftest test-ordering-of-intersect-and-union
  (xt/submit-tx tu/*node* [[:put-docs :t1 {:xt/id 1 :x 1}]
                           [:put-docs :t2 {:xt/id 1 :x 1}]
                           [:put-docs :t3 {:xt/id 1 :x 3}]])

  (t/is (= [{:x 1} {:x 3}]
           (xt/q tu/*node* "SELECT x FROM t1 INTERSECT SELECT x FROM t2 UNION SELECT x FROM t3"))))
