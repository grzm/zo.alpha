(ns net.zopg.zo.query.parse-test
  (:require
   [clojure.test :refer [deftest are is]]
   [net.zopg.zo.alpha.query :refer [parse]]))

(deftest parse-test
  (are [args expected]
      (is (= expected (parse args)))

    [:val "SELECT 1"]
    {:query-string "SELECT 1"
     :row-fn first
     :result-fn first
     :params []
     :encoders []
     :decoders []
     :as-vector? true}

    [:col "VALUES (1), (2), (3)"]
    {:query-string "VALUES (1), (2), (3)"
     :params []
     :encoders []
     :decoders []
     :row-fn first
     :result-fn identity
     :as-vector? true}

    [:row "SELECT 1, 2, 3"]
    {:query-string "SELECT 1, 2, 3"
     :params []
     :encoders []
     :decoders []
     :row-fn identity
     :result-fn first
     :as-vector? false}

    [:tab "VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)"]
    {:query-string "VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)"
     :params []
     :encoders []
     :decoders []
     :row-fn identity
     :result-fn identity
     :as-vector? false}

    [:col "SELECT user_id FROM users WHERE country_code = ? LIMIT 10" ["CA"]]
    {:query-string "SELECT user_id FROM users WHERE country_code = ? LIMIT 10"
     :params ["CA"]
     :encoders []
     :decoders []
     :row-fn first
     :result-fn identity
     :as-vector? true}))

(deftest map-parse-test
  (are [args expected]
      (is (= expected (parse args)))

    {:sql "SELECT 1"}
    {:query-string "SELECT 1"
     :params []
     :encoders []
     :decoders []
     :row-fn identity
     :result-fn identity
     :as-vector? false}

    {:sql "SELECT $1"
     :params [1]}
    {:query-string "SELECT $1"
     :params [1]
     :encoders []
     :decoders []
     :row-fn identity
     :result-fn identity
     :as-vector? false}))
