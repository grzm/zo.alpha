(ns net.zopg.zo.async.types-test
  (:require
   [clojure.core.async :as a :refer [<!!]]
   [clojure.test :refer [are deftest is]]
   [net.zopg.zo.async.alpha :as azo]
   [net.zopg.zo.test.config :refer [conn-params]]
   [net.zopg.zo.session.alpha :as session]))

(alias 'sess 'net.zopg.zo.session)

(defn unwrap [res]
  (:result (<!! res)))

(deftest types
  (are [query expected]
      (let [client (azo/client conn-params)]
        (with-open [sess (<!! (azo/connect client))]
          (let [actual (unwrap (azo/q sess query))]
            (= expected actual))))

    ;; int types
    [:rowv "SELECT $1::SMALLINT, $2::INTEGER, $3::BIGINT" [1 2 3]]
    [1 2 3]

    [:rowv "SELECT NULL::SMALLINT, NULL::INTEGER, NULL::BIGINT"]
    [nil nil nil]

    ;; float types
    [:rowv "SELECT $1::FLOAT4, $2::FLOAT8" [1.2 3.4]]
    [(float 1.2) 3.4]

    [:rowv "SELECT NULL::FLOAT4, NULL::FLOAT8"]
    [nil nil]

    ;; numeric types
    [:rowv "SELECT $1::NUMERIC" [1.2]]
    [(bigdec 1.2)]

    [:rowv "SELECT NULL::NUMERIC"]
    [nil]

    ;; text
    [:rowv "SELECT $1::TEXT" ["foobar"]]
    ["foobar"]

    [:rowv "SELECT NULL::TEXT"]
    [nil]

    ;; boolean
    [:rowv "SELECT $1::BOOLEAN, $2::BOOLEAN" [true false]]
    [true false]

    [:rowv "SELECT NULL::BOOLEAN"]
    [nil]

    ;; uuid

    ))

(deftest types-round-trip
  (are [param-val query]
      (let [client (azo/client conn-params)
            query-and-params (conj query [param-val])]
        (with-open [sess (<!! (azo/connect client))]
          (is (= param-val (unwrap (azo/q sess query-and-params))))))

    #uuid "12345678-abcd-efab-1234-0123456789ab"
    [:val "SELECT $1::UUID"]

    ;; XXX unknown type, using timestamptz since we don't have a decoder for this yet
    ;; To test this, we just need a codec that doesn't
    (java.time.Instant/parse "2018-03-10T00:00:00.00000Z")
    [:val "SELECT CAST($1::TEXT AS TIMESTAMPTZ)"]
    ))
