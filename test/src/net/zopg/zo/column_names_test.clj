(ns net.zopg.zo.column-names-test
  (:require
   [clojure.test :refer [are deftest is]]
   [clojure.test.check]
   [clojure.test.check.clojure-test :as ct :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [net.zopg.zo.session.util :refer [uniquify-names]]))

(deftest uniquify-columns
  (are [names expected]
      (= expected (into [] uniquify-names names))

    ["a" "b" "c"]
    ["a" "b" "c"]

    ["?column?" "?column?"]
    ["?column?" "?column?_2"]

    ["a" "a" "a_1"]
    ["a" "a_2" "a_1"]

    ["a" "a_2" "a"]
    ["a" "a_2" "a_2"] ;; XXX this is still a failure

    ["a" "a" "a_2"]
    ["a" "a_2" "a_2_2"]))

(defspec column-keys-are-unique
  100
  (prop/for-all
    [v (gen/vector gen/string)] ;; not a very good way of checking behavior
    (let [uniqued (into [] uniquify-names v)]
      (= (count v) (count uniqued) (count (set uniqued))))))
