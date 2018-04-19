(ns net.zopg.zo.parse-name-test
  (:require
   [clojure.string :as string]
   [clojure.test :refer [are deftest is]]
   [net.zopg.zo.anomalies :as anom])
  (:import
   (clojure.lang ExceptionInfo)
   (net.zopg.zo.util Parser)))

(defn quotify [^String s]
  (when s
    (.replaceAll s "%" "\"")))

(defn parse-name [^String s]
  (try
    (Parser/parseIdent s)
    (catch ExceptionInfo e
      {::anom/category ::anom/incorrect})))

(deftest empty-string-incorrect?
  (is (= {::anom/category ::anom/incorrect} (parse-name "")))
  (is (= {::anom/category ::anom/incorrect} (parse-name (str "foo" "\0" "bar"))))
  (is (= {::anom/category ::anom/incorrect} (parse-name "[]"))))

(deftest ary?
  (is (= "[]" Parser/ARRAY_SUFFIX)))

(deftest parse-names
  (are [s expected] (let [[nspname typname array?] expected
                          expected' (merge {:array? false}
                                           {:nspname (quotify nspname)
                                            :typname  (quotify typname)
                                            :simple? (nil? nspname)}
                                           (when array? {:array? array?}))
                          s' (quotify s)]
                      (is (= expected' (parse-name s'))))
    "text" [nil "text"]
    "int2" [nil "int2"]
    "int4" [nil "int4"]

    "int4[]" [nil "int4" true]

    "%%%%.%%%%"      ["%" "%"]
    "%%%%.%.%"       ["%" "."]
    "%%%%.%%%.%"     ["%" "%."]
    "%%%%.%.%%%%%"   ["%" ".%%"]
    "%.%.%%%%"       ["." "%"]
    "%.%.%.%"        ["." "."]
    ;; "%.%"            [nil "." false true]
    ;; "%%%%.%.%"       ["\"" "." false true]
    "ns.element"     ["ns" "element"]
    "public.element" ["public" "element"]

    "ns.% %"  ["ns" " "]
    "ns.%%%%" ["ns" "%"]
    "ns.%.%"  ["ns" "."]

    "pg_catalog.text"    ["pg_catalog" "text"]
    "pg_catalog.int2"    ["pg_catalog" "int2"]
    "pg_catalog.int4"    ["pg_catalog" "int4"]
    "public.n"           ["public" "n"]
    "public.%NS.%%TYPE%" ["public" "NS.%TYPE"]

    ))


(comment
  (quotify "foo%bar")

  )
