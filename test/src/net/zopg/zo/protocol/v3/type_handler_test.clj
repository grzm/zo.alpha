(ns net.zopg.zo.protocol.v3.type-handler-test
  (:require
   [byte-streams :refer [to-byte-buffer]]
   [net.zopg.zo.alpha.types :as types]
   [net.zopg.zo.alpha.types.core-registry :as core-registry]
   [net.zopg.zo.alpha.protocol.v3.type-handler :as type-handler]
   [clojure.test :as test :refer [are deftest is]]))

(deftest decoder
  (let [type-info nil
        registry  (core-registry/core-registry)]
    (are [expected args]
        (let [[field buffer] args
              decode (types/decoder-for registry type-info field)]
          (= expected (decode buffer)))
      1
      [{:name     "int",
        :attrelid 0,
        :attnum   0,
        :typid    (:int4 types/oids),
        :typlen   4,
        :typmod   -1,
        :format   :text}
       (to-byte-buffer "1")])))

(deftest row-fn-for
  (let [type-info nil
        registry  (core-registry/core-registry)]
    (are [expected args]
        (let [[fields row] args
              decode-row (type-handler/decode-row-fn registry type-info fields [])
              actual (decode-row row)]
          (= expected actual))

      [-15 "foobar"]
      [[{:name     "int"
         :attrelid 0
         :attnum   0
         :typid    (:int4 types/oids)
         :typlen   4
         :typmod   -1
         :format   :text}
        {:name     "text"
         :attrelid 0
         :attnum   0
         :typid    (:text types/oids)
         :typlen   -1
         :typmod   -1
         :format   :text}]
       [(to-byte-buffer "-15")
        (to-byte-buffer "foobar")]])))
