(ns net.zopg.zo.types-test
  (:refer-clojure :exclude [type])
  (:require
   [byte-streams :as bs :refer [to-byte-buffer]]
   [clojure.test :as test :refer [is deftest are]]
   [gloss.io :as io]
   [com.grzm.tespresso.bytes.alpha :as bytes]
   [net.zopg.zo.alpha.types :as types]
   [net.zopg.zo.alpha.types.core-registry :as core-registry])
  (:import
   (java.nio ByteBuffer)))

(alias 'core 'clojure.core)

(deftest decode-text
  (are [expected args] (= expected (apply types/decode-text args))
    1
    [types/int4 (to-byte-buffer "1")]

    "foo"
    [types/text (to-byte-buffer "foo")]))

(deftest text-encode
  (let [tr (core-registry/core-registry)]
    (are [val expected] (let [encoder (types/encoder-for-value tr val)
                              actual (types/text-encode encoder val)]
                          (is (= expected actual)))
      (long 1)   "1"
      (short -1) "-1"
      (int 1)    "1"

      "foo" "foo"

      )))

(defn binary-encoded [tr v]
  (let [encoder (types/encoder-for-value tr v)]
    (-> (types/binary-encode encoder v)
        (.rewind))))

#_(deftest binary-encode
  (let [tr (core-registry/core-registry)]
    (are [val expected]
        (com.grzm.tespresso.bytes/bytes=
          (bytes/byte-buffer expected)
          (binary-encoded tr val))

      #inst "2018-04-04T00:29:14.993-00:00" [0 0x02 0x0B 0xF9 0xAE 0x73 0x91 0x68]
      #inst "2000-01-01T00:00:00-00:00"     [0 0 0 0 0 0 0 0]

      true  [1]
      false [0])))

#_(deftest encode-params-formats
  (let [type-info nil
        tr (core-registry/core-registry)]
    (are [params expected]
        (= expected (:param-formats (types/encode-params type-info tr params)))

      [1 2] [:binary :binary]

      [true false 1 2 "foo" "bar"] [:binary :binary :binary :binary :text :text])))

(deftest binary-decode
  (let [decoder core-registry/timestamptz-decoder
        ts #inst "2000-01-01T00:00:00-00:00"
        encoded (bytes/byte-buffer [0 0 0 0  0 0 0 0])
        actual (java.util.Date/from (types/binary-decode decoder nil encoded))]
    (is (= ts actual))))



(deftype KeywordEnumEncoder []
  types/Encoder
  (-oid [e] (:text types/oid))
  types/TextEncode
  (-text-encode [_ kw _]
    (name kw)))

(def keyword-enum-encoder (KeywordEnumEncoder.))

(deftest custom-decoder
  (let [encoder keyword-enum-encoder]
    (are [v expected] (= expected (types/text-encode encoder v))
      :foo "foo"
      :foo/bar "bar")))
