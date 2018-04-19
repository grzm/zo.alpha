(ns net.zopg.zo.custom-encoder-test
  (:require
   [clojure.core.async :as async :refer [<!!]]
   [clojure.test :refer [are deftest is]]
   [net.zopg.zo.async.alpha :as azo]
   [net.zopg.zo.test.config :refer [conn-params]]
   [net.zopg.zo.session.alpha :as session]
   [net.zopg.zo.alpha.types :as types]
   [net.zopg.zo.alpha.types.core-registry :as core-registry]))

(defn unwrap [res-chan]
  (let [res (<!! res-chan)]
    (if-let [res' (:result res)] res' res)))

(deftype KeywordTextEncoder []
  types/Encoder
  (-oid [e] (:text types/oids))
  types/TextEncode
  (-text-encode [_ kw _]
    (name kw)))

(def keyword-text-encoder (KeywordTextEncoder.))

(deftype ZoMyEnumCodec []
  types/NamedCodec
  (-type-name [_] ["zo" "my_enum"])
  types/TextEncode
  (-text-encode [_ kw _]
    (name kw))
  types/TextDecode
  (-text-decode [_ _ s]
    (keyword s)))

(def zo-my-enum-codec (ZoMyEnumCodec.))

(deftest custom-registry
  (let [registry       (core-registry/core-registry {:named-decoders [zo-my-enum-codec]
                                                     :named-encoders [zo-my-enum-codec]})
        client         (azo/client (merge conn-params {:database "zo"})
                                   {:type-registry registry})]
    (are [query expected]
        (with-open [sess (<!! (azo/connect client))]
          (is (= expected (unwrap (azo/q sess query)))))
      [:val  "SELECT $1::zo.my_enum" [:a]]
      :a)))

(deftest simple-query-named-codec
  (let [client (azo/client (merge conn-params {:database "zo"}))]
    (with-open [sess (<!! (azo/connect client))]
      (is (= [{:my-enum :a}] (unwrap (azo/q sess {:sql      "SELECT 'a'::zo.my_enum"
                                                  :decoders [zo-my-enum-codec]})))))))

(deftest simple-query-named-codec-registry
  (let [named-decoders [zo-my-enum-codec]
        registry (core-registry/core-registry {:named-decoders named-decoders})
        client (azo/client (merge conn-params {:database "zo"})
                           {:type-registry registry})]
    (with-open [sess (<!! (azo/connect client))]
      (is (= :a (unwrap (azo/q sess [:val "SELECT 'a'::zo.my_enum"]))))
      (prn {:sess-state (-> sess :state deref :queries)}))))

(deftype SelfEncoder [encoder v]
  types/Encoder
  (-oid [e] (types/oid encoder))
  types/TextEncode
  (-text-encode [_ _ _]
    (types/text-encode encoder v)))

(deftype ZoMyEnum [v]
  types/ValueEncoder
  (-value-encoder [_]
    (SelfEncoder. keyword-text-encoder v)))

(defn zo-my-enum [v] (ZoMyEnum. v))

(defn run-query [query]
  (let [client (azo/client (merge conn-params {:database "zo"}))]
    (with-open [sess (<!! (azo/connect client))]
      (unwrap (azo/q sess query)))))

#_(deftest per-query-encoders
  (are [query expected] (= expected (run-query query))
    {:sql        "SELECT $1::zo.my_enum"
     :params     [:a]
     :as-vector? true
     :row-fn     first
     :result-fn  first
     :encoders   [zo-my-enum-codec]
     :decoders   [zo-my-enum-codec]}
    :a

    #_[:val  "SELECT $1::zo.my_enum" [(zo-my-enum :a)]]
    #_"a"
    ))
