(ns net.zopg.zo.alpha.types
  (:refer-clojure :exclude [cast type])
  (:require
   [byte-streams :as bs]
   [clojure.string :as string]
   [net.zopg.zo.alpha.type-info :as type-info])
  (:import
   (java.time Instant OffsetDateTime)
   (java.time.format DateTimeFormatter)
   (java.util UUID)))

;; this should also take the registry as an argument
;; SELECT typ.oid, typname
;;   FROM pg_type typ
;;   JOIN pg_namespace nsp ON (nsp.oid = typnamespace)
;;   WHERE (nspname, typname) = ($1, $2)

;; this doesn't work: I don't want these to be set when namespace is loaded.

(defprotocol Encoder
  (-oid [e]))

(defn oid [e] (-oid e))

(defprotocol NamedCodec
  (-type-name [e]))

(defn named-codec? [e]
  (satisfies? NamedCodec e))

(defn type-name [e]
  (-type-name e))

(defprotocol ValueEncoder
  (-value-encoder [v]))

(defn value-encoder? [e]
  (satisfies? ValueEncoder e))

;; returns encoder for a given Clojure value
(defprotocol EncoderForValue
  (-encoder-for-value [tr v]))

(defn encoder-for-value
  [tr v]
  (if (value-encoder? v)
    (-value-encoder v)
    (-encoder-for-value tr v)))

(defprotocol TextEncode
  (-text-encode [e v opts]))

(defn text-encode
  ([e v]
   (text-encode e v {}))
  ([e v opts]
   (-text-encode e v opts)))

(defprotocol BinaryEncode
  (-binary-encode [e v opts]))

(defn binary-encode
  ([e v]
   (binary-encode e v {}))
  ([e v opts]
   (-binary-encode e v opts)))

(defn binary-encoder? [e]
  (satisfies? BinaryEncode e))

(def oids
  {:unspecified  0
   :int2         21
   :_int2        1005
   :int4         23
   :_int4        1007
   :int8         20
   :_int8        1016
   :text         25
   :_text        1009
   :numeric      1700
   :_numeric     1231
   :float4       700
   :_float4      1021
   :float8       701
   :_float8      1022
   :bool         16
   :_bool        1000
   :date         1082
   :_date        1182
   :time         1083
   :_time        1183
   :timetz       1266
   :_timetz      1270
   :timestamp    1114
   :_timestamp   1115
   :timestamptz  1184
   :_timestamptz 1185
   :bytea        17
   :_bytea       1001
   :varchar      1043
   :_varchar     1015
   :oid          26
   :_oid         1028
   :bpchar       1041
   :_bpchar      1014
   :money        790
   :_money       791
   :name         19
   :_name        1003
   :bit          1560
   :_bit         1561
   :void         2278
   :interval     1186
   :_interval    1187
   :char         18 ; "char"
   :_char        1002
   :varbit       1562
   :_varbit      1563
   :uuid         2950
   :_uuid        2951
   :xml          142
   :_xml         143
   :point        600
   :_point       1017
   :box          603
   :_box         1020
   :json         114
   :_json        199
   :jsonb        3802
   :_jsonb       3807
   :refcursor    1790
   :_refcursor   2201
   })


(defprotocol DecodeText
  (-decode-text [t text]))

(defn decode-text [t val]
  (-decode-text t (when val (bs/to-string val))))

(deftype Int2 []
  DecodeText
  (-decode-text [_ text-value]
    (when text-value (Short/parseShort text-value))))

(def int2 (Int2.))

(deftype Int4 []
  DecodeText
  (-decode-text [_ text-value]
    (when text-value (Integer/parseInt text-value))))

(def int4 (Int4.))

(deftype Int8 []
  DecodeText
  (-decode-text [_ text-value]
    (when text-value (Long/parseLong text-value))))

(def int8 (Int8.))

(deftype Text [] ;; do I need to know locale?
  DecodeText
  (-decode-text [_ text]
    text))

(def text (Text.))

(deftype Numeric []
  DecodeText
  (-decode-text [_ text-value]
    (when text-value (BigDecimal. text-value))))

(def numeric (Numeric.))

(deftype Float4 []
  DecodeText
  (-decode-text [_ text-value]
    (when text-value (Float/parseFloat text-value))))

(def float4 (Float4.))

(deftype Float8 []
  DecodeText
  (-decode-text [_ text-value]
    (when text-value (Double/parseDouble text-value))))

(def float8 (Float8.))

(deftype Bool []
  DecodeText
  (-decode-text [_ text-value]
    (when text-value (case text-value "t" true "f" false))))

(def bool (Bool.))

(deftype PgUUID []
  DecodeText
  (-decode-text [_ text-value]
    (when text-value (java.util.UUID/fromString text-value))))

(def pg-uuid (PgUUID.))

(defprotocol EncoderForParameterValue
  (-encoder-for-parameter-value [tr type-info typid value]))

(defn encoder-for-parameter-value [tr type-info typid value]
  (-encoder-for-parameter-value tr type-info typid value))

(defprotocol DecoderFor
  (-decoder-for [tr type-info field-info]))

(defn decoder-for [tr type-info field-info]
  (-decoder-for tr type-info field-info))

(defprotocol TextDecode
  (-text-decode [d field-info s]))

(defn text-decode? [d] (satisfies? TextDecode d))

(defn text-decode [d field-info buf]
  (when buf
    (when-let [s (bs/to-string buf)]
      (-text-decode d field-info s))))

(defprotocol BinaryDecode
  (-binary-decode [d field-info buf]))

(defn binary-decode? [d] (satisfies? BinaryDecode d))

(defn binary-decode [d field-info buf]
  (when buf (-binary-decode d field-info buf)))

;;
(defn encoders-for-params [tr params encoders]
  (if (seq encoders)
    (map #(if %2 %2 (encoder-for-value tr %1)) params encoders)
    (map #(encoder-for-value tr %) params)))

(defn encoders-for-parameter-values [tr type-info typids values encoders]
  (if (seq encoders)
    (doall (map #(if %2 %2 (encoder-for-parameter-value tr type-info %1)) values encoders))
    (doall (map #(encoder-for-parameter-value tr type-info %1 %2) typids values))))

(defn encoder-typid [type-info encoder]
  (if (named-codec? encoder)
    (if-let [oid (apply type-info/oid-by-name type-info (type-name encoder))]
      oid
      (throw (ex-info "no oid for type name"
                      {:type-name (type-name encoder)})))
    (oid encoder)))

(defn encoder-typids [type-info encoders]
  (mapv (partial encoder-typid type-info) encoders))

(defn encode-parameter-values
  [tr type-info typids values query-encoders]
  (let [encoders       (encoders-for-parameter-values tr type-info typids values query-encoders)
        formats        (mapv #(if (binary-encoder? %) :binary :text) encoders)
        typids         (encoder-typids type-info encoders)
        #__              #_(prn {#_:param-formats #_formats
                             #_:param-types   #_typids
                             #_:values        #_values
                             :encoders      (mapv clojure.core/type encoders)})
        encoded-params (doall (mapv #(if (binary-encoder? %1)
                                       (binary-encode %1 %2)
                                       (text-encode %1 %2)) encoders values))]
    #_(prn {:encode-parameter-values values})
    {:param-formats  formats
     :encoded-params encoded-params
     :param-types    typids}))

(defn encode-params
  ([type-info tr params]
   (encode-params type-info tr params []))
  ([type-info tr params encoders]
   (let [encoders       (encoders-for-params tr params encoders)
         formats        (mapv #(if (binary-encoder? %) :binary :text) encoders)
         encoded-params (mapv #(if (binary-encoder? %1)
                                 (binary-encode %1 %2)
                                 (text-encode %1 %2)) encoders params)
         typids         (encoder-typids type-info encoders)]
     {:param-formats  formats
      :encoded-params encoded-params
      :param-types    typids})))
