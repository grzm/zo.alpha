(ns net.zopg.zo.alpha.types.core-registry
  (:require
   [clojure.string :as string]
   [net.zopg.zo.alpha.types :as types]
   [net.zopg.zo.alpha.type-info :as type-info])
  (:import
   (java.nio ByteBuffer)
   (java.time Instant OffsetDateTime)
   (java.time.format DateTimeFormatter)
   (java.util UUID)))

(defn int8 [v]
  (doto (ByteBuffer/allocate 1)
    (.put v)
    (.rewind)))

(defn int16 [v]
  (doto (ByteBuffer/allocate 2)
    (.putShort v)
    (.rewind)))

(defn int32 [v]
  (doto (ByteBuffer/allocate 4)
    (.putInt v)
    (.rewind)))

(defn int64 [^long v]
  (doto (ByteBuffer/allocate 8)
    (.putLong v)
    (.rewind)))

(def pg-epoch-offset 946684800000000)

(defn jud-to-pg-epoch [d]
  (let [i (Instant/ofEpochMilli (.getTime d))
        n (.getNano i)
        m (.toEpochMilli i)
        u (-> n (/ 1000) (rem 1000))
        e (-> (* m 1000) (+ u))]
    (- e pg-epoch-offset)))

(def pg-timestamptz-formatters (mapv #(DateTimeFormatter/ofPattern %)
                                     ["yyyy-MM-dd HH:mm:ssX"
                                      "yyyy-MM-dd HH:mm:ss.SX"
                                      "yyyy-MM-dd HH:mm:ss.SSX"
                                      "yyyy-MM-dd HH:mm:ss.SSSX"
                                      "yyyy-MM-dd HH:mm:ss.SSSSX"
                                      "yyyy-MM-dd HH:mm:ss.SSSSSX"
                                      "yyyy-MM-dd HH:mm:ss.SSSSSSX"]))
(deftype OidCodec []
  types/Encoder
  (-oid [e] (:oid types/oids))
  types/BinaryEncode
  (-binary-encode [e v _]
    (condp = (type v)
      java.lang.Short   (int32 (.intValue v))
      java.lang.Integer (int32 v)
      java.lang.Long    (int32 (.intValue v))
      (throw (ex-info "unsupported" {:value v}))))
  types/TextEncode
  (-text-encode [e v _]
    (condp = (type v)
      java.lang.Short   (str (.intValue v))
      java.lang.Integer (str v)
      java.lang.Long    (str (.intValue v))
      (throw (ex-info "unsupported" {:value v}))))
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (.getInt buf))
  types/TextDecode
  (-text-decode [_ _ s]
    (Integer/parseInt s)))

(def oid-codec (OidCodec.))

(deftype Int2Codec []
  types/Encoder
  (-oid [e] (:int2 types/oids))
  types/BinaryEncode
  (-binary-encode [e v _]
    (condp = (type v)
      java.lang.Short   (int16 v)
      java.lang.Integer (int16 (.shortValue v))
      java.lang.Long    (int16 (.shortValue v))
      (throw (ex-info "unsupported" {:value v}))))
  types/TextEncode
  (-text-encode [e v _]
    (condp = (type v)
      java.lang.Short   (str v)
      java.lang.Integer (str (.shortValue v))
      java.lang.Long    (str (.shortValue v))
      (throw (ex-info "unsupported" {:value v}))))
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (when buf (.getShort buf)))
  types/TextDecode
  (-text-decode [_ _ s]
    (Short/parseShort s)))

(def int2-codec (Int2Codec.))

(deftype Int4Codec []
  types/Encoder
  (-oid [e] (:int4 types/oids))
  types/BinaryEncode
  (-binary-encode [e v _]
    (condp = (type v)
      java.lang.Short   (int32 (.intValue v))
      java.lang.Integer (int32 v)
      java.lang.Long    (int32 (.intValue v))
      (throw (ex-info "unsupported" {:value v}))))
  types/TextEncode
  (-text-encode [e v _]
    (condp = (type v)
      java.lang.Short   (str (.intValue v))
      java.lang.Integer (str v)
      java.lang.Long    (str (.intValue v))
      (throw (ex-info "unsupported" {:value v}))))
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (when buf (.getInt buf)))
  types/TextDecode
  (-text-decode [_ _ s]
    (Integer/parseInt s)))

(def int4-codec (Int4Codec.))

(deftype Int8Codec []
  types/Encoder
  (-oid [e] (:int8 types/oids))
  types/BinaryEncode
  (-binary-encode [e v _]
    (condp = (type v)
      java.lang.Short   (int64 (.longValue v))
      java.lang.Integer (int64 (.longValue v))
      java.lang.Long    (int64 v)
      (throw (ex-info "unsupported" {:value v}))))
  types/TextEncode
  (-text-encode [e v _]
    (condp = (type v)
      java.lang.Short   (str (.LongValue v))
      java.lang.Integer (str (.LongValue v))
      java.lang.Long    (str v)
      (throw (ex-info "unsupported" {:value v}))))
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (when buf (.getLong buf)))
  types/TextDecode
  (-text-decode [_ _ s]
    (Long/parseLong s)))

(def int8-codec (Int8Codec.))

(deftype TextCodec []
  types/Encoder
  (-oid [e] (:text types/oids))
  types/TextEncode
  (-text-encode [_ v _] (str v))
  types/TextDecode
  (-text-decode [_ field-info s] s))

(def text-codec (TextCodec.))

(deftype NumericCodec []
  types/Encoder
  (-oid [e] (:numeric types/oids))
  types/TextEncode
  (-text-encode [_ v _]
    (str v))
  types/TextDecode
  (-text-decode [_ field-info s]
    (BigDecimal. s)))

(def numeric-codec (NumericCodec.))

(deftype Float4Codec []
  types/Encoder
  (-oid [e] (:float4 types/oids))
  types/TextEncode
  (-text-encode [_ v _]
    (str v))
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (.getFloat buf))
  types/TextDecode
  (-text-decode [_ _ s]
    (Float/parseFloat s)))

(def float4-codec (Float4Codec.))

(deftype Float8Codec []
  types/Encoder
  (-oid [e] (:float8 types/oids))
  types/TextEncode
  (-text-encode [_ v _]
    (str v))
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (.getDouble buf))
  types/TextDecode
  (-text-decode [_ _ s]
    (Double/parseDouble s)))

(def float8-codec (Float8Codec.))

(deftype BoolCodec []
  types/Encoder
  (-oid [e] (:bool types/oids))
  types/BinaryEncode
  (-binary-encode [_ v _]
    (int8 (byte (if v 1 0))))
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (not (zero? (.get buf))))
  types/TextDecode
  (-text-decode [_ field-info s]
    (= "t" s)))

(def bool-codec (BoolCodec.))

(deftype UuidCodec []
  types/Encoder
  (-oid [e] (:uuid types/oids))
  types/BinaryEncode
  (-binary-encode [_ v _]
    (doto (ByteBuffer/allocate 16)
      (.putLong (.getMostSignificantBits v))
      (.putLong (.getLeastSignificantBits v))
      (.rewind)))
  types/TextEncode
  (-text-encode [_ v _]
    (str v)))

(def uuid-codec (UuidCodec.))

(deftype TimestamptzCodec []
  types/Encoder
  (-oid [e] (:timestamptz types/oids))
  types/BinaryEncode
  (-binary-encode [_ v _]
    (condp = (type v)
      java.util.Date    (int64 (jud-to-pg-epoch v))
      java.time.Instant (int64 (jud-to-pg-epoch (java.util.Date/from v)))
      java.lang.String  (int64 (jud-to-pg-epoch (java.time.Instant/parse v)))))
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (let [e (+ (.getLong buf) pg-epoch-offset)
          m (/ e 1000)
          u (rem e 1000)]
      (.plusNanos (Instant/ofEpochMilli m) (* u 1000))))
  types/TextDecode
  (-text-decode [_ _ s]
    (let [matches   (re-find #"\.(\d+)" s)
          i         (if matches (count (second matches)) 0)
          formatter (nth pg-timestamptz-formatters i)]
      (.toInstant (OffsetDateTime/parse s formatter)))))

(def timestamptz-codec (TimestamptzCodec.))

(deftype NameCodec []
  types/Encoder
  (-oid [e] (:name types/oids))
  types/TextEncode
  (-text-encode [_ v _] v)
  types/TextDecode
  (-text-decode [_ field-info s] s))

(def name-codec (NameCodec.))

(deftype CharCodec []
  types/Encoder
  (-oid [e] (:char types/oids))
  types/TextEncode
  (-text-encode [_ v _]
    (str v))
  types/TextDecode
  (-text-decode [_ _ s] s))

(def char-codec (CharCodec.))

(deftype ShortEncoder []
  types/Encoder
  (-oid [e] (:int2 types/oids))
  types/BinaryEncode
  (-binary-encode [e v _]
    (condp = (type v)
      java.lang.Short   (int16 v)
      java.lang.Integer (int16 (.shortValue v))
      java.lang.Long    (int16 (.shortValue v))
      (throw (ex-info "unsupported" {:value v}))))
  types/TextEncode
  (-text-encode [e v _]
    (condp = (type v)
      java.lang.Short   (str v)
      java.lang.Integer (str (.shortValue v))
      java.lang.Long    (str (.shortValue v))
      (throw (ex-info "unsupported" {:value v})))))

(def short-encoder (ShortEncoder.))

(deftype IntegerEncoder []
  types/Encoder
  (-oid [e] (:int4 types/oids))
  types/BinaryEncode
  (-binary-encode [e v _]
    (condp = (type v)
      java.lang.Short   (int32 (.intValue v))
      java.lang.Integer (int32 v)
      java.lang.Long    (int32 (.intValue v))
      (throw (ex-info "unsupported" {:value v}))))
  types/TextEncode
  (-text-encode [e v _]
    (condp = (type v)
      java.lang.Short   (str (.intValue v))
      java.lang.Integer (str v)
      java.lang.Long    (str (.intValue v))
      (throw (ex-info "unsupported" {:value v})))))

(def integer-encoder (IntegerEncoder.))

(deftype LongEncoder []
  types/Encoder
  (-oid [e] (:int8 types/oids))
  types/BinaryEncode
  (-binary-encode [e v _]
    (condp = (type v)
      java.lang.Short   (int64 (.longValue v))
      java.lang.Integer (int64 (.longValue v))
      java.lang.Long    (int64 v)
      (throw (ex-info "unsupported" {:value v}))))
  types/TextEncode
  (-text-encode [e v _]
    (condp = (type v)
      java.lang.Short   (str (.LongValue v))
      java.lang.Integer (str (.LongValue v))
      java.lang.Long    (str v)
      (throw (ex-info "unsupported" {:value v})))))

(def long-encoder (LongEncoder.))

(deftype DateEncoder []
  types/Encoder
  (-oid [e] (:timestamptz types/oids))
  types/BinaryEncode
  (-binary-encode [_ v _]
    (int64 (jud-to-pg-epoch v))))

(def date-encoder (DateEncoder.))

(deftype BooleanEncoder []
  types/Encoder
  (-oid [e] (:bool types/oids))
  types/BinaryEncode
  (-binary-encode [_ v _]
    (int8 (byte (if v 1 0)))))

(def boolean-encoder (BooleanEncoder.))

(deftype StringEncoder []
  types/Encoder
  (-oid [e] (:text types/oids))
  types/TextEncode
  (-text-encode [_ v _] v))

(def string-encoder (StringEncoder.))

(deftype UUIDEncoder []
  types/Encoder
  (-oid [e] (:uuid types/oids))
  types/BinaryEncode
  (-binary-encode [_ v _]
    (doto (ByteBuffer/allocate 16)
      (.putLong (.getMostSignificantBits v))
      (.putLong (.getLeastSignificantBits v))
      (.rewind)))
  types/TextEncode
  (-text-encode [_ v _]
    (str v)))

(def uuid-encoder (UUIDEncoder.))

(def default-encoders
  {java.lang.Short   short-encoder
   java.lang.Integer integer-encoder
   java.lang.Long    long-encoder
   java.util.Date    date-encoder
   java.lang.Boolean boolean-encoder
   java.lang.String  string-encoder
   java.util.UUID    uuid-encoder})

(def default-oid-codecs
  {(:oid types/oids)         oid-codec
   (:int2 types/oids)        int2-codec
   (:int4 types/oids)        int4-codec
   (:text types/oids)        text-codec
   (:numeric types/oids)     numeric-codec
   (:float4 types/oids)      float4-codec
   (:float8 types/oids)      float8-codec
   (:bool types/oids)        bool-codec
   (:uuid types/oids)        uuid-codec
   (:timestamptz types/oids) timestamptz-codec
   (:name types/oids)        name-codec
   (:char types/oids)        char-codec})

(deftype DefaultEncoder []
  types/Encoder
  (-oid [_] (:text types/oids))
  types/TextEncode
  (-text-encode [_ v _]
    (str v)))

(def default-encoder (DefaultEncoder.))
;; decoders

(deftype BoolDecoder []
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (not (zero? (.get buf))))
  types/TextDecode
  (-text-decode [_ field-info s]
    (= "t" s)))

(def bool-decoder (BoolDecoder.))

(deftype Int2Decoder []
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (.getShort buf))
  types/TextDecode
  (-text-decode [_ _ s]
    (Short/parseShort s)))

(def int2-decoder (Int2Decoder.))

(deftype Int4Decoder []
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (.getInt buf))
  types/TextDecode
  (-text-decode [_ _ s]
    (Integer/parseInt s)))

(def int4-decoder (Int4Decoder.))

(deftype Int8Decoder []
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (.getLong buf))
  types/TextDecode
  (-text-decode [_ _ s]
    (Long/parseLong s)))

(def int8-decoder (Int8Decoder.))

(deftype Float4Decoder []
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (.getFloat buf))
  types/TextDecode
  (-text-decode [_ _ s]
    (Float/parseFloat s)))

(def float4-decoder (Float4Decoder.))

(deftype Float8Decoder []
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (.getDouble buf))
  types/TextDecode
  (-text-decode [_ _ s]
    (Double/parseDouble s)))

(def float8-decoder (Float8Decoder.))

(deftype NumericDecoder []
  types/TextDecode
  (-text-decode [_ field-info s]
    (BigDecimal. s)))

(def numeric-decoder (NumericDecoder.))

(deftype TextDecoder []
  types/TextDecode
  (-text-decode [_ field-info s] s))

(def text-decoder (TextDecoder.))

(deftype TimestamptzDecoder []
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (let [e (+ (.getLong buf) pg-epoch-offset)
          m (/ e 1000)
          u (rem e 1000)]
      (.plusNanos (Instant/ofEpochMilli m) (* u 1000))))
  types/TextDecode
  (-text-decode [_ _ s]
    (let [matches   (re-find #"\.(\d+)" s)
          i         (if matches (count (second matches)) 0)
          formatter (nth pg-timestamptz-formatters i)]
      (.toInstant (OffsetDateTime/parse s formatter)))))

(def timestamptz-decoder (TimestamptzDecoder.))

(deftype UUIDDecoder []
  types/BinaryDecode
  (-binary-decode [_ field-info buf]
    (UUID. (.getLong buf) (.getLong buf)))
  types/TextDecode
  (-text-decode [_ _ s]
    (java.util.UUID/fromString s)))

(def uuid-decoder (UUIDDecoder.))

(deftype NameDecoder []
  types/TextDecode
  (-text-decode [_ _ s] s))

(def name-decoder (NameDecoder.))

;; unsigned 4-byte int, so this
(deftype OidDecoder []
  types/TextDecode
  (-text-decode [_ _ s] (Integer/parseInt s)))

(def oid-decoder (OidDecoder.))

;; "char" type
(deftype CharDecoder []
  types/TextDecode
  (-text-decode [_ _ s] s))

(def char-decoder (CharDecoder.))

(def default-decoders
  {(:int2 types/oids)        int2-decoder
   (:int4 types/oids)        int4-decoder
   (:int8 types/oids)        int8-decoder
   (:text types/oids)        text-decoder
   (:numeric types/oids)     numeric-decoder
   (:float4 types/oids)      float4-decoder
   (:float8 types/oids)      float8-decoder
   (:bool types/oids)        bool-decoder
   (:uuid types/oids)        uuid-decoder
   (:timestamptz types/oids) timestamptz-decoder
   (:name types/oids)        name-decoder
   (:oid types/oids)         oid-decoder
   (:char types/oids)        char-decoder})

(deftype DefaultDecoder []
  types/BinaryDecode
  (-binary-decode [_ _ buf] buf)
  types/TextDecode
  (-text-decode [_ _ s] s))

(def default-decoder (DefaultDecoder.))

(defn get-decoder [type-info decoders named-decoders typid]
  #_(prn {:get-decoder {:typid typid :named-decoders named-decoders}})
  (if-let [decoder (get decoders typid)]
    decoder
    (when-let [type-name (type-info/type-name-by-oid type-info typid)]
      #_(prn {:type-name type-name})
      (get named-decoders type-name))))

(defn get-encoder
  ;; this only dispatches on typid, not on value
  [type-info encoders named-encoders typid value]
  #_(prn {:get-encoder [typid value] :named-encoders named-encoders})
  (if-let [encoder (get encoders typid)]
    encoder
    ;; XXX and what to do when we have no encoder
    (when-let [type-name (type-info/type-name-by-oid type-info typid)]
      (get named-encoders type-name))))

(defrecord CoreRegistry [encoders decoders named-encoders named-decoders]
  types/DecoderFor
  (-decoder-for [cr type-info {:keys [format typid] :as field-info}]
    #_(prn {:decoder-for    {:format format :typid typid}
            :type-info      (-> (:cache type-info) deref :oids (get ["zo" "my_enum"] [:not-found ["zo" "my_enum"]]))
            :named-decoders named-decoders})
    (let [decoder (get-decoder type-info @decoders @named-decoders typid)]
      (case format
        :binary (partial types/binary-decode
                         (if (and decoder (types/binary-decode? decoder))
                           decoder
                           default-decoder)
                         field-info)
        :text   (partial types/text-decode
                         (if (and decoder (types/text-decode? decoder))
                           decoder
                           default-decoder)
                         field-info))))

  types/EncoderForParameterValue
  (-encoder-for-parameter-value
    [cr type-info typid value]
    (if-let [encoder (get-encoder type-info @encoders @named-encoders typid value)]
      encoder
      default-encoder))

  types/EncoderForValue
  (-encoder-for-value [cr v]
    ;;
    (get @encoders (type v) default-encoder)))

(defn build-named-codec-map [named-decoders]
  (reduce (fn [m named-decoder]
            (assoc m (types/type-name named-decoder) named-decoder))
          {} named-decoders))

(defn core-registry
  ([]
   (core-registry {}))
  ([{:keys [encoders decoders named-decoders named-encoders]
     :or   {encoders default-oid-codecs
            decoders default-decoders}}]
   (->CoreRegistry (atom encoders) (atom decoders)
                   (atom (build-named-codec-map named-encoders))
                   (atom (build-named-codec-map named-decoders)))))
