(ns net.zopg.zo.alpha.protocol.v3.gloss
  (:refer-clojure :exclude [type name])
  (:require
   [byte-streams :as bs]
   [clojure.set :as set]
   [gloss.core :as gloss :refer :all]
   [gloss.io :as io]
   [net.zopg.zo.alpha.protocol.v3 :as v3]))

(declare encode)
(declare decode)
(declare backend-message)

(defrecord GlossCodec []
  v3/V3
  (-encode [codec msg]
    (encode msg))
  (-decode [codec msg]
    (decode msg))
  (-decode-stream [codec stream]
    (io/decode-stream stream backend-message)))

(def len-prefix (prefix :int32 #(- % 4) #(+ % 4)))
(def field-prefix (prefix :int32 #(if (zero? %) -1 %) #(if (= -1 %) 0 %)))
(def null-byte (byte 0))
(def utf-8-string (string :utf-8 :delimiters [null-byte]))

;;;; Frontend encoding

(defn frontend-message-frame
  [frame]
  (ordered-map
    :type :byte
    :body (finite-frame
            len-prefix
            frame)))

(defn replace-frontend-type [msg]
  (update msg :type (partial get v3/frontend-type-codes)))

(defmulti encode first)

(defcodec frontend-empty-message
  (ordered-map
    :type :byte
    :body (finite-block
            len-prefix))
  replace-frontend-type
  identity)

(defmethod encode :default
  [[type body :as msg]]
  (io/encode frontend-empty-message {:type type :body body}))

(defcodec bind
  (frontend-message-frame
    (ordered-map
      :portal-name utf-8-string
      :statement-name utf-8-string
      :parameter-formats (repeated
                           :int16
                           :prefix :int16)
      :parameters (repeated
                    (finite-block
                      field-prefix)
                    :prefix :int16)
      :result-formats (repeated
                        :int16
                        :prefix :int16)))
  replace-frontend-type
  identity)

(defmethod encode :bind
  [[type body]]
  (let [bind-body (-> body
                      (update :result-formats (partial mapv v3/format-code))
                      (update :parameter-formats (partial mapv v3/format-code)))]
    (io/encode bind {:type type :body bind-body})))

(defcodec cancel-request
  (finite-frame
    len-prefix
    [:int32 :int32 :int32]))

(defmethod encode :cancel-request
  [[type {:keys [cancel-request-code process-id secret-key]}]]
  (io/encode cancel-request [cancel-request-code process-id secret-key]))

(defcodec close
  (frontend-message-frame
    (ordered-map
      :code :byte
      :name utf-8-string))
  replace-frontend-type
  identity)

(defmethod encode :close
  [[type {:keys [portal-name statement-name]}]]
  (let [[close-type name] (if statement-name
                            [:statement statement-name]
                            [:portal portal-name])
        code              (get v3/close-codes close-type)]
    (io/encode close {:type type :body {:code code
                                        :name name}})))

(defcodec fe-copy-data
  (ordered-map
    :type :byte
    :body (finite-block
            len-prefix))
  replace-frontend-type
  identity)

(defmethod encode :copy-data
  [[type {:keys [data]}]]
  (io/encode fe-copy-data {:type type :body data}))

(defcodec describe
  (frontend-message-frame
    (ordered-map
      :code :byte
      :name utf-8-string))
  replace-frontend-type
  identity)

(defmethod encode :describe
  [[type {:keys [portal-name statement-name]}]]
  (let [[describe-type name] (if statement-name
                               [:statement statement-name]
                               [:portal portal-name])
        code                 (get v3/describe-codes describe-type)]
    (io/encode describe {:type type :body {:code code
                                           :name name}})))

(defcodec execute
  (frontend-message-frame
    (ordered-map
      :portal-name utf-8-string
      :max-rows :int32))
  replace-frontend-type
  identity)

(defmethod encode :execute
  [[type body]]
  (let [execute-body (merge {:max-rows 0} body)]
    (io/encode execute {:type type
                        :body execute-body})))

(defcodec function-call
  (frontend-message-frame
    (ordered-map
      :procid :int32
      :argument-formats (repeated :int16 :prefix :int16)
      :argument-values (repeated
                         (finite-block
                           field-prefix)
                         :prefix :int16)
      :result-format :int16))
  replace-frontend-type
  identity)

(defmethod encode :function-call
  [[type body]]
  (let [function-call-body
        (-> body
            (update :argument-formats (partial mapv v3/format-code))
            (update :result-format v3/format-code))]
    (io/encode function-call {:type type
                              :body function-call-body})))

(defcodec parse
  (frontend-message-frame
    (ordered-map
      :statement-name utf-8-string
      :query-string utf-8-string
      :parameter-types (repeated :int32 :prefix :int16)))
  replace-frontend-type
  identity)

(defmethod encode :parse
  [[type body]]
  (io/encode parse {:type type :body body}))

(defcodec query
  (ordered-map
    :type :byte
    :body (finite-frame
            len-prefix
            utf-8-string))
  replace-frontend-type
  identity)

(defmethod encode :query
  [[type {:keys [query-string]}]]
  (io/encode query {:type type :body query-string}))

(defcodec password-message
  (ordered-map
    :type :byte
    :body (finite-frame len-prefix utf-8-string))
  replace-frontend-type
  identity)

(defmethod encode :password-message
  [[type {:keys [password]}]]
  (io/encode password-message {:type type :body password}))

(defcodec ssl-request
  (finite-frame
    len-prefix
    :int32))

(defmethod encode :ssl-request
  [[_ {:keys [ssl-request-code]}]]
  (io/encode ssl-request ssl-request-code))

(defcodec startup-message
  (finite-frame
    len-prefix
    (delimited-frame
      [null-byte]
      [:int32
       (repeated
         [utf-8-string utf-8-string]
         :prefix :none)])))

(defmethod encode :startup-message
  [[_ body]]
  (let [protocol-version-number (:protocol-version-number body)
        params                  (dissoc body :protocol-version-number)
        msg-body                [protocol-version-number
                                 (v3/startup-message-params params)]]
    (io/encode startup-message msg-body)))

;;;; Backend messages

(def message-frame
  (ordered-map
    :type :byte
    :body (finite-block len-prefix)))

(defn replace-backend-type [msg]
  (update msg :type #(get v3/backend-types (char %))))

(defcodec backend-message
  message-frame
  identity
  replace-backend-type)

(defmulti decode :type)

(defmethod decode :default
  [{:keys [type body] :as msg}]
  (if body
    [type body]
    [type]))

(defcodec string-response utf-8-string)

(defcodec auth-short :int32)

(defn auth-request-type [body]
  (let [auth-code (io/decode auth-short body false)]
    (get v3/authentication-request-types auth-code)))

(defcodec auth-md5
  [:int32 (finite-block 4)])

(defn auth-md5-password-salt
  [body]
  (->> body (io/decode auth-md5) second io/contiguous))

(defn auth-gss-data
  [body]
  (let [data-len (- (byte-count body) 4) ;; skip int32
        fr       (compile-frame [:int32 (finite-block data-len)])]
    (->> body (io/decode fr) second io/contiguous)))

(defmethod decode :authentication
  [{:keys [body type] :as msg}]
  (if-let [req-type (auth-request-type body)]
    (let [auth-body
          (merge {:type req-type}
                 (case req-type
                   :md5-password
                   {:salt (auth-md5-password-salt body)}

                   :gss-continue
                   {:data (auth-gss-data body)}

                   nil))]
      [type auth-body])

    (throw (ex-info "unhandled authentication message type"
                    {:msg msg}))))

(defcodec backend-key-data
  (ordered-map :process-id :int32 :secret-key :int32))

(defmethod decode :backend-key-data
  [{:keys [body type] :as msg}]
  [type (io/decode backend-key-data body)])

(defmethod decode :command-complete
  [{:keys [body type] :as msg}]
  [type {:command-tag (io/decode string-response body)}])

(defn be-copy-data [body]
  (let [len (byte-count body)
        fr  (compile-frame (finite-block len))]
    (->> body (io/decode fr) (io/contiguous))))

(defmethod decode :copy-data
  [{:keys [body type] :as msg}]
  [type {:data (be-copy-data body)}])

(defcodec copy-response-body
  (ordered-map
    :format :byte
    :field-formats (repeated :int16
                             :prefix :int16)))

(defn copy-reponse
  [{:keys [body type]}]
  (let [decoded (io/decode copy-response-body body)
        cr-body (-> decoded
                    (update :format v3/format)
                    (update :field-formats (partial mapv v3/format)))]
    [type cr-body]))

(defmethod decode :copy-in-response
  [msg]
  (copy-reponse msg))

(defmethod decode :copy-out-response
  [msg]
  (copy-reponse msg))

(defmethod decode :copy-both-response
  [msg]
  (copy-reponse msg))

(defcodec data-row
  (repeated
    (finite-block field-prefix)
    :prefix :int16))

(defmethod decode :data-row
  [{:keys [body type] :as msg}]
  (let [decoded (mapv io/contiguous (io/decode data-row body))]
    [type {:fields decoded}]))

(defn tag-error-field-type [data]
  (let [ftype (:field-type data)]
    (assoc data :field-type (get v3/error-fields ftype ftype))))

(defn decode-fields-body [body]
  (->> body
       drop-last
       (partition-by zero?)
       (remove #(-> % first zero?))
       (map (fn [[t & m]]
              (let [type-code (char t)
                    buf       (bs/convert (byte-array m) java.nio.ByteBuffer)]
                [type-code buf])))
       (into {})))

(defcodec byte-body
  (repeated :byte :prefix :none))

(defn decode-error-response-body
  [body]
  (let [decoded (io/decode byte-body body)]
    (as-> decoded b
      (decode-fields-body b)
      (set/rename-keys b v3/error-fields)
      (reduce-kv (fn [m k v] (assoc m k (apply str (map char (.array v))))) {} b))))

(defmethod decode :error-response
  [{:keys [body type] :as msg}]
  (let [decoded (decode-error-response-body body)]
    [type decoded]))

(defcodec function-call-response
  (finite-block
    field-prefix))

;; likely not handling NULL correctly here
(defmethod decode :function-call-response
  [{:keys [body type] :as msg}]
  [type {:value (->> body (io/decode function-call-response) io/contiguous)}])

(defmethod decode :notice-response
  [{:keys [body type]}]
  (let [decoded (decode-error-response-body body)]
    [type decoded]))

(defcodec notification-response
  (ordered-map
    :process-id :int32
    :channel-name utf-8-string
    :payload utf-8-string))

(defmethod decode :notification-response
  [{:keys [body type]}]
  [type (io/decode notification-response body)])

(defcodec parameter-description
  (repeated :int32
            :prefix :int16))

(defmethod decode :parameter-description
  [{:keys [body type]}]
  [type {:types (io/decode parameter-description body)}])

(defcodec parameter-status
  [utf-8-string utf-8-string])

(defmethod decode :parameter-status
  [{:keys [body type] :as msg}]
  (if-let [[param-name param-value] (io/decode parameter-status body)]
    [type {:name param-name :value param-value}]
    (throw (ex-info "unhandled parameter-status message"
                    {:msg msg}))))

(defcodec ready-for-query :byte)

(defmethod decode :ready-for-query
  [{:keys [body type] :as msg}]
  (let [status (->> body
                    (io/decode ready-for-query)
                    char
                    v3/ready-for-query-status)]
    [type {:status status}]))

(defcodec row-description
  (repeated
    (ordered-map
      :name utf-8-string
      :attrelid :int32
      :attnum :int16
      :typid :int32
      :typlen :int16
      :typmod :int32
      :format :int16)
    :prefix :int16))

(defmethod decode :row-description
  [{:keys [body type] :as msg}]
  [type {:fields (->> body
                      (io/decode row-description)
                      (map (fn [f]
                             (update f :format v3/format))))}])
