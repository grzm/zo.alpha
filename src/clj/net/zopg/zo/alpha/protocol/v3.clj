(ns net.zopg.zo.alpha.protocol.v3
  (:refer-clojure :exclude [type format])
  (:require
   [clojure.set :as set]))

;; I'd like to be able to switch this out with something that
;; operates on the byte stream directly if necessary for performance.
;; What does that look like?

(defprotocol V3
  (-encode [codec msg])
  (-decode [codec msg])
  (-decode-stream [codec stream]))

(defn encode [codec msg]
  (-encode codec msg))

(defn decode [codec msg]
  (-decode codec msg))

(defn decode-stream [codec stream]
  (-decode-stream codec stream))

(def describe-codes
  {:statement \S
   :portal    \P})

(def close-codes
  {:statement \S
   :portal    \P})

(def frontend-types
  {\B :bind
   \C :close
   \d :copy-data
   \c :copy-done
   \f :copy-fail
   \D :describe
   \E :execute
   \H :flush
   \F :function-call
   \P :parse
   \p :password-message
   \Q :query
   \S :sync
   \X :terminate})

(def frontend-type-codes (set/map-invert frontend-types))

(def backend-types
  {\R :authentication
   \K :backend-key-data
   \2 :bind-complete
   \F :cancel
   \3 :close-complete
   \C :command-complete
   \d :copy-data
   \c :copy-done
   \G :copy-in-response
   \H :copy-out-response
   \W :copy-both-response
   \D :data-row
   \I :empty-query-response
   \E :error-response
   \V :function-call-response
   \n :no-data
   \N :notice-response
   \A :notification-response
   \t :parameter-description
   \S :parameter-status
   \1 :parse-complete
   \s :portal-suspended
   \Z :ready-for-query
   \T :row-description})

(def backend-type-codes (set/map-invert backend-types))

(defonce
  ^{:doc "The cancel request code. The value is chosen to contain 1234 in the most significant 16 bits, and 5678 in the least significant 16 bits. (To avoid confusion, this code must not be the same as any protocol version number.)"}
  cancel-request-code 80877102)

(defonce
  ^{:doc "The protocol version number. The most significant 16 bits are the major version number (3 for the protocol described here). The least significant 16 bits are the minor version number (0 for the protocol described here)"}
  protocol-version-number
  196608)

(defonce
  ^{:doc "The SSL request code. The value is chosen to contain 1234 in the most significant 16 bits, and 5679 in the least significant 16 bits. (To avoid confusion, this code must not be the same as any protocol version number."}
  ssl-request-code
  80877103)

(def error-fields
  {\S :localized-severity
   \V :severity
   \C :code
   \M :message
   \D :detail
   \H :hint
   \P :position
   \p :internal-position
   \q :internal-query
   \W :where
   \t :table-name
   \c :column-name
   \d :data-type-name
   \n :constraint-name
   \F :file
   \L :line
   \R :routine})

(def server-parameters
  {"server_version"    :server-version  ;; pseudo
   "server_encoding"   :server-encoding ;; pseudo
   "integer_datetimes" :integer-datetimes?});; pseudo

(def client-parameters
  {"application_name"            :application-name
   "client_encoding"             :client-encoding
   "is_superuser"                :superuser? ;; isn't this dependent on role?
   "session_authorization"       :session-authorization
   "DateStyle"                   :date-style
   "IntervalStyle"               :interval-style
   "TimeZone"                    :time-zone
   "standard_conforming_strings" :standard-conforming-strings?})

(def parameter-status-names
  (merge server-parameters
         client-parameters))

(def ready-for-query-status
  {\I :idle
   \T :in-transaction
   \E :failed-transaction})

(def formats
  {0 :text
   1 :binary})

(def format-codes (set/map-invert formats))

(defn format-code [format]
  (get format-codes format))

(defn format [code]
  (get formats code))

;; frontend

(def startup-param-names
  {:user               "user"
   :database           "database"
   :application-name   "application_name"
   :integer-datetimes? "integer_datetimes"
   :connect-timeout    "connect_timeout"
   :client-encoding    "client_encoding"
   :superuser?         "is_superuser"})

(defn stringify-booleans [params]
  (reduce-kv (fn [m k v]
               (assoc m k (if (boolean? v)
                            (str v)
                            v))) {} params))

(defn startup-message-params [params]
  (-> params
      (set/rename-keys startup-param-names)
      stringify-booleans
      set))

(defn startup-message-body [params]
  (assoc params
         :protocol-version-number protocol-version-number))

(defn startup-message [params]
  [:startup-message (startup-message-body params)])

(defn cancel-request
  [backend-key-data]
  [:cancel-request (assoc backend-key-data
                          :cancel-request-code cancel-request-code)])

;; backend

(def authentication-request-types
  {0 :ok
   2 :kerberos-v5
   3 :cleartext-password
   5 :md5-password
   6 :scm-credential
   7 :gss
   8 :gss-continue
   9 :sspi})

(defprotocol TextParameter
  (-value [v]))

(extend-protocol TextParameter
  Object
  (-value [v]
    (str v))
  nil
  (-value [_]
    nil))

(defn text-value [v]
  (-value v))
