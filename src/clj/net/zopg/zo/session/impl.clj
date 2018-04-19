(ns net.zopg.zo.session.impl
  (:refer-clojure :exclude [send])
  (:require
   [clojure.core.async :as async :refer [>! chan go]]
   [net.zopg.zo.alpha.protocol.v3 :as v3]
   [net.zopg.zo.alpha.protocol.v3.type-handler :as type-handler]
   [net.zopg.zo.alpha.query :as query]
   [net.zopg.zo.alpha.types :as types]
   [net.zopg.zo.alpha.type-info :as type-info]
   [net.zopg.zo.alpha.types.core-registry :as core-registry]
   [net.zopg.zo.alpha.util :refer [with-timeout wait-for]]
   [net.zopg.zo.anomalies :as anom]
   [net.zopg.zo.session.alpha :as session]
   [net.zopg.zo.session.io :as io]
   [net.zopg.zo.session.io.tcp :as tcp]
   [net.zopg.zo.session.util :as util])
  (:import
   (java.io Closeable)))

(alias 'sess 'net.zopg.zo.session)

(defprotocol ColumnNaming
  (-from-column-name [this s]))

;; https://clojure.org/reference/reader#_literals
;; valid keywords
;; - is like a symbol
;;   - must begin with a non-numeric character
;;   - can contain alphanumeric characters, #{"*" "+" "!" "-" "_" "'" "?"}
;;   - / has special meaning (for namespacing)
;;   - cannot begin or end with "." (reserved by Clojure)
;; - must begin with a colon (handled by `keyword` function)
;; - cannot contain "."
;; - cannot name classes (not applicable)
;; - can be namespaced (not applicable)
;; should I be this strict?

(defrecord DefaultColumnNaming []
  ColumnNaming
  (-from-column-name [this s]
    (-> (.replaceAll s "_" "-")
        keyword)))

(defn from-column-name [naming ^String s]
  (-from-column-name naming s))



(def zo-application-name "zo")

(declare receive-loop)
(declare receive)
(declare throw-illegal-state-exception)
(declare put-error)

(defn pending-query? [state]
  (seq (:queries state)))

(defn peek-query [state]
  (peek (:queries state)))

(defn pop-query [state]
  (update state :queries pop))

(defn conj-query [state query]
  (update state :queries conj query))

(defn update-query [state f]
  (let [q-count (count (:queries state))]
    (if (pos? q-count)
      (update-in state [:queries (dec q-count)] f)
      state)))

(defn get-query-out
  "Returns out for pending query. Returns nil if no out"
  [state]
  (-> state :queries peek (get-in [:result :out])))

(defn close-query-out! [query]
  (async/close! (get-in query [:result :out])))

(defn close-query-outs! [state]
  (let [queries (:queries state)]
    (dorun (map close-query-out! (:queries state)))))

(declare describe-query!)

(defn make-col-names [col-name-fn fields]
  (into [] (comp (map :name) col-name-fn) fields))

(defn create-row-handler*
  [{:keys [state type-registry column-naming] :as sess}]
  ;; Don't need to uniquify-names if returning as vectors
  #_(prn {:create-row-handler* "creating"})
  (let [col-name-fn      (comp util/uniquify-names
                               (map #(from-column-name column-naming %)))
        query            (peek-query @state)
        {:keys [fields]} (:row-description query)
        decoders         (get-in query [:parsed-query :decoders])
        decode-fields    (type-handler/decode-row-fn type-registry (:type-info @state) fields decoders)]
    (swap! state (fn [s]
                   (-> s
                       (update-query (fn [q]
                                       (-> q
                                           (assoc-in [:result :decode-fields] decode-fields)
                                           (assoc-in [:result :fields] fields)
                                           (assoc-in [:result :column-names] (make-col-names col-name-fn fields))))))))))

(defn get-parameter-type-info*
  [{:keys [state] :as sess}]
  (let [{:keys [parameter-description] :as query} (peek-query @state)
        type-info                                 (:type-info @state)]
    #_(prn {:get-parameter-type-info (:types parameter-description)})
    ;; force fetch of types
    (dorun (map #(type-info/type-name-by-oid type-info %) (:types parameter-description)))))

(defn- query-has-row-description? [state]
  (let [query (peek-query @state)]
    (:row-description query)))

(defn- query-has-decoder? [state]
  (let [query (peek-query @state)]
    (or (get-in query [:result :decode-fields])
        (= :no-data (:row-description query)))))

(defn simple-query!
  [{:keys [state io] :as sess} {:keys [query-string] :as parsed}]
  ;; need to describe here, too
  (describe-query! sess parsed)
  ;; XXX wait until we get the description to do lookups
  ;; XXX should cache query and description
  ;; figure decoders now
  (wait-for #(query-has-row-description? state) 1000 10)
  (create-row-handler* sess)
  (wait-for #(query-has-decoder? state) 1000 10)
  #_(prn {:simple-query! "gonna send"})
  (io/send io [:query {:query-string query-string}])
  #_(prn {:simple-query! "sent!"})
  (swap! state (fn [s]
                 (-> s
                     (assoc :state ::sess/sent-query)
                     (update-query #(assoc %
                                           :query query-string
                                           :parsed-query parsed)))))
  #_(prn {:simple-query! "swapped!"}))

;; perhaps a use for a macro instead of a multimethod for performance?
(defn send-dispatch [_ [type _body]] type)
(defmulti send send-dispatch)

(defmethod send :default
  [{:keys [io state]} msg]
  (io/send io msg))

(defmethod send :parse
  [{:keys [io state]} msg]
  (io/send io msg)
  (swap! state assoc :state ::sess/sent-parse))

(defmethod send :bind
  [{:keys [io state]} msg]
  (io/send io msg)
  (swap! state assoc :state ::sess/sent-bind))

(defmethod send :execute
  [{:keys [io state]} msg]
  (io/send io msg)
  (swap! state assoc :state ::sess/sent-execute))

(defmethod send :flush
  [{:keys [io]} msg]
  (io/send io msg))

(def unnamed-statement "")
(def unnamed-portal "")

(defn query-with-params!
  [{:keys [state io type-registry] :as sess}
   {:keys [query-string params encoders] :as parsed}]
  ;; how to get parameter types?
  (let [statement-name  unnamed-statement
        portal-name     unnamed-portal
        ;; XXX Need to check if we have unknown types
        parameter-count (count params)]
    (describe-query! sess parsed)
    ;; XXX wait until we get the description to do lookups
    ;; XXX should cache query and description
    ;; figure decoders now
    (wait-for #(query-has-row-description? state) 1000 10)
    (create-row-handler* sess)
    (get-parameter-type-info* sess)
    ;; Now do the real work
    (let [{:keys [parameter-description] :as query} (peek-query @state)
          typids                                    (:types parameter-description)
          {:keys [encoded-params param-formats param-types]}
          (types/encode-parameter-values type-registry (:type-info @state) typids params encoders)]
      #_(prn {:query-string query-string
              :params       params
              :param-types  param-types})
      (send sess [:parse {:statement-name  statement-name
                          :query-string    query-string
                          :parameter-types param-types}])
      (send sess [:bind {:portal-name       portal-name
                         :statement-name    statement-name
                         :parameter-formats param-formats
                         :parameters        encoded-params
                         :result-formats    [:text]}])
      (send sess [:describe {:portal-name portal-name}])
      (send sess [:execute {:portal-name portal-name}])
      (send sess [:sync])
      (swap! state #(-> (assoc % :state ::sess/sent-query-with-params)
                        (update-query (fn [q]
                                        (assoc q
                                               :query        query-string
                                               :parsed-query parsed))))))))

(defn describe-query!
  [{:keys [state] :as sess} {:keys [query-string] :as parsed}]
  ;; Might be a parameterized query, but we're going to let the backend
  ;; parser fill in the blanks for us
  (let [statement-name  unnamed-statement
        params          []
        parameter-count (count params)]
    (send sess [:parse {:statement-name  statement-name
                        :query-string    query-string
                        :parameter-types (vec (repeat parameter-count 0))}])
    (send sess [:describe {:statement-name statement-name}])
    (send sess [:sync])
    (swap! state assoc :state ::sess/sent-describe-statement)))


(defn get-statement [state label]
  (get-in @state [:statements label]))

(defn conj-query-result-out [state out parsed]
  (conj-query state {:result  {:out  out
                               :rows []
                               :res  (:res parsed)}
                     :parsed* parsed}))

(defrecord Session [client state io in error control spec type-registry column-naming]
  session/Session
  (-start [{:keys [state io control] :as this}]
    (let [msg (v3/startup-message (:spec @state))]
      (io/send io msg))
    (swap! state assoc :state ::sess/sent-startup-message
           :type-info (type-info/type-info-cache this))
    (receive-loop this)
    this)

  (-q [{:keys [state io] :as this} query]
    (let [state-kw (:state @state)
          out      (async/promise-chan)]

      #_(prn {:-q state-kw :query query})
      (if (not= ::sess/ready-for-query state-kw)
        (do
          (async/put! out {::anom/category ::anom/busy
                           ::anom/message  "protocol violation"
                           ::sess/state    @state
                           :query          query})
          (async/close! out))
        (let [parsed (query/parse query)]
          (if (anom/anomaly? parsed)
            (do
              (async/put! out parsed)
              (async/close! out))
            (let [params (:params parsed)]
              (swap! state conj-query-result-out out parsed)
              #_(prn {:grr query :queries (-> @state :queries)})
              (if (seq params)
                (query-with-params! this parsed)
                (simple-query! this parsed))))
          ;; if not conformed, should return error as well
          out))))

  (-prepare [{:keys [state] :as this} label query]
    (let [parsed (query/parse query)] ;; shouldn't have args
      (swap! state assoc-in [:statements label] {:query  query
                                                 :parsed parsed})))

  (-execute [{:keys [state io type-registry] :as this} label parameters]
    (if-let [{:keys [query parsed]} (get-statement state label)]
      (let [query-string          (:query-string parsed)
            statement-name        label
            portal-name           label
            out                   (async/promise-chan)
            {:keys [encoded-params
                    param-formats
                    param-types]} (types/encode-params
                                    (:type-info @state) type-registry parameters)]
        (swap! state conj-query-result-out out parsed)
        ;; send parse only if haven't yet for this session
        (send this [:parse {:statement-name  statement-name
                            :query-string    query-string
                            :parameter-types param-types}])
        (send this [:bind {:portal-name       portal-name
                           :statement-name    statement-name
                           :parameter-formats param-formats
                           :parameters        encoded-params
                           :result-formats    [:text]}])
        (send this [:describe {:portal-name portal-name}])
        (send this [:execute {:portal-name portal-name}])
        (swap! state #(-> (assoc % :state ::sess/sent-execute)
                          (conj-query {:query        query-string
                                       :parsed-query parsed})))
        out)

      ;; throw if query for label doesn't exist?
      ))

  (-listen [{:keys [state] :as this} l-chan]
    (let [{:keys [listener]} (swap! state
                                    (fn [{:keys [listener] :as s}]
                                      (if listener
                                        s
                                        (let [listener-in (chan)
                                              listener    (async/mult listener-in)]
                                          (assoc s
                                                 :listener listener
                                                 :listener-in listener-in)))))]
      (async/tap listener l-chan)))

  (-errors [{:keys [state] :as this} e-chan]
    (let [{:keys [error] :as s} (swap! state
                                       (fn [{:keys [error-in] :as s}]
                                         (if error-in
                                           s
                                           (let [error-in (chan)
                                                 error    (async/mult error-in)]
                                             (assoc s
                                                    :error-in error-in
                                                    :error error)))))]
      (async/tap error e-chan)))

  (-close [this]
    (.close this))

  (-ready? [{:keys [state]}]
    (= ::sess/ready-for-query (:state @state)))

  (-cancel [{:keys [state client] :as this}]
    ;; need to open a new session and send cancel message on it
    ;; what should this return?
    (let [msg                 (v3/cancel-request (:backend-key-data @state))
          {:keys [host port]} (:config client)
          ;; I don't need to create this: can I have an
          ;; even more bare-bones tcp connection?
          in                  (chan)
          io                  (tcp/tcp-io host port in)]
      (io/send io msg)
      (async/close! in)))

  Closeable
  (close [{:keys [state io] :as this}]
    (close-query-outs! @state)
    (when-let [listener-in (:listener-in @state)]
      (async/close! listener-in))
    (when-let [error-in (:error-in @state)]
      (async/close! error-in))
    (io/send io [:terminate])
    ;; should I be cleaning up the tcp channels here as well?
    (swap! state assoc :state ::sess/shutdown)))

(def initial-state {:level   0, :state ::sess/init
                    :queries []})

(defn session
  "Create a session instance"
  ([client conn-params]
   (session client conn-params {}))
  ([client
    {:keys [host port] :as conn-params}
    {:keys [column-naming init-io type-registry]
     :or   {column-naming (->DefaultColumnNaming)
            init-io       (fn [in] (tcp/tcp-io host port in))
            type-registry (core-registry/core-registry)}}]
   (let [tcp-in  (chan)
         io      (init-io tcp-in)
         in      (async/mult tcp-in)
         control (chan)
         spec    (merge
                   {:application-name zo-application-name}
                   (dissoc conn-params :host :port))
         state   (atom (assoc initial-state :spec spec))]
     (map->Session {:client        client
                    :state         state
                    :io            io
                    :in            in
                    :control       control
                    :spec          spec
                    :column-naming column-naming
                    :type-registry type-registry}))))


;; State Loop

(defn- receive-loop
  [{:keys [state control in] :as sess}]
  (let [tcp-in (chan)]
    (async/tap in tcp-in)
    (async/go-loop []
      (when-let [[msg port] (async/alts! [tcp-in control])]
        #_(prn {:sess-state (:state @state) :msg msg :queries (:queries @state)})
        (condp = port
          control
          (when-not (= :stop msg)
            (recur))

          tcp-in
          (do
            (receive sess msg)
            (recur)))))))

(defn print-state [state]
  (-> state
      (dissoc :error-in
              :error
              :type-info
              :backend-key-data
              :spec
              :parameters
              :level)
      (update :queries (fn [qs] (map #(update-in % [:result :out] boolean) qs)))))

(defn- throw-unimplemented-message
  [{:keys [state] :as sess} msg]
  (put-error sess {::anom/category ::anom/unsupported
                   ::anom/message  "unimplemented message"
                   ::sess/state    (print-state @state)
                   :message        msg} ))

(defn- throw-error-message
  [{:keys [state] :as sess} msg]
  (put-error sess {::anom/category ::anom/fault
                   ::anom/message  "received error response"
                   ::sess/state    (print-state @state)
                   :message        msg}))

(defn- throw-illegal-state-exception
  ([sess msg]
   (throw-illegal-state-exception sess msg "protocol violation"))
  ([{:keys [state] :as sess} msg error-message]
   (put-error sess {::anom/category ::anom/fault
                    ::anom/message  "protocol message"
                    ::sess/state    (print-state @state)
                    :message        msg})))

(declare receive*)

(defn dispatch-receive
  [{:keys [state]} _]
  (:state @state))

(defmulti receive dispatch-receive)
(defmethod receive :default
  [sess msg]
  (throw-unimplemented-message sess msg))

(defmethod receive ::sess/sent-startup-message
  [{:keys [state] :as sess} [type body :as msg]]
  (case type
    :authentication
    (let [{authentication-type :type} body]
      (if (= authentication-type :ok)
        (swap! state assoc :state ::sess/authenticated)
        ;; XXX where should this error appear?
        ;; user is expecting to get a sesson, not an anomaly
        (throw (ex-info "unimplemented authentication method"
                        {:state @state
                         :msg   msg}))))
    :error-response
    (do
      (swap! state assoc
             :state ::sess/failed-startup
             :error body)
      (throw-error-message sess msg))

    (throw-illegal-state-exception sess msg)))


(defmethod receive ::sess/authenticated
  [{:keys [state] :as sess} [type body :as msg]]
  (case type
    :backend-key-data
    ;; This message provides secret-key data that the frontend must save if it wants to be able to issue cancel requests later. The frontend should not respond to this message, but should continue listening for a ReadyForQuery message.
    (swap! state assoc :backend-key-data body)

    :parameter-status
    ;; This message informs the frontend about the current (initial) setting of backend parameters, such as client_encoding or DateStyle. The frontend can ignore this message, or record the settings for its future use; see Section 51.2.6 for more details. The frontend should not respond to this message, but should continue listening for a ReadyForQuery message.
    (receive* [:* type] sess msg)

    :ready-for-query ;; Start-up is completed. The frontend can now issue commands.
    (receive* [:* type] sess msg)

    :error-response ;; Start-up failed. The connection is closed after sending this message.
    (throw-error-message sess msg)

    :notice-response ;; A warning message has been issued. The frontend should display the message but continue listening for ReadyForQuery or ErrorResponse.
    (throw-unimplemented-message sess msg)

    (throw-unimplemented-message sess msg)))

(defn receive-notification-response
  [{:keys [state] :as sess} [_type body :as msg]]
  (when-let [listener-in (:listener-in @state)]
    (async/put! listener-in body)))

(defn put-error
  [{:keys [state] :as sess} msg]
  (when-let [error-in (:error-in @state)]
    (async/put! error-in msg)))

(defn return-error-response
  [{:keys [state]} [_type body :as msg]]
  ;; we're assuming have out
  (let [out (get-query-out @state)]
    (async/put! out {:error body})
    (async/close! out)
    (swap! state #(-> %
                      ;; should this be ready-for-query?
                      ;; Let people know that last thing was an error?
                      (assoc :state ::sess/command-error)))))

(defn handle-error-response
  [{:keys [state] :as sess} [_type body :as msg]]
  (let [kw (:state @state)]
    (case kw
      ::sess/sent-query
      (do
        (put-error sess msg)
        (return-error-response sess msg))

      ::sess/have-row-description
      (return-error-response sess msg)

      ;; else set error in state. At this point, let's print and throw
      (do
        (throw-error-message sess msg)))))

(defn handle-notice-response
  [{:keys [error-in]} msg]
  (async/put! error-in msg))

(defmethod receive ::sess/ready-for-query
  [sess [type body :as msg]]
  (case type
    ;; :command-complete
    ;; ;; An SQL command completed normally.
    ;; ;; CommandComplete marks the end of processing one SQL command, not the whole string.
    ;; (throw-unimplemented-message sess msg)

    ;; :copy-in-response
    ;; ;; The backend is ready to copy data from the frontend to a table
    ;; (throw-unimplemented-message sess msg)

    ;; :copy-out-response
    ;; ;; The backend is ready to copy data from a table to the frontend
    ;; (throw-unimplemented-message sess msg)

    ;; :row-description
    ;; ;; Indicates that rows are about to be returned in response to a SELECT, FETCH, etc query. The contents of this message describe the column layout of the rows. This will be followed by a DataRow message for each row being returned to the frontend.
    ;; (throw-unimplemented-message sess msg)

    ;; :data-row
    ;; ;; One of the set of rows returned by a SELECT, FETCH, etc query.
    ;; (throw-unimplemented-message sess msg)

    ;; :empty-query-response
    ;; ;; An empty query string was recognized.
    ;; ;; If a completely empty (no contents other than whitespace) query string is received, the response is EmptyQueryResponse followed by ReadyForQuery.
    ;; (throw-unimplemented-message sess msg)

    :error-response
    ;; An error has occurred.
    ;; In the event of an error, ErrorResponse is issued followed by ReadyForQuery. All further processing of the query string is aborted by ErrorResponse (even if more queries remained in it). Note that this might occur partway through the sequence of messages generated by an individual query.
    (handle-error-response sess msg)

    :notice-response
    ;; A warning message has been issued in relation to the query. Notices are in addition to other responses, i.e., the backend will continue processing the command.
    (throw-unimplemented-message sess msg)

    :notification-response
    (receive-notification-response sess msg)

    :ready-for-query
    ;; Processing of the query string is complete. A separate message is sent to indicate this because the query string might contain multiple SQL commands. (CommandComplete marks the end of processing one SQL command, not the whole string.) ReadyForQuery will always be sent, whether processing terminates successfully or with an error.
    (throw-unimplemented-message sess msg)

    (throw-illegal-state-exception sess msg)))

(defn create-row-handler
  [{:keys [state type-registry column-naming] :as sess} [_ {:keys [fields] :as msg}]]
  ;; Don't need to uniquify-names if returning as vectors
  (let [col-name-fn   (comp util/uniquify-names
                            (map #(from-column-name column-naming %)))
        query         (peek-query @state)
        decoders      (get-in query [:parsed-query :decoders])
        #__           #_ (prn {:create-row-handler {:decoders decoders}})
        decode-fields (type-handler/decode-row-fn type-registry (:type-info @state) fields decoders)]
    (swap! state (fn [s]
                   (-> s
                       (update-query (fn [q]
                                       (-> q
                                           (assoc-in [:result :decode-fields] decode-fields)
                                           (assoc-in [:result :fields] fields)
                                           (assoc-in [:result :column-names] (make-col-names col-name-fn fields)))))
                       (assoc :state ::sess/have-row-description))))))

(defn add-parameter-description [state pd]
  (update-query state (fn [q] (assoc q :parameter-description pd))))

(defn add-row-description [state rd]
  (update-query state (fn [q] (assoc q :row-description rd))))

(defmethod receive ::sess/sent-describe-statement
  [{:keys [state] :as sess} [type body :as msg]]
  (case type
    :parse-complete
    nil

    :no-data
    (swap! state add-row-description :no-data)

    :parameter-description
    ;; update query with parameter description info
    (swap! state add-parameter-description body)

    :row-description
    (swap! state add-row-description body)

    :ready-for-query
    (receive* [:* type] sess msg)

    :error-response
    (handle-error-response sess msg)

    :notice-response
    (put-error sess msg)

    (throw-illegal-state-exception sess msg)))

(defmethod receive ::sess/sent-query
  [sess [type body :as msg]]
  (case type
    :command-complete
    (receive* [:* :command-complete] sess msg)

    :row-description
    (create-row-handler sess msg)

    :error-response
    (handle-error-response sess msg)

    :notice-response
    (put-error sess msg)

    (throw-illegal-state-exception sess msg)))

(defmethod receive ::sess/sent-query-with-params
  [{:keys [state] :as sess} [type body :as msg]]
  (case type
    :command-complete
    (receive* [:* :command-complete] sess msg)

    :parse-complete
    (swap! state update-query  (fn [q]
                                 (update-in q [:query-state] conj {:parsed? true})))

    :bind-complete
    (swap! state update-query (fn [q]
                                (update-in q [:query-state] conj {:bound? true})))

    :data-row
    (receive* [:* :data-row] sess msg)

    :row-description
    (create-row-handler sess msg)

    :error-response
    (handle-error-response sess msg)

    :notice-response
    (handle-notice-response sess msg)

    ;; default
    (throw-illegal-state-exception sess msg)))

(defmethod receive ::sess/have-row-description
  [{:keys [state] :as sess} [type body :as msg]]
  (case type
    :data-row
    (do
      (receive* [:* :data-row] sess msg)
      (swap! state assoc :state ::sess/receiving-data))

    :command-complete
    (receive* [:* :command-complete] sess msg)

    :error-response
    (handle-error-response sess msg)

    :notice-response
    (throw-unimplemented-message sess msg)

    (throw-illegal-state-exception sess msg)))

(defmethod receive ::sess/receiving-data
  [sess [type body :as msg]]
  (case type
    :data-row
    (receive* [:* :data-row] sess msg)

    :command-complete
    (receive* [:* :command-complete] sess msg)

    :error-response
    (handle-error-response sess msg)

    :notice-response
    (throw-unimplemented-message sess msg)

    (throw-illegal-state-exception sess msg)))

(defmethod receive ::sess/command-complete
  [sess [type body :as msg]]
  (case type
    :ready-for-query
    (receive* [:* type] sess msg)

    :error-response
    (throw-unimplemented-message sess msg)

    :notice-response
    (throw-unimplemented-message sess msg)

    (throw-illegal-state-exception sess msg)))

(defmethod receive ::sess/command-error
  [sess [type body :as msg]]
  (case type
    :ready-for-query
    (receive* [:* type] sess msg)

    :error-response
    (throw-unimplemented-message sess msg)

    :notice-response
    (throw-unimplemented-message sess msg)

    (throw-illegal-state-exception sess msg)))

(defmethod receive ::sess/sent-parse
  [{:keys [state] :as sess} [type body :as msg]]
  (case type
    :parse-complete
    (swap! state assoc :state ::sess/parse-complete)

    :parameter-description
    ;; update query with parameter description info
    (swap! state add-parameter-description body)

    ;; default
    (return-error-response sess msg)))

(defmethod receive ::sess/parse-complete
  [{:keys [state] :as sess} [type body :as msg]]
  (case type
    :parameter-description
    ;; update query with parameter description info
    (swap! state add-parameter-description body)

    :row-description
    (swap! state add-row-description body)

    :bind-complete
    (swap! state assoc :state ::sess/bind-complete)

    :ready-for-query
    (receive* [:* type] sess msg)

    ;; default
    (return-error-response sess msg)))

(defmethod receive ::sess/sent-bind
  [{:keys [state] :as sess} [type body :as msg]]
  (case type
    :bind-complete
    (swap! state assoc :state ::sess/bind-complete)

    (put-error sess {::anom/category ::anom/unsupported
                     ::sess/state    (print-state @state)
                     ::sess/msg      msg})))


(defmethod receive ::sess/sent-execute
  [{:keys [state] :as sess} [type body :as msg]]
  (case type
    :row-description
    (do
      #_(prn [::sess/sent-execute type (peek-query @state)])
      ;; create row handler if don't already have decode-fields
      )
    :parse-complete
    (do
      #_(prn [::sess/sent-execute type (peek-query @state)]))

    :bind-complete
    (do #_(prn [::sess/sent-execute type (peek-query @state)]))

    :data-row
    (do
      (receive* [:* :data-row] sess msg)
      (swap! state assoc :state ::sess/receiving-data))

    ;; what am I going to do if I don't have a data handler yet?
    (put-error sess {::anom/category ::anom/unsupported
                     ::sess/state    (print-state @state)
                     ::sess/msg      msg})))

(defn dispatch-receive*
  [[state-kw msg-type] _ _]
  [state-kw msg-type])

(defmulti receive* dispatch-receive*)

(defmethod receive* :default
  [_ state msg]
  (throw-unimplemented-message state msg))

(defmethod receive*  [:* :parameter-status]
  [_ {:keys [state] :as sess} [_ {param-name :name param-value :value}]]
  (swap! state assoc-in [:parameters param-name] param-value))

(defmethod receive* [:* :ready-for-query]
  [_ {:keys [state] :as sess} [_ {:keys [status]}]]
  #_(prn {:ready-for-query (:state @state) :queries (:queries @state)})
  (swap! state assoc
         :state ::sess/ready-for-query
         :backend-status status)
  #_(prn {:ready-for-query-state-after (:state @state)}))

(defmethod receive* [:* :command-complete]
  [_ {:keys [state] :as sess} [type body :as msg]]
  (let [query                         (peek-query @state)
        {:keys [result parsed-query]} query
        {:keys [out errors rows]}     result]
    (if out
      (do
        ;; this should be its own function
        (if (seq errors)
          ;; this should be ::anom/fault if errors are on callee
          ;; or ::anom/incorrect if errors are on caller
          (async/put! out {::anom/category ::anom/fault
                           ::anom/message  "Errors while decoding"
                           ::sess/state    (print-state @state)
                           :rows           rows})
          (async/put! out {:result ((:result-fn parsed-query) rows)}))
        (async/close! out))
      (throw-illegal-state-exception sess msg "no out"))
    (swap! state  #(-> %
                       pop-query
                       (assoc :last-command body
                              :state ::sess/command-complete)))))

(defmethod receive* [:* :data-row]
  [_ {:keys [state] :as sess} [type body :as msg]]
  (let [query                           (peek-query @state)
        {:keys [row-fn as-vector?]}     (get query :parsed-query)
        column-names                    (get-in query [:result :column-names])
        {:keys [decode-fields out res]} (:result query)
        decoded                         (decode-fields (:fields body))]
    ;; XXX need to handle error when decoded is NULL (don't have decoder)
    ;; We can probably push :res down further.
    ;; Let's decide that when we figure out how to do general
    ;; transformations on returning rows
    ;; XXX Need to handle decode error
    (swap! state update-query #(update-in % [:result :rows]
                                          conj (if as-vector?
                                                 (row-fn decoded)
                                                 (zipmap column-names (row-fn decoded)))))))
