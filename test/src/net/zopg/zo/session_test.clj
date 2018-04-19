(ns net.zopg.zo.session-test
  (:refer-clojure :exclude [send])
  (:require
   [byte-streams :refer [to-byte-buffer]]
   [clojure.core.async :as async :refer [chan <!!]]
   [clojure.test :as test :refer [is deftest are]]
   [com.grzm.tespresso.alpha :as tespresso]
   [net.zopg.zo.async.alpha :as azo]
   [net.zopg.zo.session.alpha :as session]
   [net.zopg.zo.alpha.query :as query]
   [net.zopg.zo.session.impl :as session.impl]
   [net.zopg.zo.test.session :refer [session-with-state
                                     session-state
                                     session-state-kw
                                     assert-state-kw
                                     assert-state=
                                     receive
                                     send
                                     wait
                                     with-timeout]]
   [net.zopg.zo.io.vector-io :as vio :refer [vector-io]]
   [net.zopg.zo.alpha.types.core-registry :as core-registry]
   [net.zopg.zo.alpha.query :as query]))

(alias 'sess 'net.zopg.zo.session)

(def spec {:user "sess"})

(deftest receive-transitions
  (are [params]
      (let [[args expected-state] params
            [init-state msg] args
            sess (session-with-state init-state)]
        (is (= expected-state (session.impl/receive sess msg))))

    [[{:state ::sess/sent-startup-message,
       :spec  spec}
      [:authentication {:type :ok}]]
     {:state ::sess/authenticated
      :spec  spec}]

    [[{:state ::sess/authenticated,
       :spec  spec}
      [:parameter-status {:name  "some-name"
                          :value "some-value"}]]
     {:state      ::sess/authenticated
      :spec       spec
      :parameters {"some-name" "some-value"}}]

    [[{:state ::sess/authenticated,
       :spec  spec}
      [:backend-key-data {:process-id 42
                          :secret-key 48}]]
     {:state            ::sess/authenticated
      :spec             spec
      :backend-key-data {:process-id 42
                         :secret-key 48}}]

    [[{:state ::sess/authenticated,
       :spec  spec}
      [:ready-for-query {:status :idle}]]
     {:state          ::sess/ready-for-query
      :spec           spec
      :backend-status :idle}]))

(deftest receive-row-description
  (let [out        (async/promise-chan)
        init-state {:state   ::sess/sent-query
                    :queries [{:result {:out out}}]}
        msg-body   {:fields [{:name     "now"
                              :attrelid 0
                              :attnum   0
                              :typid    25
                              :tylpen   -1
                              :typmod   -1
                              :format   :text}]}
        msg        [:row-description msg-body]]

    (with-open [sess (session-with-state init-state)]
      (let [state (session.impl/receive sess msg)]
        (is (= ::sess/have-row-description (:state state)))
        (is (fn? (get-in state [:queries 0 :result :decode-fields])))))))

(deftest receive-error
  (let [[args expected]
        [[{:state ::sess/sent-startup-message
           :spec  spec}
          [:error-response {:severity "FATAL"}]]
         {:data-keys {:message [:error-response {:severity "FATAL"}]
                      :state   {:spec  spec
                                :state ::sess/failed-startup
                                :error {:severity "FATAL"}}}}]
        {:keys [data-keys error-message]} expected
        [init-state msg]                  args]
    (with-open [sess (session-with-state init-state)]
      (-> sess
          (receive msg)
          (assert-state-kw ::sess/failed-startup)))))

(deftest startup
  (let [vio (vector-io)]
    (with-open [sess (session.impl/session :dummy-client
                                           {:user "some-user"}
                                           {:init-io (fn [in] (vector-io in))})]
      (-> sess
          (assert-state-kw ::sess/init)
          session/start
          (assert-state-kw ::sess/sent-startup-message)
          (receive [:authentication {:type :ok}])
          (assert-state-kw ::sess/authenticated)
          (receive [:backend-key-data {:process-id 54 :secret-key 32}])
          (assert-state-kw ::sess/authenticated)
          (assert-state= {:process-id 54 :secret-key 32} :backend-key-data)
          (receive [:parameter-status {:name "some-name" :value "some-value"}])
          (receive [:parameter-status {:name "some-name2" :value "some-value2"}])
          (assert-state= {"some-name"  "some-value"
                          "some-name2" "some-value2"} :parameters)
          (receive [:ready-for-query {:status :idle}])
          (assert-state-kw ::sess/ready-for-query)
          ))))

(defn q
  "Runs the given query, piping the output to the given channel."
  [sess res & query]
  (let [state (session-state sess)]
    (async/pipe (apply azo/q sess query) res)
    (wait sess state)
    sess))

(defn prepare
  [sess label query]
  (azo/prepare sess label query)
  sess)

(defn execute [sess res label args]
  (let [state (session-state sess)]
    (async/pipe (azo/execute sess label args) res)
    (wait sess state)
    sess))

(defn unwrap-result [res-chan]
  (if-let [r (with-timeout 10
               (let [res (<!! res-chan)]
                 (if-let [result (:result res)]
                   result
                   res)))]
    r
    :unwrap-result-timeout-failure))

(deftest handle-invalid-query
  (let [init-state {:state ::sess/ready-for-query}
        res        (async/promise-chan)]
    (with-open [sess (session-with-state init-state)]
      (-> sess
          (q res "Some invalid query"))
      (is (seq (:explain-data (unwrap-result res)))))
    (async/close! res)))

(deftest simple-query
  (let [init-state {:state   ::sess/ready-for-query
                    :queries []}
        res        (async/promise-chan)]
    (with-open [sess (session-with-state init-state)]
      (-> sess
          (assert-state-kw ::sess/ready-for-query)
          (q res [:val "SELECT 1 AS foo"])
          (assert-state-kw ::sess/sent-query)
          (assert-state= {:result-fn first
                          :row-fn    first}
                         #(-> %
                              :queries
                              last
                              :parsed-query
                              (select-keys [:result-fn :row-fn])))
          (receive [:row-description {:fields [{:name     "foo"
                                                :attrelid 0
                                                :attnum   0
                                                :typid    23
                                                :typlen   -1
                                                :typmod   -1
                                                :format   :text}]}])
          (assert-state-kw ::sess/have-row-description)
          (receive [:data-row {:fields [(to-byte-buffer "1")]}])
          (assert-state-kw ::sess/receiving-data)
          (receive [:command-complete {:command-tag "SELECT 1"}])
          (assert-state-kw ::sess/command-complete)
          (receive [:ready-for-query {:status :idle}])
          (assert-state-kw ::sess/ready-for-query)
          (assert-state= [] :queries))
      (is (= 1 (unwrap-result res)))
      (async/close! res))))

(deftest extended-query
  (let [init-state {:state ::sess/ready-for-query
                    :queries []}
        res        (async/promise-chan)]
    (with-open [sess (session-with-state init-state)]
      (-> sess
          (assert-state-kw ::sess/ready-for-query)
          (q res [:val "SELECT $1::INT AS foo" [1]])
          (assert-state= {:result-fn first
                          :row-fn    first}
                         #(-> %
                              :queries
                              last
                              :parsed-query
                              (select-keys [:result-fn :row-fn])))
          (receive [:row-description {:fields [{:name     "foo"
                                                :attrelid 0
                                                :attnum   0
                                                :typid    23
                                                :typlen   -1
                                                :typmod   -1
                                                :format   :text}]}])
          (assert-state-kw ::sess/have-row-description)
          (receive [:data-row {:fields [(to-byte-buffer "1")]}])
          (assert-state-kw ::sess/receiving-data)
          (receive [:command-complete {:command-tag "SELECT 1"}])
          (assert-state-kw ::sess/command-complete)
          (receive [:ready-for-query {:status :idle}])
          (assert-state-kw ::sess/ready-for-query)
          (assert-state= [] :queries))
      (is (= 1 (unwrap-result res)))
      (async/close! res))))

#_(deftest prepare-and-execute
    (let [init-state {:state ::sess/ready-for-query}
          res        (chan)]
      (with-open [sess (session-with-state init-state)]
        (-> sess
            (assert-state-kw ::sess/ready-for-query)
            (prepare "foo" [:val "SELECT $1 AS foo"])
            (execute res "foo" [1])
            (assert-state= :val #(get-in % [:result :res]))
            (receive [:row-description {:fields [{:name     "foo"
                                                  :attrelid 0
                                                  :attnum   0
                                                  :typid    23
                                                  :typlen   -1
                                                  :typmod   -1
                                                  :format   :text}]}])
            (assert-state-kw ::sess/have-row-description)
            (receive [:data-row {:fields [(to-byte-buffer "1")]}])
            (assert-state-kw ::sess/receiving-data)
            (receive [:command-complete {:command-tag "SELECT 1"}])
            (assert-state-kw ::sess/command-complete)
            (receive [:ready-for-query {:status :idle}])
            (assert-state-kw ::sess/ready-for-query)
            (assert-state= nil :result))
        (is (= 1 (unwrap res))))))
