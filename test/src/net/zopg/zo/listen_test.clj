(ns net.zopg.zo.listen-test
  (:require
   [clojure.core.async :as async :refer [<!!]]
   [clojure.test :refer [deftest is]]
   [net.zopg.zo.test.session :as test.session
    :refer [session-with-state
            receive-async
            assert-state-kw
            assert-state=]]
   [net.zopg.zo.session.alpha :as session]))

(alias 'sess 'net.zopg.zo.session)

(deftest receive-notifications-while-listening
  (let [initial-state {:state ::sess/ready-for-query}]
    (with-open [sess (session-with-state initial-state)]
      (let [listen-chan (session/listen sess)
            msg         {:process-id 24, :channel-name "foo", :payload "my-payload"}]
        (-> sess
            (assert-state-kw ::sess/ready-for-query)
            (receive-async [:notification-response msg])
            (assert-state-kw ::sess/ready-for-query))
        (is (= msg (<!! listen-chan)))))))

(comment
  (def sess (session-with-state {:state ::sess/ready-for-query}))
  sess
  (keys sess)
  (def in (async/chan))
  (async/tap (:mult-in sess) in)
  (defn prn-receive [in]
    (async/go-loop []
      (when-let [m (async/<! in)]
        (prn {:RECEIVED m})
        (recur))))
  (prn-receive in)
  (test.session/receive sess [:notification-response
                              {:process-id 24 :channel-name "foo" :payload "my-payload2"}])


  (def listener (session/listen sess))
  (-> sess :state deref :listener)
  listener
  (prn-receive listener)
  (keys sess)
  (.close sess)

  )
