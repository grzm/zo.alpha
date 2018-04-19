(ns net.zopg.zo.client.impl
  (:require
   [clojure.core.async :refer [>! go promise-chan]]
   [net.zopg.zo.alpha.util :refer [with-timeout]]
   [net.zopg.zo.client.alpha :as client]
   [net.zopg.zo.session.alpha :as session]
   [net.zopg.zo.session.impl :as session.impl]
   [net.zopg.zo.anomalies :as anom]))

(alias 'sess 'net.zopg.zo.session)

(defn wait-for
  [pred timeout-ms sleep-ms]
  (with-timeout timeout-ms
    (while (not (pred))
      (Thread/sleep sleep-ms))))

(defrecord Client [config session-opts]
  client/Client
  (-connect [this]
    (let [c (promise-chan)
          timeout-ms 1000 ;; XXX this should be configurable
          polling-interval-ms 1]
      (go
        (let [sess (session.impl/session this config session-opts)]
          (session/start sess)
          (wait-for #(= ::sess/ready-for-query (:state @(:state sess))) timeout-ms polling-interval-ms)
          (if (= ::sess/ready-for-query (:state @(:state sess)))
            (>! c sess)
            (do
              (.close sess)
              (>! c {::anom/category ::incorrect
                     ::anom/message "connection failed"
                     ::sess/state @(:state sess)})))))
      c)))

(defn client
  ([config]
   (client config {}))
  ([config session-opts]
   (->Client config session-opts)))
