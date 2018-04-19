(ns net.zopg.zo.test.session
  (:refer-clojure :exclude [send])
  (:require
   [clojure.core.async :as async]
   [clojure.test :refer [is]]
   [net.zopg.zo.alpha.types.core-registry :as core-registry]
   [net.zopg.zo.session.alpha :as session]
   [net.zopg.zo.session.impl :as session.impl]
   [net.zopg.zo.session.io :as io]
   [net.zopg.zo.io.vector-io :as vector-io]))

(alias 'sess 'net.zopg.zo.session)

(defn session-state [sess]
  @(:state sess))

(defn session-state-kw [sess]
  (:state (session-state sess)))

(defmacro with-timeout [ms & body]
  `(let [future# (future ~@body)]
     (try
       (.get future# ~ms java.util.concurrent.TimeUnit/MILLISECONDS)
       (catch java.util.concurrent.TimeoutException ex#
         (do
           (future-cancel future#)
           nil)))))

(defn wait-for
  ([sess pred]
   (wait-for sess pred 1000 1))
  ([sess pred timeout-ms sleep-ms]
   (with-timeout timeout-ms
     (while (not (pred (session-state sess)))
       (Thread/sleep sleep-ms)))
   sess))

(defn wait
  ([sess initial-state]
   (wait sess initial-state 1000 1))
  ([sess initial-state timeout-ms sleep-ms]
   (with-timeout timeout-ms
     (while (and (= initial-state (session-state sess)))
       (Thread/sleep sleep-ms)))
   sess))

(defn session-with-state
  ([init-state]
   (session-with-state init-state (fn [in] (vector-io/vector-io in))))
  ([init-state init-io]
   (let [sess (session.impl/session :dummy-client {} {:init-io init-io})]
     (session/start sess)
     (reset! (:state sess) init-state)
     sess)))

(defn assert-state-kw
  [sess state-kw]
  (is (= state-kw (session-state-kw sess)))
  sess)

(defn assert-state [sess f]
  (is (true? (f (session-state sess)))))

(defn assert-state=
  [sess expected f]
  (is (= expected (f (session-state sess))))
  sess)

(defn receive
  [{:keys [io] :as sess} msg]
  (let [state (session-state sess)]
    (io/receive io msg)
    (wait sess state)
    sess))

(defn receive-async
  [{:keys [io] :as sess} msg]
  (io/receive io msg)
  sess)

(defn send
  [sess msg]
  (session.impl/send sess msg)
  sess)

(defn prn-receive
  "Print incoming server messages to *stdout*"
  [sess]
  (let [c (async/chan)]
    (async/tap (:in sess) c)
    (async/go-loop []
      (when-let [msg (async/<! c)]
        (println "RECEIVED" (pr-str msg))
        (recur)))))

(defn prn-error
  "Print incoming server messages to *stdout*"
  [sess]
  (let [c (session/errors sess)]
    (async/go-loop []
      (when-let [msg (async/<! c)]
        (println "ERROR" (pr-str msg))
        (recur)))))
