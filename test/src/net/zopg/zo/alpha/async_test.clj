(ns net.zopg.zo.alpha.async-test
  (:require
   [clojure.core.async :as async :refer [<! >! <!! chan go-loop put!]]
   [clojure.test :as test :refer [are deftest is]]))

(defn state-loop [msgs in stop]
  (go-loop []
    (when-let [[msg port] (async/alts! [in stop])]
      (when-not (and (= stop port) (= :stop msg))
        (swap! msgs conj msg)
        (recur)))))

#_(deftest test-state-loop
  (let [msgs (atom [])
        in   (chan)
        stop (chan)]
    (state-loop msgs in stop)
    (put! in :first)
    (= [:first] @msgs)
    (put! stop stop)))

(defn open-data
  ([state-ref]
   (open-data state-ref (async/chan)))
  ([state-ref c]
   (swap! state-ref assoc :out c)
   c))

(defn process-in
  [state-ref [msg-type body]]
  (condp = msg-type
    :data
    (let [out (:out @state-ref)]
      (async/put! out body))

    :data-complete
    (let [out (:out @state-ref)]
      (async/close! out)
      (swap! state-ref dissoc :out))))

(defn recv-loop
  [state-ref in stop]
  (go-loop []
    (when-let [[msg port] (async/alts! [in stop])]
      (when-not (and (= stop port) (= :stop msg))
        (process-in state-ref msg)
        (recur)))))

(deftest test-async-sequence
  (let [state-ref (atom {})
        stop (async/chan)
        in (async/chan)]
    (recv-loop state-ref in stop)
    (try
      (let [out (open-data state-ref)
            res (async/into [] out)]
        (async/put! in [:data 1])
        (async/put! in [:data 2])
        (async/put! in [:data 3])
        (async/put! in [:data-complete :done])
        (is (= [1 2 3] (async/<!! res))))
      (finally
        (async/put! stop :stop)))))

