(ns net.zopg.zo.anomalies
  (:require
   [clojure.spec.alpha :as s]))

;; inspired by https://github.com/cognitect-labs/anomalies

(s/def ::category #{::busy ::fault ::incorrect})
(s/def ::message string?)
(s/def ::anomaly (s/keys :req [::category]
                         :opt [::message]))

(defn anomaly?
  "Returns true if the given object is an anomaly and false otherwise."
  [x]
  ;; Includes map? and ::category? checks to short circuit more expensive
  ;; s/valid? check
  ;; Should probably check whether this is actually faster
  (and (map? x) (::category x) (s/valid? ::anomaly x)))
