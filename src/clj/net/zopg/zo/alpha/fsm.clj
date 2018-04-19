(ns net.zopg.zo.alpha.fsm
  (:require
   [automat.core :as automat]
   [automat.viz :as viz]))

(def start-up
  [(automat/* :parameter-status)
   :backend-key-data
   :ready-for-query])

(def simple-query
  [:query
   :row-description
   (automat/or
     [:row-description
      (automat/+ :data-row)
      :command-complete
      :ready-for-query]
     [:empty-query-response
      :command-complete
      :ready-for-query])])

(def extended-query
  [:parse
   (automat/? :flush)
   :bind
   (automat/? :flush)
   :execute])

(def terminate [:terminate])

(comment
  (viz/view start-up)
  (viz/view simple-query)
  (viz/view [start-up (automat/or
                        (automat/* simple-query)
                        (automat/* extended-query)) terminate])
  (viz/view extended-query)

  )
