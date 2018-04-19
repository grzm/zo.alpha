(ns net.zopg.zo.session.alpha
  (:require
   [clojure.core.async :as async]))

(in-ns 'net.zopg.zo.session)
(in-ns 'net.zopg.zo.session.alpha)

(defprotocol Session
  (-start [this])
  (-close [this])
  (-q [this query])
  (-prepare [this label query])
  (-errors [this c])
  (-execute [this label args])
  (-listen [this c])
  (-ready? [this])
  (-cancel [this]))

(defn start [sess]
  (-start sess))

(defn q [sess query]
  (-q sess query))

(defn prepare [sess label query]
  (-prepare sess label query))

(defn execute [sess label args]
  (-execute sess label args))

(defn close [sess]
  (-close sess))

(defn listen
  ([sess]
   (listen sess (async/chan)))
  ([sess c]
   (-listen sess c)
   c))

(defn errors
  ([sess]
   (errors sess (async/chan)))
  ([sess c]
   (-errors sess c)
   c))

(defn ready? [sess]
  (-ready? sess))

(defn cancel [sess]
  (-cancel sess))

