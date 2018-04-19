(ns net.zopg.zo.session.io
  (:refer-clojure :exclude [send]))

(defprotocol IO
  (-send [io msg])
  (-receive [io msg])
  (-new [io]))

(defn send [io msg]
  (-send io msg))

(defn receive [io msg]
  (-receive io msg))

(defn new [io]
  (-new io))
