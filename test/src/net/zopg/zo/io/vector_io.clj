(ns net.zopg.zo.io.vector-io
  (:require
   [clojure.core.async :as async]
   [net.zopg.zo.session.io :as io])
  (:import
   (java.io Closeable)))

(defrecord VectorIO [msgs in]
  Closeable
  (close [_])

  io/IO
  (-send [io msg]
    (swap! msgs conj msg))
  (-receive [io msg]
    (async/put! in msg)))

(defn sent [{:keys [msgs]}]
  @msgs)

(defn vector-io
  ([]
   (->VectorIO (atom []) (async/chan)))
  ([in]
   (->VectorIO (atom []) in)))
