(ns net.zopg.zo.alpha
  (:require
   [clojure.core.async :as a :refer [<!!]]
   [net.zopg.zo.async.alpha :as async]
   [net.zopg.zo.session.alpha :as session]))

(defn client
  ([cfg]
   (client cfg {}))
  ([cfg opts]
   (let [{:keys [error] :as c} (async/client cfg opts)]
     (if error
       (throw (ex-info "invalid config"
                       {:config cfg
                        :error  error}))
       c))))

(defn connect
  [client]
  (<!! (async/connect client)))

(defn q
  [sess query]
  (let [res (<!! (async/q sess query))]
    (if (contains? res :result)
      (:result res)
      res)))

(defn cancel
  [sess]
  (session/cancel sess))

(defn ready? [sess]
  (session/ready? sess))

(defn listen
  [sess]
  (session/listen sess))

(defn errors
  [sess]
  (session/errors sess))

(defn close
  [sess]
  (session/close sess))
