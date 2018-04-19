(ns net.zopg.zo.async.alpha
  (:require
   [clojure.core.async :as a]
   [clojure.spec.alpha :as s]
   [net.zopg.zo.client.alpha :as client]
   [net.zopg.zo.client.impl :as client-impl]
   [net.zopg.zo.session.alpha :as session]
   [net.zopg.zo.spec.alpha :as zo.spec]))

(defn explain-config-error [cfg]
  (let [data (s/explain-data :net.zopg.zo.spec/connection-parameters cfg)]
    {:error        :invalid-connection-params
     :explain-data data}))

(defn client
  ([cfg]
   (client cfg {}))
  ([cfg opts]
   (let [conn-params (merge {:host "localhost" :port 5432}
                            cfg)]
     (if (s/valid? :net.zopg.zo.spec/connection-parameters conn-params)
       (client-impl/client conn-params opts)
       (explain-config-error conn-params)))))

(defn connect
  [client]
  (client/connect client))

(defn q
  [sess query]
  (session/q sess query))

(defn prepare
  [sess label query]
  (session/prepare sess label query))

(defn execute
  [sess label args]
  (session/execute sess label args))

(defn close [sess]
  (session/close sess))

(defn ready? [sess]
  (session/ready? sess))

(defn cancel [sess]
  (session/cancel sess))

(defn listen [sess]
  (session/listen sess))

(comment

  (require '[clojure.core.async :as a :refer [<!!]])
  (def conn-params {:host     "localhost"
                    :port     5496
                    :database "zo_ex"
                    :user     "grzm"})

  (def c (client conn-params))
  (:config c)
  (def sess (<!! (connect c)))
  sess
  (def select-1 [:val "SELECT 'foo'::TEXT, 1"])
  (def res (<!! (q sess select-1)))
  (def res (<!! (a/into [] (q sess select-1))))
  res

  sess
  (def text-vals [:ary "VALUES ('foo'::TEXT), ('bar'), ('baz')"])
  (def res (a/into [] (q sess text-vals)))
  (<!! res)
  (:state @(:state sess))
  (close sess)

  )
