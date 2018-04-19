(ns net.zopg.zo.test.config
  (:require
   [com.grzm.pique.alpha.env :as env]
   [environ.core :as environ]))

(def environ-params
  {:database (environ/env :test-database-name)
   :user (environ/env :test-database-user)
   :port (Integer/parseInt (environ/env :test-database-port))})

(def conn-params
  (let [params (env/params)]
    (merge {:host "localhost"}
           (if (seq params)
             params
             environ-params))))
