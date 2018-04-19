(ns net.zopg.zo.type-info-cache-test
  (:require
   [clojure.core.async :refer [<!!]]
   [clojure.test :refer [are deftest is]]
   [net.zopg.zo.async.alpha :as azo]
   [net.zopg.zo.alpha.types :as types]
   [net.zopg.zo.alpha.type-info :as type-info]
   [net.zopg.zo.test.config :refer [conn-params]]))

#_(deftest type-info-cache
  (let [client (azo/client conn-params)]
    (with-open [sess (<!! (azo/connect client))]
      (let [tic                  (type-info/type-info-cache sess)
            typtype-col          {:nspname "pg_catalog" :relname "pg_type" :attname "typtype"}
            typtype-relid-attnum [1247 6]
            pg-type-typ-oid      71
            pg-type-name         ["pg_catalog" "pg_type"]

            pg-attribute-typ-oid 75
            pg-attribute-name    ["pg_catalog" "pg_attribute"]
            ]
        (is (= typtype-col (apply type-info/column-info tic typtype-relid-attnum)))
        ;; get same answer on second lookup
        ;; XXX This is not confirming we're doing a cached lookup
        (is (= typtype-col (apply type-info/column-info tic typtype-relid-attnum)))

        (is (= pg-type-typ-oid (apply type-info/oid-by-name tic pg-type-name)))
        ;; get same answer on second lookup
        ;; XXX This is not confirming we're doing a cached lookup
        (is (= pg-type-typ-oid (apply type-info/oid-by-name tic pg-type-name)))

        #_(is (= pg-attribute-name (type-info/type-name-by-oid tic pg-attribute-typ-oid)))
        ;; get same answer on second lookup
        ;; XXX This is not confirming we're doing a cached lookup
        #_(is (= pg-attribute-name (type-info/type-name-by-oid tic pg-attribute-typ-oid)))
        ))))
