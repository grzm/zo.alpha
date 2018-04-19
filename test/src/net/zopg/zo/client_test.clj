(ns net.zopg.zo.client-test
  (:require
   [clojure.test :refer [deftest is]]
   [com.grzm.tespresso.alpha :as tespresso]
   [net.zopg.zo.alpha :as zo]))

(deftest throws-with-ill-formed-config
  (let [conn-params {:user "grzm", :port 5499, :database "some-dbname", :host "localhost"}]
    (is (nil? (:error (zo/client conn-params))))
    #_(is (com.grzm.tespresso/thrown-with-data?
          (tespresso/ex-data-select= {:error :invalid-connection-params})
          (zo/client (dissoc conn-params :host))) "missing host")
    (is (com.grzm.tespresso/thrown-with-data?
          (tespresso/ex-data-select= {:error :invalid-connection-params})
          (zo/client (dissoc conn-params :database))) "missing database")
    (is (com.grzm.tespresso/thrown-with-data?
          (tespresso/ex-data-select= {:error :invalid-connection-params})
          (zo/client (dissoc conn-params :user))) "missing user")
    #_(is (com.grzm.tespresso/thrown-with-data?
          (tespresso/ex-data-select= {:error :invalid-connection-params})
          (zo/client (dissoc conn-params :port))) "missing port")
    (is (com.grzm.tespresso/thrown-with-data?
          (tespresso/ex-data-select= {:error :invalid-connection-params})
          (zo/client (assoc conn-params :port -1))) "invalid port")
    (is (com.grzm.tespresso/thrown-with-data?
          (tespresso/ex-data-select= {:error :invalid-connection-params})
          (zo/client (assoc conn-params :port 100000))) "invalid port")
    (is (com.grzm.tespresso/thrown-with-data?
          (tespresso/ex-data-select= {:error :invalid-connection-params})
          (zo/client (assoc conn-params :port "a"))) "invalid port")))
