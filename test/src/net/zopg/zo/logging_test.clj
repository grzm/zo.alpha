(ns net.zopg.zo.logging-test
  (:require
   [clojure.core.async :as async :refer [<!! close!]]
   [clojure.test :refer [are deftest is]]
   [clojure.tools.logging :as log]
   [net.zopg.zo.alpha :as zo]
   [net.zopg.zo.test.config :refer [conn-params]]
   [com.grzm.tespresso.tools-logging.alpha :refer [with-logging]]
   [net.zopg.zo.session.alpha :as session]
   [net.zopg.zo.test.session :as test.session]))

(alias 'sess 'net.zopg.zo.session)

(deftest accumulate-errors
  (let [client (zo/client conn-params)]
    (with-open [sess (zo/connect client)]
      (let [e-chan (zo/errors sess)
            errors (async/into [] e-chan)]
        (is (not (nil? (-> sess :state deref :error))))
        (let [res (zo/q sess [:val "SELECT 1/0"])]
          (is (= {:code     "22012"
                  :message  "division by zero"
                  :severity "ERROR"}
                 (select-keys (:error res) [:code :message :severity])))
          (is (= ::sess/ready-for-query(test.session/session-state-kw sess))))
        (let [res (zo/q sess [:val "SELECT 2/0"])]
          (is (= {:code     "22012"
                  :message  "division by zero"
                  :severity "ERROR"}
                 (select-keys (:error res) [:code :message :severity]))))
        (close! e-chan)
        (let [es (<!! errors)]
          (is (= {:code "22012"
                  :message "division by zero"
                  :severity "ERROR"}
                 (-> es first second (select-keys [:code :message :severity]))))
          (is (= {:code "22012"
                  :message "division by zero"
                  :severity "ERROR"}
                 (-> es second second (select-keys [:code :message :severity])))))))))
