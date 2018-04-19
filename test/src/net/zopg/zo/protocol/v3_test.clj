(ns net.zopg.zo.protocol.v3-test
  (:require
   [net.zopg.zo.alpha.protocol.v3 :as v3]
   [clojure.test :as t :refer [deftest is]]))

(deftest startup-message-params
  (is (= #{["user" "my-user"]
           ["database" "my-db"]
           ["is_superuser" "false"]}
         (v3/startup-message-params
           {:user "my-user"
            :database "my-db"
            :superuser? false}))))

(deftest startup-message
  (is (= [:startup-message {:protocol-version-number v3/protocol-version-number
                            :user "me"
                            :database "my-db"
                            :application-name "zo-text"}]
         (v3/startup-message {:user "me"
                              :database "my-db"
                              :application-name "zo-text"}))))

(deftest cancel-request
  (let [process-id 32
        secret-key 42]
    (is (= [:cancel-request {:cancel-request-code v3/cancel-request-code
                             :process-id          process-id
                             :secret-key          secret-key}]
           (v3/cancel-request {:process-id process-id
                               :secret-key secret-key})))))
