(ns net.zopg.zo.async-test
  (:require
   [clojure.core.async :as a :refer [<!!]]
   [clojure.test :refer [are deftest is]]
   [net.zopg.zo.async.alpha :as azo]
   [net.zopg.zo.test.config :refer [conn-params]]
   [net.zopg.zo.session.alpha :as session]))

(alias 'sess 'net.zopg.zo.session)

(defn unwrap [res]
  (:result (<!! res)))

(deftest create-session
  (let [client (azo/client conn-params)]
    (with-open [sess (<!! (azo/connect client))]
      (is (= ::sess/ready-for-query (:state @(:state sess)))))))

(deftest queries
  (are [query expected]
      (let [client (azo/client conn-params)]
        (with-open [sess (<!! (azo/connect client))]
          (is (= expected (unwrap (azo/q sess query))))))

    [:val "SELECT 1"] 1

    [:col "VALUES (1), (2), (3)"] [1 2 3]

    [:row "SELECT 1, 2, 3"]
    {:?column? 1 :?column?-2 2 :?column?-3 3}

    [:tab "VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)"]
    [{:column1 1 :column2 2 :column3 3}
     {:column1 4 :column2 5 :column3 6}
     {:column1 7 :column2 8 :column3 9}]


    [:val "SELECT $1::INT" [1]] 1

    {:sql        "SELECT $1::INT"
     :params     [1]
     :row-fn     first
     :result-fn  first
     :as-vector? true}
    1))

(deftest cancel-query
  (let [client (azo/client conn-params)]
    (with-open [sess (<!! (azo/connect client))]
      (let [res-chan (azo/q sess [:val "WITH v (i, x) as (VALUES (1::INT, pg_sleep(10))) SELECT i FROM v;"])]
        (azo/cancel sess)
        (is (= {:error {:code               "57014"
                        :file               "postgres.c"
                        :line               "2997"
                        :localized-severity "ERROR"
                        :message            "canceling statement due to user request"
                        :routine            "ProcessInterrupts",
                        :severity           "ERROR"}}
               (<!! res-chan)))
        ;; this next one is timing sensitive, as it depends on when the
        ;; server sends the ":ready-for-query" message
        (is (true? (azo/ready? sess)))))))

(deftest listen
  (let [client (azo/client conn-params)]
    (with-open [listener (<!! (azo/connect client))
                notifier (<!! (azo/connect client))]
      (let [l-chan       (azo/listen listener)
            res          (a/into [] l-chan)
            pg-chan-name "this_channel"]
        (azo/q listener [:nil (str "LISTEN " pg-chan-name)])
        (azo/q notifier [:nil (str "NOTIFY " pg-chan-name ", 'a'")])
        (Thread/sleep 10) ;; XXX hack to allow response for server: should block
        (azo/q notifier [:nil (str "NOTIFY " pg-chan-name ", 'b'")])
        (Thread/sleep 10) ;; XXX hack to wait for response.
        (a/close! l-chan)
        (is (= [{:channel-name "this_channel" :payload "a"}
                {:channel-name "this_channel" :payload "b"}]
               (map #(select-keys % [:channel-name :payload]) (<!! res))))))))
