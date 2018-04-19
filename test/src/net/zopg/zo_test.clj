(ns net.zopg.zo-test
  (:require
   [clojure.core.async :as async :refer [<!!]]
   [clojure.test :refer [are deftest is]]
   [net.zopg.zo.alpha :as zo]
   [net.zopg.zo.session.alpha]
   [net.zopg.zo.test.config :refer [conn-params]]))

(alias 'sess 'net.zopg.zo.session)

(deftest create-session
  (let [client (zo/client conn-params)]
    (with-open [sess (zo/connect client)]
      (is (= ::sess/ready-for-query (:state @(:state sess)))))))

(deftest queries
  (are [query expected]
      (let [client (zo/client conn-params)]
        (with-open [sess (zo/connect client)]
          (is (= expected (zo/q sess query)))))

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
    1

    {:sql "SELECT $1::bool, $2::text::timestamptz"
     :params [true  "2018-04-02T00:29:14.993-00:00"]
     :result-fn first
     :as-vector? true}
    [true (java.time.Instant/parse "2018-04-02T00:29:14.993Z")]))

;; this is hanging, and I'm not sure why
#_(deftest cancel-query
  (let [client (zo/client conn-params)]
    (with-open [sess (zo/connect client)]
      (let [res-chan (async/thread (zo/q sess [:val "WITH v (i, x) as (VALUES (1::INT, pg_sleep(10))) SELECT i FROM v;"]))]
        (zo/cancel sess)
        ;; This error message is version sensitive (due to *at least* the file/line references)
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
        (is (true? (zo/ready? sess)))
        (async/close! res-chan)))))

(deftest listen
  (let [client (zo/client conn-params)]
    (with-open [listener (zo/connect client)
                notifier (zo/connect client)]
      (let [l-chan       (zo/listen listener)
            res          (async/into [] l-chan)
            pg-chan-name "this_channel"]
        (zo/q listener [:nil (str "LISTEN " pg-chan-name)])
        (zo/q notifier [:nil (str "NOTIFY " pg-chan-name ", 'a'")])
        (Thread/sleep 10) ;; XXX hack to allow response for server so sess can cycle
        (zo/q notifier [:nil (str "NOTIFY " pg-chan-name ", 'b'")])
        (Thread/sleep 10) ;; XXX hack to wait for response.
        (async/close! l-chan)
        (let [[msgs c] (async/alts!! [(async/timeout 10) res])]
          (if (= c res)
            (is (= [{:channel-name "this_channel" :payload "a"}
                {:channel-name "this_channel" :payload "b"}]
                   (map #(select-keys % [:channel-name :payload]) msgs)))
            (is (true? false "failed due to timeout"))))
        (async/close! res)))))
