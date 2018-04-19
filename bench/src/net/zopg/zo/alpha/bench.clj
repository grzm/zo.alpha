(ns net.zopg.zo.alpha.bench
  (:require
   [jmh.core :as jmh]
   [clojure.edn :as edn]
   [clojure.java.io :as io])
  (:gen-class))

(def bench-env
    (-> "benchmarks.edn" io/resource slurp edn/read-string))

(defn run [opts]
  (jmh/run bench-env opts))

(defn -main [& [arg]]
  (let [opts (edn/read-string arg)]
    (prn (run opts))))
