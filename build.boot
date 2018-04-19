(require '[clojure.edn :as edn])

(def project (-> "project.edn" slurp edn/read-string))
(def version "0.1.1")

(set-env! :dependencies '[[adzerk/boot-test "RELEASE" :scope "test"]
                          [boot-codox "0.10.3" :scope "test"]
                          [boot-fmt "0.1.6" :scope "test"]
                          [metosin/boot-alt-test "0.3.2" :scope "test"]
                          [seancorfield/boot-tools-deps "0.3.0" :scope "test"
                           :exclusions [ch.qos.logback/logback-classic
                                        org.clojure/clojure]]
                          [zprint "0.4.2" :scope "test"
                           :exclusions [org.clojure/clojure]]])

(require '[boot-tools-deps.core :refer [deps load-deps]])

(task-options!
  pom (constantly (assoc project :version version)))

(deftask build
  "Build and install the project locally."
  []
  (comp (deps) (pom) (javac) (jar) (install)))

;;; testing

(require '[adzerk.boot-test :as boot-test])

(deftask test
  []
  (comp (deps :aliases [:test] :overwrite-boot-deps true)
        (javac)
        (boot-test/test)))

(require '[metosin.boot-alt-test :as boot-alt-test])

(deftask alt-test
  []
  (comp (deps :aliases [:test]
              :quick-merge true)
        (watch)
        (javac)
        (boot-alt-test/alt-test)))

;;; code formating

(require '[boot-fmt.core :as fmt])

(task-options!
  fmt/fmt {:options {:style  :community
                     :extend {:flow?         true
                              :indent        2
                              :nl-separator? true}
                     :fn-map {":import"     :flow
                              ":require"    :flow
                              "and"         :force-nl-body
                              "defprotocol" :arg1-body
                              "merge"       :hang
                              "or"          :force-nl-body
                              "try"         :flow-body
                              "comment"     :flow-body}
                     :list   {:indent-arg 1}
                     :map    {:justify? true}
                     :pair   {:justify? true}
                     :set    {:wrap? nil}
                     :vector {:wrap? nil}}
           :files   #{"src"}
           :mode    :diff})

(deftask fmt
  "format"
  []
  (comp (deps :aliases [:test :bench]) (fmt/fmt)))

;;; documentation

(require '[codox.boot :refer [codox]])

(task-options!
  codox {:version      version
         :source-paths  (get-env :resource-paths)
         :name         (name (:project project))
         :output-path "codox"})

(deftask docs
  "generate html documentation"
  []
  (comp (deps :quick-merge true)
        (codox)
        (target)))
