(ns net.zopg.zo.spec.alpha
  (:require
   [clojure.spec.alpha :as s]))

(in-ns 'net.zopg.zo.spec)
(in-ns 'net.zopg.zo.spec.alpha)
(alias 'zo 'net.zopg.zo.spec)

;; connection parameters

;; charset restrictions?
(s/def ::zo/user string?)
;; what's range of port numbers? Port numbers have a range of 0..65535
(s/def ::zo/port (s/int-in 0 65535))
(s/def ::zo/database string?)
(s/def ::zo/host string?)
(s/def ::zo/application-name string?)

(s/def ::zo/connection-parameters
  (s/keys :req-un [::zo/user
                   ::zo/port
                   ::zo/database
                   ::zo/host]))

(s/def ::zo/tcp-conn-params
  (s/keys :req-un [::zo/host
                   ::zo/port]))

(comment

  (s/explain-data ::zo/connection-parameters {:user "foo" :port 924 :database "baz" :host "as"})

  (require '[clojure.spec.gen.alpha :as gen])

  (gen/generate (s/gen ::zo/port))
  (gen/sample (s/gen ::zo/connection-parameters))
  (gen/sample (s/gen ::zo/tcp-conn-params))
  )
