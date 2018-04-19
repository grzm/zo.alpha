(ns net.zopg.zo.alpha.protocol.v3.spec
  (:require
   [clojure.spec.alpha :as s]
   [net.zopg.zo.alpha.protocol.v3 :as v3]))

(s/def ::v3/user string?)
(s/def ::v3/database string?)
(s/def ::v3/application-name string?)

(s/def ::v3/startup-message
  (s/keys :req-un [::v3/user]
          :opt-un [::v3/database
                   ::v3/application-name]))

