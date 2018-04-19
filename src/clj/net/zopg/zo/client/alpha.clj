(ns net.zopg.zo.client.alpha)

(defprotocol Client
  (-connect [this]))

(defn connect
  "Returns a database session"
  [client]
  (-connect client))
