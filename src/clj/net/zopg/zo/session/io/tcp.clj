(ns net.zopg.zo.session.io.tcp
  (:require
   [aleph.tcp :as tcp]
   [clojure.core.async :as async]
   [net.zopg.zo.alpha.protocol.v3 :as v3]
   [net.zopg.zo.alpha.protocol.v3.gloss :as v3-gloss]
   [net.zopg.zo.session.io :as io]
   [manifold.stream :as stream])
  (:import
   (java.io Closeable)))

(def codec (v3-gloss/->GlossCodec))

(defn recv
  [io msg]
  (->> msg
       (v3/decode codec)
       (io/receive io)))

(defrecord TcpIO [host port in source client]
  Closeable
  (close [{:keys [source]}]
    (stream/close! source))

  io/IO
  (-send [{:keys [client] :as io} msg]
    @(stream/put! @client (v3/encode codec msg)))
  (-receive [{:keys [client in] :as io} msg]
    (async/put! in msg))
  ;; send-only client, used for sending cancel requests
  (-new [io]
    (let [])))

(defn tcp-io [host port in]
  (let [client (tcp/client {:host host :port port})
        source (v3/decode-stream codec @client)
        io (->TcpIO host port in source client)]
    (stream/consume (partial recv io) source)
    io))
