(ns net.zopg.zo.alpha.util)

(defmacro with-timeout [ms & body]
  `(let [future# (future ~@body)]
     (try
       (.get future# ~ms java.util.concurrent.TimeUnit/MILLISECONDS)
       (catch java.util.concurrent.TimeoutException ex#
         (do
           (future-cancel future#)
           nil)))))

(defn wait-for
  [pred timeout-ms sleep-ms]
  (with-timeout timeout-ms
    (while (not (pred))
      (Thread/sleep sleep-ms))))
