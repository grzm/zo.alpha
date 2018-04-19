(ns net.zopg.zo.session.util)

;; inspired by clojure.java.jdbc/make-cols-unique
;; https://github.com/clojure/java.jdbc/blob/37587dc32a29ea39ace9d944a250445619f528a3/src/main/clojure/clojure/java/jdbc.clj#L384-L399
(defn uniquify-names
  [xf]
  (let [seen (volatile! {})]
    (fn
      ([] (xf))
      ([result] (xf result))
      ([result input]
       (if-let [suffix (get @seen input)]
         (let [input' (str input "_" suffix)]
           (vswap! seen assoc
                   input (inc suffix)
                   input' 2) ;; this can fail if input' is also already seen
           (xf result input'))
         (do
           (vswap! seen assoc input 2)
           (xf result input)))))))

(defn default-column-naming
  [^String col-name]
  (-> (.replaceAll col-name "_" "-")
      keyword))
