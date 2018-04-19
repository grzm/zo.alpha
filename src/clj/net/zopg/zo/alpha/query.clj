(ns net.zopg.zo.alpha.query
  (:require
   [clojure.spec.alpha :as s]
   [net.zopg.zo.anomalies :as anom]))

(defprotocol Query
  (-parameter-description [q])
  (-row-description [q]))

(defn parameter-description [q]
  (-parameter-description q))

(defn row-description [q]
  (-row-description q))

(in-ns 'net.zopg.zo.query)
(in-ns 'net.zopg.zo.alpha.query)
(alias 'q 'net.zopg.zo.query)

(s/def ::q/query-string string?)
(def res? #{:val :rowv :row :col :tab :tabv :nil})
(s/def ::q/res res?)
(s/def ::q/arg any?)
(s/def ::q/args (s/coll-of ::q/arg :kind vector?))
(s/def ::q/vector-query (s/cat :res (s/? ::q/res)
                               :query-string ::q/query-string
                               :args (s/? ::q/args)))


(s/def ::q/sql ::q/query-string)
(s/def ::q/param any?)
(s/def ::q/params (s/* ::q/param))

(s/def ::q/result-fn fn?)
(s/def ::q/row-fn fn?)
(s/def ::q/as-vector? boolean?)

(s/def ::q/encoder any?)
(s/def ::q/encoders (s/* ::q/encoder))

(s/def ::q/decoder any?)
(s/def ::q/decoders (s/* ::q/decoder))

(s/def ::q/map-query
  (s/keys :req-un [::q/sql]
          :opt-un [::q/result-fn
                   ::q/row-fn
                   ::q/params
                   ::q/encoders
                   ::q/decoders
                   ::q/as-vector?]))

(s/def ::q/query
  (s/or :vector ::q/vector-query
        :map ::q/map-query))

(s/def ::q/parsed-query
  (s/keys :req-un [::q/query-string
                   ::q/result-fn
                   ::q/row-fn
                   ::q/params
                   ::q/encoders
                   ::q/decoders
                   ::q/as-vector?]))

;; :rowv row as vector instead of map
;; :tabv vector of vectors instead of maps

;; want be able to make rows of tuples as well
;; [[name value] [name value] [name value] [name value]]
;; note, names don't need to be unique in this case

(defn parse-vector-query
  [[_ {:keys [res args query-string] :or {args []}}]]
  (let [result-fn  (if (#{:val :row :rowv} res) first identity)
        row-fn     (if (#{:val :col} res) first identity)
        as-vector? (boolean (#{:val :col :rowv :tabv} res))]
    {:query-string query-string
     :params       args
     :encoders     []
     :decoders     []
     :as-vector?   as-vector?
     :row-fn       row-fn
     :result-fn    result-fn}))

(defn parse-map-query
  [[_ {:keys [sql
              params
              encoders
              decoders
              row-fn
              result-fn
              as-vector?]
       :or   {params     []
              encoders   []
              decoders   []
              row-fn     identity
              result-fn  identity
              as-vector? false}}]]
  {:query-string sql
   :params       params
   :encoders     encoders
   :decoders     decoders
   :row-fn       row-fn
   :result-fn    result-fn
   :as-vector?   as-vector?})

(defn parse
  [query]
  (let [conformed (s/conform ::q/query query)]
    (if (s/invalid? conformed)
      {::anom/category ::anom/incorrect
       ::message       "Invalid query"
       :query          query
       :explain-data   (s/explain-data ::q/query query)}
      (case (first conformed)
        :vector (parse-vector-query conformed)
        :map    (parse-map-query conformed)))))

(s/fdef parse
        :args ::q/query
        :ret ::q/parsed-query)

;; we're going to use $1 instead of ? (which is JDBC specific)

;; The query string contained in a Parse message cannot include more
;; than one SQL statement; else a syntax error is reported. This
;; restriction does not exist in the simple-query protocol, but it
;; does exist in the extended protocol, because allowing prepared
;; statements or portals to contain multiple commands would complicate
;; the protocol unduly.

(comment

  (parse ["SELECT 1"])
  (parse [:val "SELECT $1" [1]])
  (s/conform ::q/query [:val "SELECT $1" [1]])
  (s/conform ::q/query {:sql "SELECT 1" :params [1]})

  (s/conform ::q/query [:val "SELECT $1" 1])
  (parse [:val "SELECT $1" [1]])
  (parse {:sql "SELECT $1" :params [1]})
  )

(def max-param-count 65536)

(defn parse-param-count [sql]
  (count (re-seq #"\$\d+" sql)))
