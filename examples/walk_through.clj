;; Note to self:
;;  - setenv before starting CIDER
;;  - check window size

;; Hey! We've got a REPL!
(+ 1 2)

(require
  '[net.zopg.zo.alpha :as zo]
  '[com.grzm.pique.alpha.env :as env])

(env/params)

(def conn-params (env/params))

(def client (zo/client conn-params))

;; Queries

(def sess (zo/connect client))

(zo/q sess {:sql "SELECT 1 AS col"})

(zo/q sess {:sql "SELECT $1::INT AS a, $2::BOOL AS b"
            :params [42 true]})

(zo/q sess {:sql (str "SELECT oid, typname, typcategory"
                      " FROM pg_type"
                      " WHERE typname ~ $1"
                      " AND typcategory = $2")
            :params ["time" "D"]})

;; shorthand
(zo/q sess [:val "SELECT $1::INT" [3]])

;; Custom types

;; CREATE SCHEMA zo;
;; CREATE TYPE zo.my_enum AS ENUM ('a', 'c', 'b');
;; In Postgres, enum literals are 'a', 'c', 'b'

;; In Clojure, it's common to use keywords for enums
;; :a :c :b

(require '[net.zopg.zo.alpha.types :as types])

(name :a)
(keyword "a")

(deftype ZoMyEnumCodec []
  types/NamedCodec
  (-type-name [_] ["zo" "my_enum"])
  
  types/TextEncode
  (-text-encode [_ kw _]
    (name kw)) ;; :a => "a"

  types/TextDecode
  (-text-decode [_ _ s]
    (keyword s))) ;; "a" => :a

(def zo-my-enum-codec (ZoMyEnumCodec.))

;; specify/override encoders directly

(zo/q sess {:sql "SELECT $1::zo.my_enum AS col"
            :params [:a]
            :encoders [zo-my-enum-codec]
            :decoders [zo-my-enum-codec]})

;;
(require
  '[net.zopg.zo.alpha.types.core-registry :as registry])

;; XXX use named-codecs

(def my-registry
  (registry/core-registry
    {:named-encoders [zo-my-enum-codec]
     :named-decoders [zo-my-enum-codec]}))

(def custom-client
  (zo/client conn-params {:type-registry my-registry}))

(def custom-sess (zo/connect custom-client))

;; XXX this is hanging when sess doesn't know how to handle type
(zo/q custom-sess {:sql "SELECT $1::zo.my_enum AS col"
                   :params [:a]})

;; LISTEN/NOTIFY

(require
  '[clojure.core.async :refer [<! <!! close! go-loop]])

(def listener (zo/connect client))

;; Get a channel that receives all
;; NOTIFY messages subscribed to

(def listen-chan (zo/listen listener))

(defn prn-loop [c]
  (go-loop []
    (when-let [msg (<! c)]
      (println "HEARD" (pr-str msg))
      (recur))))

(prn-loop listen-chan)

;; Tell backend we're listening to "my_chan" NOTIFY messages
(zo/q listener {:sql "LISTEN my_chan"})
(zo/q listener {:sql "LISTEN my_other_chan"})

(def notifier (zo/connect client))
(zo/q notifier {:sql "NOTIFY my_chan"})
(zo/q notifier {:sql "NOTIFY my_chan, 'foo'"})
(zo/q notifier {:sql "NOTIFY my_other_chan, 'bar'"})
(zo/q notifier {:sql "NOTIFY not_my_chan, 'BAM!'"})
(zo/q notifier {:sql "NOTIFY my_other_chan, 'yay!'"})
