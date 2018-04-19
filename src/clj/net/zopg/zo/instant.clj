(ns net.zopg.zo.instant
  (:import
   (java.io Writer)
   (java.time Instant OffsetDateTime ZoneId ZoneOffset)
   (java.time.format DateTimeFormatter)))

(defn- print-offset-date-time*
  [^OffsetDateTime odt, ^Writer w]
  (let [^DateTimeFormatter formatter (DateTimeFormatter/ofPattern
                                       "yyyy-MM-dd'T'HH:mm:ss")]
    (.write w (str "#inst "\"))
    (.write w (.format odt formatter))
    (.write w (format ".%09d-00:00" (.getNano odt)))
    (.write w "\"")))

(defn- print-offset-date-time
  [^OffsetDateTime odt, ^Writer w]
  (print-offset-date-time* (.withOffsetSameInstant odt ZoneOffset/UTC) w))

;; (defmethod print-method OffsetDateTime
;;   [^OffsetDateTime odt, ^Writer w]
;;   (print-offset-date-time odt w))

;; (defmethod print-dup OffsetDateTime
;;   [^OffsetDateTime odt, ^Writer w]
;;   (print-offset-date-time odt w))

(defn- print-instant
  [^Instant i, ^Writer w]
  (let [zone-id (ZoneId/of "Z")]
    (print-offset-date-time* (OffsetDateTime/ofInstant i zone-id) w)))

;; (defmethod print-method Instant
;;   [^Instant, i ^Writer w]
;;   (print-instant i w))

;; (defmethod print-dup Instant
;;   [^Instant i, ^Writer w]
;;   (print-instant i w))


(comment
  (def i (java.time.Instant/now))
  (def formatter (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss"))
  (def iodt (OffsetDateTime/ofInstant i (ZoneId/of "Z")))

  (java.sql.Timestamp/from i)
  
  i    ;; => #object[java.time.Instant 0xfee6037 "2018-04-05T00:45:20.642922Z"]
  iodt ;; => #object[java.time.OffsetDateTime 0x2542d57 "2018-04-05T00:45:20.642922Z"]
  (.format iodt formatter) ;; => 2018-04-05T00:45:20
  (format ".%09d-00:00" (.getNano iodt)) ;; => .642922000-00:00

  (def odt (java.time.OffsetDateTime/now))
  odt  ;; => #object[java.time.OffsetDateTime 0x2b55d7d7 "2018-04-04T19:48:55.718555-05:00"]
  (def zodt (.withOffsetSameInstant odt ZoneOffset/UTC))
  zodt ;; => #object[java.time.OffsetDateTime 0x66a7fc6c "2018-04-05T00:48:55.718555Z"]

  (print-offset-date-time* odt *out*)

  (.write *out* "#inst \"")
  
  
  )
