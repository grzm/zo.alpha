(ns net.zopg.zo.protocol.v3.gloss-test
  (:require
   [byte-streams :as bs]
   [clojure.test :as test :refer [are deftest is]]
   [clojure.walk :as walk]
   [com.grzm.tespresso.alpha :as tespresso]
   [com.grzm.tespresso.bytes.alpha :as bytes]
   [net.zopg.zo.alpha.protocol.v3 :as v3]
   [net.zopg.zo.alpha.protocol.v3.gloss :as gloss]
   [gloss.io :as io]
   [gloss.core :as g])
  (:import
   (java.nio ByteBuffer)))

(def codec (gloss/->GlossCodec))

(defn byte-buffer
  "Converts a sequence of integers representing bytes to a ByteBuffer"
  [byte-seq]
  (-> byte-seq byte-array (bs/convert ByteBuffer)))

(defn byte-array-from-offset
  "Copies the bytes from the given buffer from the current offset into a
  new  byte array, returning the byte array."
  [bb]
  (let [r  (.remaining bb)
        nb (byte-array r)]
    (.get bb nb)
    nb))

(defn byte-vector
  "Converts a ByteBuffer to a vector of integers representing bytes"
  [buf]
  (-> buf byte-array-from-offset vec))

(defn vectorize-buffers [x]
  (if (instance? ByteBuffer x)
    (byte-vector x)
    x))

(deftest encode
  (are [cmd expected-byte-vector]
      (com.grzm.tespresso.bytes/bytes=
        (byte-buffer expected-byte-vector)
        (-> (v3/encode codec cmd) io/contiguous))

    [:bind {:portal-name       ""
            :statement-name    ""
            :parameter-formats [:text]
            :parameters        ["1"]
            :result-formats    [:text]}]
    [(int \B) 0 0 0 21
     0       ;; portal name
     0       ;; statement-name
     0 1     ;; parameter format count
     0 0     ;; first parameter format code
     0 1     ;; number of parameter values
     0 0 0 1 ;; len of first parameter value
     49      ;; first parameter value "1"
     0 1     ;; number of format codes
     0 0     ;; first format code
     ]

    [:cancel-request {:cancel-request-code v3/cancel-request-code
                      :process-id          54
                      :secret-key          32}]
    [0 0 0 16  0x04 0xd2 0x16 0x2e
     0 0 0 54   0  0  0 32]
    [:close {:statement-name ""}] [(int \C) 0 0 0 6 (int \S) 0]
    [:close {:portal-name ""}]    [(int \C) 0 0 0 6 (int \P) 0]

    [:copy-data {:data (byte-buffer [1 3 5 7 2 4 6 8])}]
    [(int \d) 0 0 0 12
     1 3 5 7  2 4 6 8]
    [:copy-done] [(int \c) 0 0 0 4]
    [:copy-fail] [(int \f) 0 0 0 4]

    [:describe {:statement-name ""}] [(int \D) 0 0 0 6 (int \S) 0]
    [:describe {:portal-name ""}]    [(int \D) 0 0 0 6 (int \P) 0]

    [:execute {:portal-name ""}] [(int \E) 0 0 0 9 0 0 0 0 0]
    [:execute {:portal-name "FOO" :max-rows 10}]
    [(int \E) 0 0 0 12 0x46 0x4f 0x4f 0 0 0 0 10]

    [:flush] [(int \H) 0 0 0 4]

    [:function-call {:procid           1282
                     :argument-formats [:text]
                     :argument-values  ["foo"]
                     :result-format    :text}]
    [(int \F) 0 0 0 23
     0 0 5 2                 ;; procid
     0 1                     ;; argument format count
     0 0                     ;; first argument format
     0 1                     ;; argument count
     0 0 0 3 0x66 0x6f 0x6f  ;; first argument
     0 0] ;; result format

    [:parse {:statement-name            ""
             :query-string              "SELECT CAST($1 AS INT)"
             :parameter-types           []}]
    [(int \P) 0 0 0 30
     0                        ;; statement name
     83 69 76 69  67 84 32 67
     65 83 84 40  36 49 32 65
     83 32 73 78  84 41 0     ;; query string
     0 0                      ;; number of specified parameters
     ]

    [:parse {:statement-name            "STMT"
             :query-string              "SELECT CAST($1 AS INT)"
             :parameter-types           [23]}]
    [(int \P) 0 0 0 38
     83 84 77 84 0            ;; statement name
     83 69 76 69  67 84 32 67
     65 83 84 40  36 49 32 65
     83 32 73 78  84 41 0     ;; query string
     0 1                      ;; number of specified parameters
     0 0 0 23]

    [:password-message {:password "supersecret"}]
    [(int \p) 0 0 0 16
     0x73 0x75 0x70 0x65 0x72 0x73 0x65 0x63 0x72 0x65 0x74 0x00]

    [:query {:query-string "SELECT TRUE"}]
    [(int \Q) 0 0 0 16  83 69 76 69 67 84 32 84 82 85 69 0]

    [:ssl-request {:ssl-request-code v3/ssl-request-code}]
    [0 0 0 8 0x04 0xd2 0x16 0x2f]

    [:startup-message
     {:protocol-version-number v3/protocol-version-number
      :user                    "me"
      :application-name        "zoo test"}]
    [0x00 0x00 0x00 0x2b  0x00 0x03 0x00 0x00
     0x75 0x73 0x65 0x72  0x00 0x6d 0x65 0x00
     0x61 0x70 0x70 0x6c  0x69 0x63 0x61 0x74
     0x69 0x6f 0x6e 0x5f  0x6e 0x61 0x6d 0x65
     0x00 0x7a 0x6f 0x6f  0x20 0x74 0x65 0x73
     0x74 0x00 0x00]

    [:sync]      [(int \S) 0 0 0 4]
    [:terminate] [(int \X) 0 0 0 4]))

(comment
  (def ex-vec
    [(int \d) 0 0 0 8
     9 8 7 6])

  (def out
    (->> ex-vec
         byte-buffer
         (io/decode gloss/backend-message)
         (v3/decode codec)

         ))


  (def d (-> out second :data))
  (def ba (byte-array (.remaining d)))
  (.get d ba)
  (vec ba)
  (-> d .remaining)
  (-> d rem vec))

(deftest decode
  (are [byte-vector data] (= data
                             (->> byte-vector
                                  byte-buffer
                                  (io/decode gloss/backend-message)
                                  (v3/decode codec)
                                  (walk/postwalk vectorize-buffers)))

    [(int \R) 0 0 0  8  0 0 0 0] [:authentication {:type :ok}]
    [(int \R) 0 0 0  8  0 0 0 2] [:authentication {:type :kerberos-v5}]
    [(int \R) 0 0 0  8  0 0 0 3] [:authentication {:type :cleartext-password}]
    [(int \R) 0 0 0 12  0 0 0 5
     0 0 0 0]                    [:authentication {:type :md5-password
                                                   :salt [0 0 0 0]}]
    [(int \R) 0 0 0  8  0 0 0 6] [:authentication {:type :scm-credential}]
    [(int \R) 0 0 0  8  0 0 0 7] [:authentication {:type :gss}]
    [(int \R) 0 0 0 16  0 0 0 8
     1 2 3 4  5 6 7 8]           [:authentication {:type :gss-continue
                                                   :data [1 2 3 4  5 6 7 8]}]
    [(int \R) 0 0 0  8  0 0 0 9] [:authentication {:type :sspi}]

    [(int \K) 0 0 0 12
     0 0 0 54  0 0 0 32] [:backend-key-data {:process-id 54 :secret-key 32}]

    [(int \2) 0 0 0 4] [:bind-complete]
    [(int \F) 0 0 0 4] [:cancel]
    [(int \3) 0 0 0 4] [:close-complete]

    [(int \d) 0 0 0 8
     9 8 7 6] [:copy-data {:data [9 8 7 6]}]

    [(int \c) 0 0 0 4] [:copy-done]

    [(int \G) 0 0 0 11
     0 ;; overall format
     0 2 ;; field count
     0 0 0 0] ;; field formats
    [:copy-in-response {:format        :text
                        :field-formats [:text :text]}]

    [(int \H) 0 0 0 11
     0 ;; overall format
     0 2 ;; field count
     0 0 0 0] ;; field formats
    [:copy-out-response {:format        :text
                         :field-formats [:text :text]}]

    [(int \W) 0 0 0 11
     0 ;; overall format
     0 2 ;; field count
     0 0 0 0] ;; field formats
    [:copy-both-response {:format        :text
                          :field-formats [:text :text]}]



    [(int \C) 0 0 0 13
     83 69 76 69  67 84 32 49
     0] [:command-complete {:command-tag "SELECT 1"}]

    [(int\D) 0 0 0 11
     0 1
     0 0 0 1  1] [:data-row {:fields [[1]]}]

    [(int \I) 0 0 0 4] [:empty-query-response]

    [(int \E) 00 00 00 65
     83 69 82 82 79 82 0
     86 69 82 82 79 82 0
     67 50 50 48 49 50 0
     77 100 105 118 105 115 105 111 110 32 98 121 32 122 101 114 111 0
     70 105 110 116 46 99 0
     76 55 49 57 0
     82 105 110 116 52 100 105 118 0
     0]
    [:error-response {:localized-severity "ERROR"
                      :severity           "ERROR"
                      :code               "22012"
                      :message            "division by zero"
                      :file               "int.c"
                      :line               "719"
                      :routine            "int4div"}]

    [(int \V) 0 0 0 12
     0 0 0 4  1 2 3 4] [:function-call-response {:value [1 2 3 4]}]

    [(int \n) 0 0 0 4] [:no-data]

    [(int \N) 00 00 00 109
     83   78  79  84  73  67  69  0    ;;  NOTICE
     86   78  79  84  73  67  69  0    ;;  NOTICE
     67   48  48  48  48  48   0       ;;  00000
     77  116  97  98 108 101  32 34    ;;  table "
     102 111 111  34  32 100 111 101   ;; foo" doe
     115  32 110 111 116  32 101 120   ;; s not ex
     105 115 116  44  32 115 107 105   ;; ist, ski
     112 112 105 110 103 0             ;; pping
     70  116  97  98 108 101  99 109   ;;  tablecm
     100 115  46  99 0                 ;;  ds.c
     76   55  54  52 0                 ;;  764
     82   68 114 111 112  69 114 114   ;;  DropErr
     111 114  77 115 103  78 111 110   ;; orMsgNon
     69  120 105 115 116 101 110 116 0 ;; Existent
     0]
    [:notice-response {:localized-severity "NOTICE"
                       :severity           "NOTICE"
                       :code               "00000"
                       :message            "table \"foo\" does not exist, skipping"
                       :file               "tablecmds.c"
                       :line               "764"
                       :routine            "DropErrorMsgNonExistent"}]

    [(int \A) 0 0 0 23
     0 0 0 24
     0x66 0x6f 0x6f 0
     0x6d 0x79 0x20 0x70 0x61
     0x79 0x6c 0x6f 0x61 0x64 0]    [:notification-response
                                     {:process-id   24
                                      :channel-name "foo"
                                      :payload      "my payload"}]
    [(int \t) 0 0 0 14
     0 2
     0 0 0 25  0 0 0 23]            [:parameter-description {:types [25 23]}]
    [(int \S) 0 0 0 25
     0x73 0x6f 0x6d 0x65 0x5f
     0x6e 0x61 0x6d 0x65 0x00
     0x73 0x6f 0x6d 0x65 0x5f
     0x76 0x61 0x6c 0x75 0x65 0x00] [:parameter-status
                                     {:name  "some_name"
                                      :value "some_value"}]
    [(int \1) 0 0 0 4]              [:parse-complete]
    [(int \s) 0 0 0 4]              [:portal-suspended]
    [(int \Z) 0 0 0 5 (int \I)]     [:ready-for-query {:status :idle}]
    [(int \Z) 0 0 0 5 (int \T)]     [:ready-for-query {:status :in-transaction}]
    [(int \Z) 0 0 0 5 (int \E)]     [:ready-for-query {:status :failed-transaction}]
    [(int \T) 0 0 0 28
     0 1                 ;; number of fields
     0x6e 0x6f 0x77 0x00 ;; field name
     0 0 0 0             ;; relid
     0 0                 ;; attnum
     0 0 0 25            ;; attypid
     0xff 0xff           ;; typlen
     0xff 0xff 0xff 0xff ;; typmod
     0 0                 ;; format code
     ]                              [:row-description
                                     {:fields [{:name     "now"
                                                :attrelid 0
                                                :attnum   0
                                                :typid    25
                                                :typlen   -1
                                                :typmod   -1
                                                :format   :text}]}]
    ))
