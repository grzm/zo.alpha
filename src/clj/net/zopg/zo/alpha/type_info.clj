(ns net.zopg.zo.alpha.type-info
  (:require
   [clojure.core.async :refer [<!!]]
   [net.zopg.zo.session.alpha :as session]))

(defprotocol TypeInfo
  (-oid-by-name [ti nspname typname])
  (-type-name-by-oid [ti oid])
  (-column-info [ti relid attnum]))

(defn oid-by-name [ti nspname typname]
  (-oid-by-name ti nspname typname))

(defn type-name-by-oid [ti oid]
  (-type-name-by-oid ti oid))

(defn column-info [ti relid attnum]
  (-column-info ti relid attnum))

(defrecord PgType [oid nspname typname typcategory typtype typelem typarray typdelim])

(def built-in-type-rows
  #{
    [702 "pg_catalog" "abstime" "D" "b" 0 1023 ","]
    [1023 "pg_catalog" "_abstime" "A" "b" 702 0 ","]
    [1033 "pg_catalog" "aclitem" "U" "b" 0 1034 ","]
    [1034 "pg_catalog" "_aclitem" "A" "b" 1033 0 ","]
    [2276 "pg_catalog" "any" "P" "p" 0 0 ","]
    [2277 "pg_catalog" "anyarray" "P" "p" 0 0 ","]
    [2283 "pg_catalog" "anyelement" "P" "p" 0 0 ","]
    [3500 "pg_catalog" "anyenum" "P" "p" 0 0 ","]
    [2776 "pg_catalog" "anynonarray" "P" "p" 0 0 ","]
    [3831 "pg_catalog" "anyrange" "P" "p" 0 0 ","]
    [1560 "pg_catalog" "bit" "V" "b" 0 1561 ","]
    [1561 "pg_catalog" "_bit" "A" "b" 1560 0 ","]
    [16 "pg_catalog" "bool" "B" "b" 0 1000 ","]
    [1000 "pg_catalog" "_bool" "A" "b" 16 0 ","]
    [603 "pg_catalog" "box" "G" "b" 600 1020 ";"]
    [1020 "pg_catalog" "_box" "A" "b" 603 0 ";"]
    [1042 "pg_catalog" "bpchar" "S" "b" 0 1014 ","]
    [1014 "pg_catalog" "_bpchar" "A" "b" 1042 0 ","]
    [17 "pg_catalog" "bytea" "U" "b" 0 1001 ","]
    [1001 "pg_catalog" "_bytea" "A" "b" 17 0 ","]
    [18 "pg_catalog" "char" "S" "b" 0 1002 ","]
    [1002 "pg_catalog" "_char" "A" "b" 18 0 ","]
    [29 "pg_catalog" "cid" "U" "b" 0 1012 ","]
    [1012 "pg_catalog" "_cid" "A" "b" 29 0 ","]
    [650 "pg_catalog" "cidr" "I" "b" 0 651 ","]
    [651 "pg_catalog" "_cidr" "A" "b" 650 0 ","]
    [718 "pg_catalog" "circle" "G" "b" 0 719 ","]
    [719 "pg_catalog" "_circle" "A" "b" 718 0 ","]
    [2275 "pg_catalog" "cstring" "P" "p" 0 1263 ","]
    [1263 "pg_catalog" "_cstring" "A" "b" 2275 0 ","]
    [1082 "pg_catalog" "date" "D" "b" 0 1182 ","]
    [1182 "pg_catalog" "_date" "A" "b" 1082 0 ","]
    [3912 "pg_catalog" "daterange" "R" "r" 0 3913 ","]
    [3913 "pg_catalog" "_daterange" "A" "b" 3912 0 ","]
    [3838 "pg_catalog" "event_trigger" "P" "p" 0 0 ","]
    [3115 "pg_catalog" "fdw_handler" "P" "p" 0 0 ","]
    [700 "pg_catalog" "float4" "N" "b" 0 1021 ","]
    [1021 "pg_catalog" "_float4" "A" "b" 700 0 ","]
    [701 "pg_catalog" "float8" "N" "b" 0 1022 ","]
    [1022 "pg_catalog" "_float8" "A" "b" 701 0 ","]
    [3642 "pg_catalog" "gtsvector" "U" "b" 0 3644 ","]
    [3644 "pg_catalog" "_gtsvector" "A" "b" 3642 0 ","]
    [325 "pg_catalog" "index_am_handler" "P" "p" 0 0 ","]
    [869 "pg_catalog" "inet" "I" "b" 0 1041 ","]
    [1041 "pg_catalog" "_inet" "A" "b" 869 0 ","]
    [21 "pg_catalog" "int2" "N" "b" 0 1005 ","]
    [1005 "pg_catalog" "_int2" "A" "b" 21 0 ","]
    [22 "pg_catalog" "int2vector" "A" "b" 21 1006 ","]
    [1006 "pg_catalog" "_int2vector" "A" "b" 22 0 ","]
    [23 "pg_catalog" "int4" "N" "b" 0 1007 ","]
    [1007 "pg_catalog" "_int4" "A" "b" 23 0 ","]
    [3904 "pg_catalog" "int4range" "R" "r" 0 3905 ","]
    [3905 "pg_catalog" "_int4range" "A" "b" 3904 0 ","]
    [20 "pg_catalog" "int8" "N" "b" 0 1016 ","]
    [1016 "pg_catalog" "_int8" "A" "b" 20 0 ","]
    [3926 "pg_catalog" "int8range" "R" "r" 0 3927 ","]
    [3927 "pg_catalog" "_int8range" "A" "b" 3926 0 ","]
    [2281 "pg_catalog" "internal" "P" "p" 0 0 ","]
    [1186 "pg_catalog" "interval" "T" "b" 0 1187 ","]
    [1187 "pg_catalog" "_interval" "A" "b" 1186 0 ","]
    [114 "pg_catalog" "json" "U" "b" 0 199 ","]
    [199 "pg_catalog" "_json" "A" "b" 114 0 ","]
    [3802 "pg_catalog" "jsonb" "U" "b" 0 3807 ","]
    [3807 "pg_catalog" "_jsonb" "A" "b" 3802 0 ","]
    [2280 "pg_catalog" "language_handler" "P" "p" 0 0 ","]
    [628 "pg_catalog" "line" "G" "b" 701 629 ","]
    [629 "pg_catalog" "_line" "A" "b" 628 0 ","]
    [601 "pg_catalog" "lseg" "G" "b" 600 1018 ","]
    [1018 "pg_catalog" "_lseg" "A" "b" 601 0 ","]
    [829 "pg_catalog" "macaddr" "U" "b" 0 1040 ","]
    [1040 "pg_catalog" "_macaddr" "A" "b" 829 0 ","]
    [774 "pg_catalog" "macaddr8" "U" "b" 0 775 ","]
    [775 "pg_catalog" "_macaddr8" "A" "b" 774 0 ","]
    [790 "pg_catalog" "money" "N" "b" 0 791 ","]
    [791 "pg_catalog" "_money" "A" "b" 790 0 ","]
    [19 "pg_catalog" "name" "S" "b" 18 1003 ","]
    [1003 "pg_catalog" "_name" "A" "b" 19 0 ","]
    [1700 "pg_catalog" "numeric" "N" "b" 0 1231 ","]
    [1231 "pg_catalog" "_numeric" "A" "b" 1700 0 ","]
    [3906 "pg_catalog" "numrange" "R" "r" 0 3907 ","]
    [3907 "pg_catalog" "_numrange" "A" "b" 3906 0 ","]
    [26 "pg_catalog" "oid" "N" "b" 0 1028 ","]
    [1028 "pg_catalog" "_oid" "A" "b" 26 0 ","]
    [30 "pg_catalog" "oidvector" "A" "b" 26 1013 ","]
    [1013 "pg_catalog" "_oidvector" "A" "b" 30 0 ","]
    [2282 "pg_catalog" "opaque" "P" "p" 0 0 ","]
    [602 "pg_catalog" "path" "G" "b" 0 1019 ","]
    [1019 "pg_catalog" "_path" "A" "b" 602 0 ","]
    [3221 "pg_catalog" "_pg_lsn" "A" "b" 3220 0 ","]
    [600 "pg_catalog" "point" "G" "b" 701 1017 ","]
    [1017 "pg_catalog" "_point" "A" "b" 600 0 ","]
    [604 "pg_catalog" "polygon" "G" "b" 0 1027 ","]
    [1027 "pg_catalog" "_polygon" "A" "b" 604 0 ","]
    [2249 "pg_catalog" "record" "P" "p" 0 2287 ","]
    [2287 "pg_catalog" "_record" "P" "p" 2249 0 ","]
    [1790 "pg_catalog" "refcursor" "U" "b" 0 2201 ","]
    [2201 "pg_catalog" "_refcursor" "A" "b" 1790 0 ","]
    [2205 "pg_catalog" "regclass" "N" "b" 0 2210 ","]
    [2210 "pg_catalog" "_regclass" "A" "b" 2205 0 ","]
    [3734 "pg_catalog" "regconfig" "N" "b" 0 3735 ","]
    [3735 "pg_catalog" "_regconfig" "A" "b" 3734 0 ","]
    [3769 "pg_catalog" "regdictionary" "N" "b" 0 3770 ","]
    [3770 "pg_catalog" "_regdictionary" "A" "b" 3769 0 ","]
    [4089 "pg_catalog" "regnamespace" "N" "b" 0 4090 ","]
    [4090 "pg_catalog" "_regnamespace" "A" "b" 4089 0 ","]
    [2203 "pg_catalog" "regoper" "N" "b" 0 2208 ","]
    [2208 "pg_catalog" "_regoper" "A" "b" 2203 0 ","]
    [2204 "pg_catalog" "regoperator" "N" "b" 0 2209 ","]
    [2209 "pg_catalog" "_regoperator" "A" "b" 2204 0 ","]
    [24 "pg_catalog" "regproc" "N" "b" 0 1008 ","]
    [1008 "pg_catalog" "_regproc" "A" "b" 24 0 ","]
    [2202 "pg_catalog" "regprocedure" "N" "b" 0 2207 ","]
    [2207 "pg_catalog" "_regprocedure" "A" "b" 2202 0 ","]
    [4096 "pg_catalog" "regrole" "N" "b" 0 4097 ","]
    [4097 "pg_catalog" "_regrole" "A" "b" 4096 0 ","]
    [2206 "pg_catalog" "regtype" "N" "b" 0 2211 ","]
    [2211 "pg_catalog" "_regtype" "A" "b" 2206 0 ","]
    [703 "pg_catalog" "reltime" "T" "b" 0 1024 ","]
    [1024 "pg_catalog" "_reltime" "A" "b" 703 0 ","]
    [210 "pg_catalog" "smgr" "U" "b" 0 0 ","]
    [25 "pg_catalog" "text" "S" "b" 0 1009 ","]
    [1009 "pg_catalog" "_text" "A" "b" 25 0 ","]
    [27 "pg_catalog" "tid" "U" "b" 0 1010 ","]
    [1010 "pg_catalog" "_tid" "A" "b" 27 0 ","]
    [1083 "pg_catalog" "time" "D" "b" 0 1183 ","]
    [1183 "pg_catalog" "_time" "A" "b" 1083 0 ","]
    [1114 "pg_catalog" "timestamp" "D" "b" 0 1115 ","]
    [1115 "pg_catalog" "_timestamp" "A" "b" 1114 0 ","]
    [1184 "pg_catalog" "timestamptz" "D" "b" 0 1185 ","]
    [1185 "pg_catalog" "_timestamptz" "A" "b" 1184 0 ","]
    [1266 "pg_catalog" "timetz" "D" "b" 0 1270 ","]
    [1270 "pg_catalog" "_timetz" "A" "b" 1266 0 ","]
    [704 "pg_catalog" "tinterval" "T" "b" 0 1025 ","]
    [1025 "pg_catalog" "_tinterval" "A" "b" 704 0 ","]
    [2279 "pg_catalog" "trigger" "P" "p" 0 0 ","]
    [3310 "pg_catalog" "tsm_handler" "P" "p" 0 0 ","]
    [3615 "pg_catalog" "tsquery" "U" "b" 0 3645 ","]
    [3645 "pg_catalog" "_tsquery" "A" "b" 3615 0 ","]
    [3908 "pg_catalog" "tsrange" "R" "r" 0 3909 ","]
    [3909 "pg_catalog" "_tsrange" "A" "b" 3908 0 ","]
    [3910 "pg_catalog" "tstzrange" "R" "r" 0 3911 ","]
    [3911 "pg_catalog" "_tstzrange" "A" "b" 3910 0 ","]
    [3614 "pg_catalog" "tsvector" "U" "b" 0 3643 ","]
    [3643 "pg_catalog" "_tsvector" "A" "b" 3614 0 ","]
    [2970 "pg_catalog" "txid_snapshot" "U" "b" 0 2949 ","]
    [2949 "pg_catalog" "_txid_snapshot" "A" "b" 2970 0 ","]
    [705 "pg_catalog" "unknown" "X" "p" 0 0 ","]
    [2950 "pg_catalog" "uuid" "U" "b" 0 2951 ","]
    [2951 "pg_catalog" "_uuid" "A" "b" 2950 0 ","]
    [1562 "pg_catalog" "varbit" "V" "b" 0 1563 ","]
    [1563 "pg_catalog" "_varbit" "A" "b" 1562 0 ","]
    [1043 "pg_catalog" "varchar" "S" "b" 0 1015 ","]
    [1015 "pg_catalog" "_varchar" "A" "b" 1043 0 ","]
    [2278 "pg_catalog" "void" "P" "p" 0 0 ","]
    [28 "pg_catalog" "xid" "U" "b" 0 1011 ","]
    [1011 "pg_catalog" "_xid" "A" "b" 28 0 ","]
    [142 "pg_catalog" "xml" "U" "b" 0 143 ","]
    [143 "pg_catalog" "_xml" "A" "b" 142 0 ","]})

(def built-in-col-info-rows
  [[:attrelid :attnum :relnamespace :relname :attname]
   [1247 -2 "pg_catalog" "pg_type" "oid"]
   [1247 1 "pg_catalog" "pg_type" "typname"]
   [1247 6 "pg_catalog" "pg_type" "typtype"]
   [1247 7 "pg_catalog" "pg_type" "typcategory"]
   [1247 10 "pg_catalog" "pg_type" "typdelim"]
   [1247 12 "pg_catalog" "pg_type" "typelem"]
   [1247 13 "pg_catalog" "pg_type" "typarray"]
   [1249 1 "pg_catalog" "pg_attribute" "attrelid"]
   [1249 2 "pg_catalog" "pg_attribute" "attname"]
   [1249 6 "pg_catalog" "pg_attribute" "attnum"]
   [1259 -2 "pg_catalog" "pg_class" "oid"]
   [2615 -2 "pg_catalog" "pg_namespace" "oid"]
   [2615 1 "pg_catalog" "pg_namespace" "nspname"]])

(defn add-pg-type-row
  ([m row]
   (add-pg-type-row m row (apply ->PgType row)))
  ([m [oid nspname typname typcategory typtype typelem typarray typdelim :as row] pg-type]
   (-> m
       (assoc-in [:types oid] pg-type)
       (assoc-in [:oids [nspname typname]] oid))))

(defn fetch-pg-type
  [{:keys [session cache] :as ti} nspname typname]
  ;; I always want the element row, regardless of the name that's passed in
  (let [query
        [:rowv (str "SELECT typ.oid, nspname, typname,"
                    " typcategory, typtype,"
                    " typelem, typarray, typdelim"
                    " FROM pg_catalog.pg_type typ"
                    " JOIN pg_catalog.pg_namespace nsp"
                    " ON nsp.oid = typnamespace"
                    " WHERE (nspname, typname) = ($1, $2)")
         [nspname typname]]
        q-res                    (session/q session query)
        {:keys [result] :as res} (<!! q-res)]
    (when (seq result)
      (swap! cache add-pg-type-row result)
      (first result))))

(defn fetch-pg-type-by-oid
  [{:keys [session cache] :as ti} oid]
  ;; I always want the element row, regardless of the name that's passed in
  (let [query
        [:rowv (str "SELECT typ.oid, typnamespace::regnamespace::name, typname,"
                    " typcategory, typtype,"
                    " typelem, typarray, typdelim"
                    " FROM pg_catalog.pg_type typ"
                    " WHERE typ.oid = $1")
         [oid]]
        res                       (session/q session query)
        #__                         #_(prn {:fetch-pg-type-by-oid query
                                        :res                  (type res)
                                        :queries              (map #(update-in % [:result :out] boolean)
                                                                   (-> session :state deref :queries))})
        {:keys [result] :as res'} (<!! res)]
    #_(prn {:res' res' :result result})
    (when (seq result)
      (let [pg-type (apply ->PgType result)]
        (swap! cache add-pg-type-row result pg-type)
        #_(prn {:cache {:oid-by-name (-> @cache (get-in [:oids ["zo" "my_enum"]]))
                        :big-oids    (filter #(> 5000 (first %1)) (:types @cache) )}})
        pg-type))))

(defn add-col-info-row
  ([cache [attrelid attnum nspname relname attname]]
   (add-col-info-row cache attrelid attnum
                     {:nspname nspname,
                      :relname relname,
                      :attname attname}))
  ([cache attrelid attnum col-info]
   (assoc-in cache [:cols [attrelid attnum]] col-info)))

(defn fetch-column-info
  [{:keys [session cache] :as ti} relid attnum]
  (let [query
        [:rowv (str "SELECT attrelid, attnum,"
                    " relnamespace::regnamespace::name, relname, attname"
                    " FROM pg_catalog.pg_attribute"
                    " JOIN pg_catalog.pg_class rel ON rel.oid = attrelid"
                    " WHERE (attrelid, attnum) = ($1, $2)")
         [relid attnum]]

        {:keys [result] :as res} (<!! (session/q session query))]
    (when (seq result)
      (let [[attrelid attnum nspname relname attname]
            result

            col-info
            {:nspname nspname :relname relname :attname attname}]
        (swap! cache add-col-info-row attrelid attnum col-info)
        col-info))))

(defn type-name-from-pg-type
  [{:keys [nspname typname]}]
  [nspname typname])

(defrecord TypeInfoCache [session cache]
  TypeInfo
  (-oid-by-name [ti nspname typname]
    (if-let [oid (get-in @cache [:oids [nspname typname]])]
      oid
      (fetch-pg-type ti nspname typname)))
  (-type-name-by-oid [ti oid]
    (if-let [pg-type (get-in @cache [:types oid])]
      (type-name-from-pg-type pg-type)
      (when-let [pg-type (fetch-pg-type-by-oid ti oid)]
        (type-name-from-pg-type pg-type))))
  (-column-info [ti relid attnum]
    (if-let [col-info (get-in @cache [:cols [relid attnum]])]
      col-info
      (fetch-column-info ti relid attnum))))


(def pg-types (reduce #(add-pg-type-row %1 %2)
                      {:types {}
                       :oids  {}}
                      built-in-type-rows))

(def col-info (reduce #(add-col-info-row %1 %2) {} built-in-col-info-rows))

(def initial-type-info-cache (assoc pg-types :cols col-info))

(defn type-info-cache
  ([sess]
   (type-info-cache sess initial-type-info-cache))
  ([sess initial-cache]
   (->TypeInfoCache sess (atom initial-cache))))


(defn- print-type-info-cache
  [^TypeInfoCache tic, ^java.io.Writer w]
  (.write w "#net.zopg.zo.alpha.type-info/TypeInfoCache ")
  (.write w (str (-> (dissoc tic :session)
                     (update :cache deref)))))

(defmethod print-dup TypeInfoCache
  [^TypeInfoCache tic, ^java.io.Writer w]
  (print-type-info-cache tic w))

(defmethod print-method TypeInfoCache
  [^TypeInfoCache tic, ^java.io.Writer w]
  (print-type-info-cache tic w))
