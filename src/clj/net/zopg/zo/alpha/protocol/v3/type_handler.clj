(ns net.zopg.zo.alpha.protocol.v3.type-handler
  (:refer-clojure :exclude [type])
  (:require
   [net.zopg.zo.alpha.types :as types]))

(defn decoder-for [decoder {:keys [format] :as field-info}]
  ;; XXX should check if decoder supports format
  (case format
    :binary (partial types/binary-decode decoder field-info)
    :text   (partial types/text-decode decoder field-info)))

(defn decoders-for [registry type-info fields decoders]
  #_(prn {:decoders-for {:fields fields :decoders decoders :seq-decoders (seq decoders)}})
  (if (seq decoders)
    ;; XXX This is going to fail if length of decoders is wrong
    (doall (map #(if %2 (decoder-for %2 %1) (types/decoder-for registry type-info %1)) fields decoders))
    (let [ds (doall (map #(types/decoder-for registry type-info %) fields))]
      ;; ds needs to be forced, too.
      #_(prn {:mapped (count ds)})
      ds)))

(defn have-named-type? [{:keys [cache] :as type-inf} type-name]
  (when cache
    (-> @cache :oids (get type-name [:not-found type-name]))))

(defn decode-row-fn [registry type-info fields decoders]
  #_(prn {:decode-row-fn {:fields     fields :type-info (have-named-type? type-info ["zo" "my_enum"])
                          :sess-state (-> (:session type-info) :state deref :state)}})
  (let [decoders' (decoders-for registry type-info fields decoders)
        ;; need to force this sequence somehow
        drf       (fn [cols] (mapv (fn [decoder col] (decoder col)) decoders' cols))]
    #_(prn {#_:decode-row-fn-after #_(have-named-type? type-info ["zo" "my_enum"]) :drf drf})
    drf))
