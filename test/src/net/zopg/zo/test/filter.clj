(ns net.zopg.zo.test.filter)


(defn require-meta [tag test-var]
  (tag (meta test-var)))

(defn exclude-meta [tag test-var]
  (not (require-meta tag test-var)))

(defn no-db [test-var]
  (let [ks (-> test-var meta keys set)]
    (and (contains? ks :no-db)
         (not (contains? ks :skip)))))
