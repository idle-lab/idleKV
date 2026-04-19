(ns idlekv.elle-check
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [elle.rw-register :as rw]
            [jepsen.history :as h]))

(defn read-history [path]
  (with-open [reader (java.io.PushbackReader. (io/reader path))]
    (-> (edn/read {:eof nil} reader)
        h/history)))

(defn parse-models [raw]
  (->> (str/split raw #",")
       (remove str/blank?)
       (map keyword)
       vec))

(defn -main [& args]
  (let [[history-path raw-models directory] args
        _ (when (nil? history-path)
            (binding [*out* *err*]
              (println "usage: clojure -M -m idlekv.elle-check <history.edn> [model[,model...]] [directory]"))
            (System/exit 1))
        models (if raw-models
                 (parse-models raw-models)
                 [:strict-serializable])
        opts (cond-> {:consistency-models models}
               directory (assoc :directory directory))
        result (rw/check opts (read-history history-path))]
    (pprint result)
    (when-not (:valid? result)
      (System/exit 2))))
