(ns claudia.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]))

(defn csv->clj [filename]
  (with-open [rdr (io/reader filename)]
    (let [raw-headers (first (line-seq rdr))
          headers (map keyword (str/split raw-headers #"\t"))
          raw-data (rest (line-seq rdr))
          data (doall (map  #(str/split % #"\t") raw-data))]

      (comment (do
                 (println raw-headers)
                 (println headers)
                 (println (last raw-data))
                 (println (last data))))

      (->> data
           (map #(zipmap headers %))))))

(def desired-columns  [:referralId
                       :studentReadableId
                       :studentHasIep
                       :studentHas504Plan
                       :studentGenderId
                       :studentGradeId
                       :studentRaceEthnicity
                       :studentDisabilities
                       :problemBehaviors
                       :actionsTaken
                       (keyword "Other Administrative Decision")])

(defn maps->csv [path xs]
  (let [headers (map name desired-columns)
        rows (mapv #(mapv % desired-columns) xs)]
    (with-open [file (io/writer path)]
      (csv/write-csv file (cons headers rows)))))


(defn do-it []
  (let [output-path "/tmp/claudia.csv"
        data (->> "resources/data.tsv"
                  (csv->clj)
                  (map #(select-keys % desired-columns))
                  (map #(reduce-kv (fn [m k v]
                                     (assoc m k (if (= :problemBehaviors k)
                                                  (str/replace v #"\"" "")
                                                  v)))
                                   {}
                                   %))
                  (map #(into (sorted-map-by (fn [x y] (< (.indexOf desired-columns x)
                                                          (.indexOf desired-columns y)))) %))
                  )
        ]
    (maps->csv output-path data)
    ))

