(ns claudia.post
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]
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

(defn maps->csv [path xs]
  (let [columns (keys (first xs))
        headers (map name columns)
        rows (mapv #(mapv % columns) xs)]
    (with-open [file (io/writer path)]
      (csv/write-csv file (cons headers rows)))))

(defn remove-extra-doublequotes [m]
  (reduce-kv (fn [m k v]
               (assoc m k
                      (if (some #{k} '(:problemBehaviors :actionsTaken))
                        (str/replace v #"\"" "")
                        v)))
             {}
             m))

(def int-keys [
               ;; :Arrest
               ;; :Bridges
               ;; :OSS
               ;; :OSSplusBridges
               ;; :PoliceContact
               ;; :actionsTaken
               :pbAlcoholTobaccoDrugs
               :pbArsonBombWeapons
               :pbBullyingHarass
               :pbDisrespectDisruptionInappLan
               :pbGangDisplay
               :pbHITlying
               :pbHIToppositional
               :pbHITphysical
               :pbHITstealing
               :pbInappAffection
               :pbOtherDressTech
               :pbOutBounds
               :pbPropDamage
               ;; :problemBehaviors
               :referralId
               ;; :studentDisabilities
               ;; :studentGenderId
               :studentGradeId
               ;; :studentHas504Plan
               ;; :studentHasIep
               ;; :studentRaceEthnicity
               :studentReadableId
               ])

(defn cast-int-fields [m]
  (reduce-kv (fn [m k v]
               (assoc m k (if (some #{k} int-keys)
                            (int (bigdec v))
                            v)))
             {}
             m))


(defn do-it []
  (let [data (->> "resources/post.tsv"
                  (csv->clj)
                  (map remove-extra-doublequotes)
                  (map cast-int-fields))
        indexed-data (set/index data [:studentReadableId])
        ]
    indexed-data))


