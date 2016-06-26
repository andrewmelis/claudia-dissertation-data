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
               :Alcohol
               :Arson
               :Bomb
               :Bullying
               :Defiance
               :DisrespectDisruptionInappLan
               :Drugs
               :Fight
               :GangDisplay
               :Harassment
               :InappAffection
               :Lying
               :OtherDressTech
               :OutBounds
               :PhysicalAgg
               :PropDamage
               :Stealing
               :Tobacco
               :Weapons
               
               ;; :actionsTaken
               ;; :problemBehaviors
               ;; :referralId
               ;; :studentDisabilities
               ;; :studentGenderId
               ;; :studentGradeId
               ;; :studentHas504Plan
               ;; :studentHasIep
               ;; :studentRaceEthnicity
               ;; :studentReadableId
               ])

(defn cast-int-fields [m]
  (reduce-kv (fn [m k v]
               (assoc m k (if (some #{k} int-keys)
                            (long (bigdec v))
                            v)))
             {}
             m))

(defn merge-student-records [set]
  (reduce (fn [x y]
            (merge-with (fn [former latter]
                          (if (number? former)
                            (+ former latter)
                            (if (seq? former)
                              (conj former latter)
                              (conj '() former latter))))
                        x
                        y))
          set))

(defn readable-seqs [m]
  (reduce-kv (fn [m k v]
               (assoc m
                      k
                      (if (and (seq? v)
                               (apply = v))
                        (first v)
                        (-> (str/replace (str v) #"\" \"" "\",\"")
                            (str/replace #"[\(\)]" "")))))
             {} m))


;; reduce stage
;; reduce referrals to collection for each student
(defn do-it []
  (let [output-path "/tmp/claudia-v2/v2-reduce-swis.csv"
        data (->> "resources/map-stage-output.tsv"
                  (csv->clj)
                  (map remove-extra-doublequotes)
                  (map cast-int-fields)
                  (map #(assoc % :numberOfReferrals 1)))
        indexed-data (set/index data [:studentReadableId])
        reduced-data (->> (map #(merge-student-records (val %)) indexed-data)
                          (map readable-seqs)
                          )
        ]
    ;; reduced-data
    (maps->csv output-path reduced-data)
    ))
