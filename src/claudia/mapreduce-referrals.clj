(ns claudia.mapreduce-referrals
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]))


(defn load-tsv [filename]
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

;; get rid of all columns except these
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

(defn seed-keys
  ;; "initalize" each possible problem behavior category to 0
  ;; standardizes each record to have a key for each possible behavior
  ;; enables increment of actual problem behavior later
  ;; then easy reduction of all referral maps by summing problem behavior keys
  [keys m]
  (apply assoc m (interleave keys (repeat 0))))

(defn increment-problem-behavior
  ;; increment the key for the actual problem behavior recorded for the referral
  [m]
  (let [problem-behavior-category (get problem-behaviors (:problemBehaviors m))]
    (update m problem-behavior-category inc)))

(defn hydrate-referral-event-records
  ;; adds counters for each category
  ;; "map" stage
  ;; ie "hydrate" referral event records
  ;; accepts sequence of referral maps
  [xs]
  (->> xs
       (map #(select-keys % desired-columns)) ;; only keep desired columns
       (map #(reduce-kv (fn [m k v]
                          (assoc m k
                                 (if (some #{k} '(:problemBehaviors :actionsTaken))
                                   (str/replace v #"\"" "")
                                   v)))
                        {}
                        %)) ;; clean up quotation marks in data
       (map #(assoc % :numberOfReferrals 1))
       (map #(seed-keys (distinct (vals problem-behaviors)) %)) ;; seed each record
       (map increment-problem-behavior))) ;; differentiate each record

(defn map-stage []
  (let [output-path "/tmp/claudia-v3-wip/map-swis.csv"
        raw-data (load-tsv "resources/raw-referral-data.tsv")
        hydrated-data (hydrate-referral-event-records raw-data)]
    (maps->csv output-path hydrated-data)))
