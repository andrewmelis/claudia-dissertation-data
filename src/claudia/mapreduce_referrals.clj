(ns claudia.mapreduce-referrals
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]])
  (:gen-class))

(defn load-key-value-tsv [filename]
  (with-open [rdr (io/reader filename)]
    (->> (line-seq rdr)
         (map #(str/split % #"\t"))
         (flatten)
         (apply assoc {}))))

(defn load-tsv [filename]
  (with-open [rdr (io/reader filename)]
    (let [raw-headers (first (line-seq rdr))
          headers (map keyword (str/split raw-headers #"\t"))
          raw-data (rest (line-seq rdr))
          data (doall (map #(str/split % #"\t") raw-data))]
      (map #(zipmap headers %) data))))

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
  [problem-behaviors m]
  (let [problem-behavior-category (get problem-behaviors (:problemBehaviors m))]
    (update m problem-behavior-category inc)))

(defn hydrate-referral-event-records
  ;; adds counters for each category
  ;; "map" stage
  ;; ie "hydrate" referral event records
  ;; accepts sequence of referral maps
  [xs problem-behaviors]
  (->> xs
       (map #(select-keys % desired-columns)) ;; only keep desired columns
       (map #(reduce-kv (fn [m k v]
                          (assoc m k
                                 (if (some #{k} '(:problemBehaviors :actionsTaken))
                                   (str/replace v #"\"" "")
                                   v)))
                        {}
                        %)) ;; clean up quotation marks in data
       (map #(seed-keys (distinct (vals problem-behaviors)) %)) ;; seed each record
       (map (partial increment-problem-behavior problem-behaviors)) ;; differentiate each record
       (map #(assoc % :numberOfReferrals 1))))

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

(defn process [raw-data problem-behaviors]
  (let [hydrated-data (hydrate-referral-event-records raw-data problem-behaviors)
        indexed-data (set/index hydrated-data [:studentReadableId])
        reduced-data (map #(merge-student-records (val %)) indexed-data)
        post-processed-data (map readable-seqs reduced-data)]
    post-processed-data))

(defn -main
  [problem-behavior-mapping-path raw-data-path output-path]
  (let [problem-behaviors (load-key-value-tsv (or problem-behavior-mapping-path
                                                  "resources/input/default-problem-behavior-mapping.tsv"))
        raw-data (load-tsv (or raw-data-path
                               "resources/input/raw-referral-data.tsv"))
        output-path (or output-path
                        "resources/output/referral-output.csv")]
    (maps->csv output-path
               (process raw-data problem-behaviors))))
