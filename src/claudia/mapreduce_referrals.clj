(ns claudia.mapreduce-referrals
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]])
  (:gen-class))


(defn load-tsv [filename]
  (with-open [rdr (io/reader filename)]
    (let [raw-headers (first (line-seq rdr))
          headers (map keyword (str/split raw-headers #"\t"))
          raw-data (rest (line-seq rdr))
          data (doall (map #(str/split % #"\t") raw-data))]

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

(def problem-behaviors
  {
   "Alcohol"				"Alcohol"
   "Arson"				"Arson"
   "Bomb"				"Bomb"
   "Bomb, Prop dam"			"Bomb"
   "Bullying"				"Bullying"
   "Bullying, Defiance"			"Bullying"
   "Bullying, Disruption"		"Bullying"
   "Bullying, Harass"			"Bullying"
   "Bullying, Out Bounds"		"Bullying"
   "Defiance"				"Defiance"
   "Defiance, Disrespect"		"Defiance"
   "Defiance, Disruption"		"Defiance"
   "Defiance, Harass"			"Harassment"
   "Defiance, Inapp affection"		"Defiance"
   "Defiance, Inapp Lan"		"Defiance"
   "Defiance, Lying"			"Lying"
   "Defiance, Other"			"Defiance"
   "Defiance, Out Bounds"		"Defiance"
   "Defiance, PAgg"			"PhysicalAgg"
   "Defiance, Prop dam"			"Defiance"
   "Disrespect"				"DisrespectDisruptionInappLan"
   "Disrespect, Defiance"		"Defiance"
   "Disrespect, Disruption"		"DisrespectDisruptionInappLan"
   "Disrespect, Dress"			"DisrespectDisruptionInappLan"
   "Disrespect, Gang Display"		"GangDisplay"
   "Disrespect, Harass"			"Harassment"
   "Disrespect, Inapp Lan"		"DisrespectDisruptionInappLan"
   "Disrespect, Lying"			"Lying"
   "Disrespect, Other"			"DisrespectDisruptionInappLan"
   "Disrespect, Out Bounds"		"DisrespectDisruptionInappLan"
   "Disrespect, PAgg"			"PhysicalAgg"
   "Disrespect, Prop dam"		"PropDamage"
   "Disruption"				"DisrespectDisruptionInappLan"
   "Disruption, Bullying"		"Bullying"
   "Disruption, Defiance"		"Defiance"
   "Disruption, Disrespect"		"DisrespectDisruptionInappLan"
   "Disruption, Gang Display"		"GangDisplay"
   "Disruption, Inapp Lan"		"DisrespectDisruptionInappLan"
   "Disruption, Other"			"DisrespectDisruptionInappLan"
   "Disruption, Out Bounds"		"DisrespectDisruptionInappLan"
   "Disruption, PAgg"			"PhysicalAgg"
   "Dress"				"OtherDressTech"
   "Dress, Defiance"			"Defiance"
   "Dress, Disrespect"			"DisrespectDisruptionInappLan"
   "Drugs"				"Drugs"
   "Drugs, PAgg"			"PhysicalAgg"
   "Fight"				"Fight"
   "Gang Display"			"GangDisplay"
   "Gang Display, Disruption"		"GangDisplay"
   "Gang Display, Fight"		"Fight"
   "Gang Display, Inapp Lan"		"GangDisplay"
   "Harass"				"Harassment"
   "Harass, Disrespect"			"Harassment"
   "Harass, Disruption"			"Harassment"
   "Harass, Out Bounds"			"Harassment"
   "Inapp affection"			"InappAffection"
   "Inapp affection, Defiance"		"Defiance"
   "Inapp affection, Disruption"	"DisrespectDisruptionInappLan"
   "Inapp affection, Harass"		"Harassment"
   "Inapp affection, Inapp Lan"		"DisrespectDisruptionInappLan"
   "Inapp Lan"				"DisrespectDisruptionInappLan"
   "Inapp Lan, Defiance"		"Defiance"
   "Inapp Lan, Disrespect"		"DisrespectDisruptionInappLan"
   "Inapp Lan, Disruption"		"DisrespectDisruptionInappLan"
   "Inapp Lan, Harass"			"Harassment"
   "Inapp Lan, Inapp affection"		"DisrespectDisruptionInappLan"
   "Inapp Lan, Other"			"DisrespectDisruptionInappLan"
   "Inapp Lan, Out Bounds"		"DisrespectDisruptionInappLan"
   "Inapp Lan, PAgg"			"PhysicalAgg"
   "Inapp Lan, Prop dam"		"PropDamage"
   "Lying"				"Lying"
   "Lying, Disrespect"			"Lying"
   "Lying, Out Bounds"			"Lying"
   "M-Disrespect, M-Inapp Lan"		"DisrespectDisruptionInappLan"
   "Other"				"OtherDressTech"
   "Other, Defiance"			"Defiance"
   "Other, Disrespect"			"DisrespectDisruptionInappLan"
   "Other, Inapp Lan"			"DisrespectDisruptionInappLan"
   "Other, Out Bounds"			"OutBounds"
   "Other, Prop dam"			"PropDamage"
   "Out Bounds"				"OutBounds"
   "Out Bounds, Bullying"		"Bullying"
   "Out Bounds, Defiance"		"Defiance"
   "Out Bounds, Disrespect"		"DisrespectDisruptionInappLan"
   "Out Bounds, Disruption"		"DisrespectDisruptionInappLan"
   "Out Bounds, Fight"			"Fight"
   "Out Bounds, Gang Display"		"GangDisplay"
   "Out Bounds, Harass"			"Harassment"
   "Out Bounds, Inapp affection"	"InappAffection"
   "Out Bounds, Inapp Lan"		"DisrespectDisruptionInappLan"
   "Out Bounds, Other"			"OutBounds"
   "Out Bounds, PAgg"			"PhysicalAgg"
   "Out Bounds, Prop dam"		"PropDamage"
   "PAgg"				"PhysicalAgg"
   "PAgg, Defiance"			"PhysicalAgg"
   "PAgg, Disrespect"			"PhysicalAgg"
   "PAgg, Disruption"			"PhysicalAgg"
   "PAgg, Fight"			"Fight"
   "PAgg, Harass"			"PhysicalAgg"
   "PAgg, Inapp Lan"			"PhysicalAgg"
   "PAgg, Other"			"PhysicalAgg"
   "PAgg, Out Bounds"			"PhysicalAgg"
   "Prop dam"				"PropDamage"
   "Prop dam, Defiance"			"Defiance"
   "Prop dam, Disruption"		"PropDamage"
   "Prop dam, Inapp Lan"		"PropDamage"
   "Prop dam, Out Bounds"		"PropDamage"
   "Tech"				"OtherDressTech"
   "Theft"				"Stealing"
   "Theft, Lying"			"Stealing"
   "Theft, Out Bounds"			"Stealing"
   "Tobacco"				"Tobacco"
   "Weapons"				"Weapons"
})

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
       (map #(seed-keys (distinct (vals problem-behaviors)) %)) ;; seed each record
       (map increment-problem-behavior)
       (map #(assoc % :numberOfReferrals 1)))) ;; differentiate each record

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

(defn process []
  (let [output-path "/tmp/claudia-v3-wip/NEW-reduced-swis.csv"
        raw-data (load-tsv "resources/raw-referral-data.tsv")
        hydrated-data (hydrate-referral-event-records raw-data)
        indexed-data (set/index hydrated-data [:studentReadableId])
        reduced-data (map #(merge-student-records (val %)) indexed-data)
        post-processed-data (map readable-seqs reduced-data)]
    (maps->csv output-path post-processed-data)))

(defn -main [& args]
  (process))
