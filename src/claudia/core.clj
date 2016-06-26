(ns claudia.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]))

;; technically this is a tsv not a csv
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

;; TODO: extract these, mapping to be passed as argument
(def problem-behaviors
  {
   "Alcohol"                     "pbAlcoholTobaccoDrugs"         
   "Arson"                       "pbArsonBombWeapons"            
   "Bomb"                        "pbArsonBombWeapons"            
   "Bomb, Prop dam"              "pbArsonBombWeapons"            
   "Bullying"                    "pbBullyingHarass"              
   "Bullying, Defiance"          "pbHIToppositional"             
   "Bullying, Disruption"        "pbBullyingHarass"              
   "Bullying, Harass"            "pbBullyingHarass"              
   "Bullying, Out Bounds"        "pbBullyingHarass"              
   "Defiance"                    "pbHIToppositional"             
   "Defiance, Disrespect"        "pbHIToppositional"             
   "Defiance, Disruption"        "pbHIToppositional"             
   "Defiance, Harass"            "pbHIToppositional"             
   "Defiance, Inapp affection"   "pbHIToppositional"             
   "Defiance, Inapp Lan"         "pbHIToppositional"             
   "Defiance, Lying"             "pbHIToppositional"             
   "Defiance, Other"             "pbHIToppositional"             
   "Defiance, Out Bounds"        "pbHIToppositional"             
   "Defiance, PAgg"              "pbHITphysical"                 
   "Defiance, Prop dam"          "pbHIToppositional"             
   "Disrespect"                  "pbDisrespectDisruptionInappLan"
   "Disrespect, Defiance"        "pbHIToppositional"             
   "Disrespect, Disruption"      "pbDisrespectDisruptionInappLan"
   "Disrespect, Dress"           "pbDisrespectDisruptionInappLan"
   "Disrespect, Gang Display"    "pbGangDisplay"                 
   "Disrespect, Harass"          "pbBullyingHarass"              
   "Disrespect, Inapp Lan"       "pbDisrespectDisruptionInappLan"
   "Disrespect, Lying"           "pbHITlying"                    
   "Disrespect, Other"           "pbDisrespectDisruptionInappLan"
   "Disrespect, Out Bounds"      "pbDisrespectDisruptionInappLan"
   "Disrespect, PAgg"            "pbHITphysical"                 
   "Disrespect, Prop dam"        "pbPropDamage"                  
   "Disruption"                  "pbDisrespectDisruptionInappLan"
   "Disruption, Bullying"        "pbBullyingHarass"              
   "Disruption, Defiance"        "pbHIToppositional"             
   "Disruption, Disrespect"      "pbDisrespectDisruptionInappLan"
   "Disruption, Gang Display"    "pbGangDisplay"                 
   "Disruption, Inapp Lan"       "pbDisrespectDisruptionInappLan"
   "Disruption, Other"           "pbDisrespectDisruptionInappLan"
   "Disruption, Out Bounds"      "pbDisrespectDisruptionInappLan"
   "Disruption, PAgg"            "pbHITphysical"                 
   "Dress"                       "pbOtherDressTech"              
   "Dress, Defiance"             "pbHIToppositional"             
   "Dress, Disrespect"           "pbDisrespectDisruptionInappLan"
   "Drugs"                       "pbAlcoholTobaccoDrugs"         
   "Drugs, PAgg"                 "pbHITphysical"                 
   "Fight"                       "pbHITphysical"                 
   "Gang Display"                "pbGangDisplay"                 
   "Gang Display, Disruption"    "pbGangDisplay"                 
   "Gang Display, Fight"         "pbHITphysical"                 
   "Gang Display, Inapp Lan"     "pbGangDisplay"                 
   "Harass"                      "pbBullyingHarass"              
   "Harass, Disrespect"          "pbBullyingHarass"              
   "Harass, Disruption"          "pbBullyingHarass"              
   "Harass, Out Bounds"          "pbBullyingHarass"              
   "Inapp affection"             "pbInappAffection"              
   "Inapp affection, Defiance"   "pbHIToppositional"             
   "Inapp affection, Disruption" "pbDisrespectDisruptionInappLan"
   "Inapp affection, Harass"     "pbBullyingHarass"              
   "Inapp affection, Inapp Lan"  "pbDisrespectDisruptionInappLan"
   "Inapp Lan"                   "pbDisrespectDisruptionInappLan"
   "Inapp Lan, Defiance"         "pbHIToppositional"             
   "Inapp Lan, Disrespect"       "pbDisrespectDisruptionInappLan"
   "Inapp Lan, Disruption"       "pbDisrespectDisruptionInappLan"
   "Inapp Lan, Harass"           "pbBullyingHarass"              
   "Inapp Lan, Inapp affection"  "pbDisrespectDisruptionInappLan"
   "Inapp Lan, Other"            "pbDisrespectDisruptionInappLan"
   "Inapp Lan, Out Bounds"       "pbDisrespectDisruptionInappLan"
   "Inapp Lan, PAgg"             "pbHITphysical"                 
   "Inapp Lan, Prop dam"         "pbPropDamage"                  
   "Lying"                       "pbHITlying"                    
   "Lying, Disrespect"           "pbHITlying"                    
   "Lying, Out Bounds"           "pbHITlying"                    
   "M-Disrespect, M-Inapp Lan"   "pbDisrespectDisruptionInappLan"
   "Other"                       "pbOtherDressTech"              
   "Other, Defiance"             "pbHIToppositional"             
   "Other, Disrespect"           "pbDisrespectDisruptionInappLan"
   "Other, Inapp Lan"            "pbDisrespectDisruptionInappLan"
   "Other, Out Bounds"           "pbOutBounds"                   
   "Other, Prop dam"             "pbPropDamage"                  
   "Out Bounds"                  "pbOutBounds"                   
   "Out Bounds, Bullying"        "pbBullyingHarass"              
   "Out Bounds, Defiance"        "pbHIToppositional"             
   "Out Bounds, Disrespect"      "pbDisrespectDisruptionInappLan"
   "Out Bounds, Disruption"      "pbDisrespectDisruptionInappLan"
   "Out Bounds, Fight"           "pbHITphysical"                 
   "Out Bounds, Gang Display"    "pbGangDisplay"                 
   "Out Bounds, Harass"          "pbBullyingHarass"              
   "Out Bounds, Inapp affection" "pbInappAffection"              
   "Out Bounds, Inapp Lan"       "pbDisrespectDisruptionInappLan"
   "Out Bounds, Other"           "pbOutBounds"                   
   "Out Bounds, PAgg"            "pbHITphysical"                 
   "Out Bounds, Prop dam"        "pbPropDamage"                  
   "PAgg"                        "pbHITphysical"                 
   "PAgg, Defiance"              "pbHITphysical"                 
   "PAgg, Disrespect"            "pbHITphysical"                 
   "PAgg, Disruption"            "pbHITphysical"                 
   "PAgg, Fight"                 "pbHITphysical"                 
   "PAgg, Harass"                "pbHITphysical"                 
   "PAgg, Inapp Lan"             "pbHITphysical"                 
   "PAgg, Other"                 "pbHITphysical"                 
   "PAgg, Out Bounds"            "pbHITphysical"                 
   "Prop dam"                    "pbPropDamage"                  
   "Prop dam, Defiance"          "pbHIToppositional"             
   "Prop dam, Disruption"        "pbPropDamage"                  
   "Prop dam, Inapp Lan"         "pbPropDamage"                  
   "Prop dam, Out Bounds"        "pbPropDamage"                  
   "Tech"                        "pbOtherDressTech"              
   "Theft"                       "pbHITstealing"                 
   "Theft, Lying"                "pbHITstealing"                 
   "Theft, Out Bounds"           "pbHITstealing"                 
   "Tobacco"                     "pbAlcoholTobaccoDrugs"         
   "Weapons"                     "pbArsonBombWeapons"
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
       (map increment-problem-behavior))) ;; differentiate each record 

(defn map-stage []
  (let [output-path "/tmp/claudia-v2/v2-map-swis.csv"
        raw-data (csv->clj "resources/raw-referral-data.tsv")
        hydrated-data (hydrate-referral-event-records raw-data)]
    (maps->csv output-path hydrated-data)))
