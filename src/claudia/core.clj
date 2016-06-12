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

(def problem-behaviors
  {"Alcohol"                     "AlcoholTobaccoDrugs"         
   "Arson"                       "ArsonBombWeapons"            
   "Bomb"                        "ArsonBombWeapons"            
   "Bomb, Prop dam"              "ArsonBombWeapons"            
   "Bullying"                    "BullyingHarass"              
   "Bullying, Defiance"          "HIToppositional"             
   "Bullying, Disruption"        "BullyingHarass"              
   "Bullying, Harass"            "BullyingHarass"              
   "Bullying, Out Bounds"        "BullyingHarass"              
   "Defiance"                    "HIToppositional"             
   "Defiance, Disrespect"        "HIToppositional"             
   "Defiance, Disruption"        "HIToppositional"             
   "Defiance, Harass"            "HIToppositional"             
   "Defiance, Inapp affection"   "HIToppositional"             
   "Defiance, Inapp Lan"         "HIToppositional"             
   "Defiance, Lying"             "HIToppositional"             
   "Defiance, Other"             "HIToppositional"             
   "Defiance, Out Bounds"        "HIToppositional"             
   "Defiance, PAgg"              "HITphysical"                 
   "Defiance, Prop dam"          "HIToppositional"             
   "Disrespect"                  "DisrespectDisruptionInappLan"
   "Disrespect, Defiance"        "HIToppositional"             
   "Disrespect, Disruption"      "DisrespectDisruptionInappLan"
   "Disrespect, Dress"           "DisrespectDisruptionInappLan"
   "Disrespect, Gang Display"    "GangDisplay"                 
   "Disrespect, Harass"          "BullyingHarass"              
   "Disrespect, Inapp Lan"       "DisrespectDisruptionInappLan"
   "Disrespect, Lying"           "HITlying"                    
   "Disrespect, Other"           "DisrespectDisruptionInappLan"
   "Disrespect, Out Bounds"      "DisrespectDisruptionInappLan"
   "Disrespect, PAgg"            "HITphysical"                 
   "Disrespect, Prop dam"        "PropDamage"                  
   "Disruption"                  "DisrespectDisruptionInappLan"
   "Disruption, Bullying"        "BullyingHarass"              
   "Disruption, Defiance"        "HIToppositional"             
   "Disruption, Disrespect"      "DisrespectDisruptionInappLan"
   "Disruption, Gang Display"    "GangDisplay"                 
   "Disruption, Inapp Lan"       "DisrespectDisruptionInappLan"
   "Disruption, Other"           "DisrespectDisruptionInappLan"
   "Disruption, Out Bounds"      "DisrespectDisruptionInappLan"
   "Disruption, PAgg"            "HITphysical"                 
   "Dress"                       "OtherDressTech"              
   "Dress, Defiance"             "HIToppositional"             
   "Dress, Disrespect"           "DisrespectDisruptionInappLan"
   "Drugs"                       "AlcoholTobaccoDrugs"         
   "Drugs, PAgg"                 "HITphysical"                 
   "Fight"                       "HITphysical"                 
   "Gang Display"                "GangDisplay"                 
   "Gang Display, Disruption"    "GangDisplay"                 
   "Gang Display, Fight"         "HITphysical"                 
   "Gang Display, Inapp Lan"     "GangDisplay"                 
   "Harass"                      "BullyingHarass"              
   "Harass, Disrespect"          "BullyingHarass"              
   "Harass, Disruption"          "BullyingHarass"              
   "Harass, Out Bounds"          "BullyingHarass"              
   "Inapp affection"             "InappAffection"              
   "Inapp affection, Defiance"   "HIToppositional"             
   "Inapp affection, Disruption" "DisrespectDisruptionInappLan"
   "Inapp affection, Harass"     "BullyingHarass"              
   "Inapp affection, Inapp Lan"  "DisrespectDisruptionInappLan"
   "Inapp Lan"                   "DisrespectDisruptionInappLan"
   "Inapp Lan, Defiance"         "HIToppositional"             
   "Inapp Lan, Disrespect"       "DisrespectDisruptionInappLan"
   "Inapp Lan, Disruption"       "DisrespectDisruptionInappLan"
   "Inapp Lan, Harass"           "BullyingHarass"              
   "Inapp Lan, Inapp affection"  "DisrespectDisruptionInappLan"
   "Inapp Lan, Other"            "DisrespectDisruptionInappLan"
   "Inapp Lan, Out Bounds"       "DisrespectDisruptionInappLan"
   "Inapp Lan, PAgg"             "HITphysical"                 
   "Inapp Lan, Prop dam"         "PropDamage"                  
   "Lying"                       "HITlying"                    
   "Lying, Disrespect"           "HITlying"                    
   "Lying, Out Bounds"           "HITlying"                    
   "M-Disrespect, M-Inapp Lan"   "DisrespectDisruptionInappLan"
   "Other"                       "OtherDressTech"              
   "Other, Defiance"             "HIToppositional"             
   "Other, Disrespect"           "DisrespectDisruptionInappLan"
   "Other, Inapp Lan"            "DisrespectDisruptionInappLan"
   "Other, Out Bounds"           "OutBounds"                   
   "Other, Prop dam"             "PropDamage"                  
   "Out Bounds"                  "OutBounds"                   
   "Out Bounds, Bullying"        "BullyingHarass"              
   "Out Bounds, Defiance"        "HIToppositional"             
   "Out Bounds, Disrespect"      "DisrespectDisruptionInappLan"
   "Out Bounds, Disruption"      "DisrespectDisruptionInappLan"
   "Out Bounds, Fight"           "HITphysical"                 
   "Out Bounds, Gang Display"    "GangDisplay"                 
   "Out Bounds, Harass"          "BullyingHarass"              
   "Out Bounds, Inapp affection" "InappAffection"              
   "Out Bounds, Inapp Lan"       "DisrespectDisruptionInappLan"
   "Out Bounds, Other"           "OutBounds"                   
   "Out Bounds, PAgg"            "HITphysical"                 
   "Out Bounds, Prop dam"        "PropDamage"                  
   "PAgg"                        "HITphysical"                 
   "PAgg, Defiance"              "HITphysical"                 
   "PAgg, Disrespect"            "HITphysical"                 
   "PAgg, Disruption"            "HITphysical"                 
   "PAgg, Fight"                 "HITphysical"                 
   "PAgg, Harass"                "HITphysical"                 
   "PAgg, Inapp Lan"             "HITphysical"                 
   "PAgg, Other"                 "HITphysical"                 
   "PAgg, Out Bounds"            "HITphysical"                 
   "Prop dam"                    "PropDamage"                  
   "Prop dam, Defiance"          "HIToppositional"             
   "Prop dam, Disruption"        "PropDamage"                  
   "Prop dam, Inapp Lan"         "PropDamage"                  
   "Prop dam, Out Bounds"        "PropDamage"                  
   "Tech"                        "OtherDressTech"              
   "Theft"                       "HITstealing"                 
   "Theft, Lying"                "HITstealing"                 
   "Theft, Out Bounds"           "HITstealing"                 
   "Tobacco"                     "AlcoholTobaccoDrugs"         
   "Weapons"                     "ArsonBombWeapons"})

(defn do-it []
  (let [output-path "/tmp/wip-swis.csv"
        data (->> "resources/data.tsv"
                  (csv->clj)
                  (map #(select-keys % desired-columns))
                  (map #(reduce-kv (fn [m k v]
                                     (assoc m k
                                            (if (some #{k} '(:problemBehaviors :actionsTaken))
                                              (str/replace v #"\"" "")
                                              v)))
                                   {}
                                   %)) ; clean columns
                  (map #(into (sorted-map-by (fn [x y]
                                               (let [x-index (.indexOf desired-columns x)
                                                     y-index (.indexOf desired-columns y)
                                                     x-normalized (if (= -1 x-index)
                                                                    Integer/MAX_VALUE
                                                                    x-index)
                                                     y-normalized (if (= -1 y-index)
                                                                    Integer/MAX_VALUE
                                                                    y-index)]
                                                 (< x-normalized y-normalized)))) %)) ; sort each row
                  )
        ]
    ;; (maps->csv output-path data)

    ;; (comment
      ;; (->>
      ;;  (map :actionsTaken data)
      ;;  distinct
      ;;  sort
      ;;  pprint
      ;;  )
      (nth data 7)
      ;; )
    ))

