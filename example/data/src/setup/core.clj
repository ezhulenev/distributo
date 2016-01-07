(ns setup.core
  (:require [clojure.tools.cli :as cli]))

(def negative-predictors {:sites     ["bbc.com", "cnn.com", "news.com", "yahoo.com", "fox.com", "wsj.com"]
                          :contexts  ["news", "politics", "sport", "finance"]
                          :zip-codes ["10001", "11201", "10016", "10015"]})

(def positive-predictors {:sites     ["buzzfeed.com", "wired.com", "gawker.com", "ycombinator.com", "espn.go.com"]
                          :contexts  ["technology", "music", "start-ups", "sport", "health"]
                          :zip-codes ["94101", "94122", "94002", "94006"]})

(defn index-predictors
  [neg pos]
  (let [flip (fn [idx itm] [itm idx])
        all-predictors #(concat (%1 neg) (%1 pos))
        indexed #(into {} (map-indexed flip (set (all-predictors %1))))]
    {:sites     (indexed :sites)
     :contexts  (indexed :contexts)
     :zip-codes (indexed :zip-codes)}))


(defn- feauturize-predictors
  [idx p]
  (into (sorted-map-by <) (for [[k v] p
                                :let [e (find idx k)]
                                :when e]
                            [(idx k) v])))

(defn featurize
  [idx p]
  (let [extract #(feauturize-predictors (%1 idx) (%1 p))
        num-sites (count (:sites idx))
        num-contexts (count (:contexts idx))
        sites (extract :sites)
        contexts (extract :contexts)
        zip-codes (extract :zip-codes)
        rebase (fn [base] #(into (sorted-map-by <)
                                 (for [[k v] %1] [(+ base k) v])
                                 ))]
    (merge
      ((rebase 0) sites)
      ((rebase num-sites) contexts)
      ((rebase (+ num-sites num-contexts)) zip-codes))))

(defn gen-instance
  [resp n pred]
  (let [take-rand (fn [n kw] (take (rand-int n) (shuffle (kw pred))))
        sites (take-rand 3 :sites)
        contexts (take-rand 3 :contexts)
        zip-codes (take-rand 2 :zip-codes)
        rand-map #(into {} (map (fn [k] [k (+ 1 (rand-int n))]) %1))]
    {:response  resp
     :sites     (rand-map sites)
     :contexts  (rand-map contexts)
     :zip-codes (rand-map zip-codes)}))

;(defn generate-input
;  [neg pos]
;  (println (str "Generate Input: " params)))
;
;(def cli-options
;  [["-g" "--generate" "Generate model input"]])
;
;(defn -main
;  [& args]
;  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
;    (cond
;      (:generate options) (generate-input options)
;      :else                (println summary))))
