(ns setup.core
  (:require [clojure.tools.cli :as cli]
            [me.raynes.fs :as fs]))

(def models (map #(str "model-" %) (range 1 10)))

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
  (let [take-rand (fn [n kw] (take (inc (rand-int n)) (shuffle (kw pred))))
        sites (take-rand 3 :sites)
        contexts (take-rand 3 :contexts)
        zip-codes (take-rand 2 :zip-codes)
        rand-map #(into {} (map (fn [k] [k (inc (rand-int n))]) %1))]
    {:response  resp
     :sites     (rand-map sites)
     :contexts  (rand-map contexts)
     :zip-codes (rand-map zip-codes)}))

(defmacro doseq-indexed
  "loops over a set of values, binding index-sym to the 0-based index of each value"
  ([[index-sym val-sym values] & code]
   `(loop [vals# (seq ~values)
           ~index-sym (long 0)]
      (if vals#
        (let [~val-sym (first vals#)]
          ~@code
          (recur (next vals#) (inc ~index-sym)))
        nil))))

(defn write-instances [indexed model instances]
  (let [out-path (str "output/" model)]
    (println (str "Writing instances to files. model = " model))
    (fs/mkdirs out-path)
    (with-open [predictors-o (clojure.java.io/writer (str out-path "/predictors.csv"))
                response-o (clojure.java.io/writer (str out-path "/response.csv"))]
      (binding [*out* predictors-o]
        (println "row,column,value")
        (doseq-indexed [idx instance instances]
                       (doseq [[col val] (featurize indexed instance)]
                         (println (str idx "," col "," val)))))
      (binding [*out* response-o]
        (println "row,response")
        (doseq-indexed [idx instance instances]
                       (println (str idx "," (:response instance))))))))

(defn generate-input
  [num-neg num-pos]
  (println (str "Generate model input: num-neg = " num-neg "; num-pos = " num-pos))
  (let [indexed (index-predictors negative-predictors positive-predictors)]
    (doseq [model models]
      (let [negs (repeat num-neg (gen-instance 0.0 10 negative-predictors))
            poss (repeat num-pos (gen-instance 1.0 10 positive-predictors))]
        (write-instances indexed model (shuffle (concat negs poss)))))))

(def cli-options
  [["-g" "--generate" "Generate model input"]
   ["-n" "--num-negatives COUNT" "Number of negatives in each model"
    :default 1000
    :parse-fn #(Long/parseLong %)]
   ["-p" "--num-positives COUNT" "Number of positives in each model"
    :default 9000
    :parse-fn #(Long/parseLong %)]])

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (:generate options) (generate-input (:num-negatives options) (:num-positives options))
      :else (println summary))))
