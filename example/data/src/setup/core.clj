(ns setup.core
  (:require [clojure.tools.cli :as cli]
            [me.raynes.fs :as fs]))

(def models (map #(str "model-" %) (range 1 10)))

(def features [:sites :contexts :zip-codes])

(def negative-predictors {:sites     ["bbc.com", "cnn.com", "news.com", "yahoo.com", "fox.com", "wsj.com"]
                          :contexts  ["news", "politics", "sport", "finance"]
                          :zip-codes ["10001", "11201", "10016", "10015"]})

(def positive-predictors {:sites     ["buzzfeed.com", "wired.com", "gawker.com", "ycombinator.com", "espn.go.com"]
                          :contexts  ["technology", "music", "start-ups", "sport", "health"]
                          :zip-codes ["94101", "94122", "94002", "94006"]})

(defn index-features
  "Compute predictors to index mapping for given features"
  [features neg pos]
  (let [flip (fn [idx itm] [itm idx])
        predictors #(set (concat (%1 neg) (%1 pos)))
        index #(into {} (map-indexed flip (predictors %1)))]
    (into {} (zipmap features (map index features)))))

(defn featurize-predictors
  "Compute featurization for single feature"
  [index predictors]
  (into (sorted-map-by <) (for [[k v] predictors
                                :let [e (find index k)]
                                :when e]
                            [(index k) v])))

(defn base
  [features index]
  (zipmap features (cons 0 (reductions + (map #(count (%1 index)) features)))))

(defn col-names
  [features index]
  (let [base (base features index)]
    (into (sorted-map)
          (->> features
               (mapcat (fn [ft] (->> (ft index)
                                     (map (fn [[val idx]]
                                            [(+ (base ft) idx) (str (name ft) ":" val)])))))))))

(defn featurize
  [features index instance]
  (let [base (base features index)
        rebase (fn [feature] #(into (sorted-map-by <)
                                    (for [[k v] %1] [(+ (feature base) k) v])
                                    ))]
    (zipmap features
            (map (fn [ft] ((rebase ft) (featurize-predictors (ft index) (ft instance))))
                 features))))

(defn gen-instance
  [features pred n resp]
  (let [rand-pred (fn [n kw] (take (inc (rand-int n)) (shuffle (kw pred))))
        rand-map #(into {} (map (fn [k] [k (inc (rand-int n))]) %1))]
    (merge {:response resp}
           (zipmap features (->> features
                                 (map #(rand-pred 3 %))
                                 (map rand-map))))))

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

(defn select-values [map ks]
  (reduce #(conj %1 (map %2)) [] ks))

(defn write-instances [features indexed name instances]
  (let [out-path (str "output/" name)
        merge-features (fn [m] (apply merge (select-values m features)))]
    (println (str "Writing instances to files. Model name = " name))
    (fs/mkdirs out-path)
    (with-open [col-names-o (clojure.java.io/writer (str out-path "/columns.csv"))
                predictors-o (clojure.java.io/writer (str out-path "/predictors.csv"))
                response-o (clojure.java.io/writer (str out-path "/response.csv"))]
      (binding [*out* col-names-o]
        (println "column,name")
        (doseq [[col val] (col-names features indexed)]
          (println (str col "," val))))
      (binding [*out* predictors-o]
        (println "row,column,value")
        (doseq-indexed [idx instance instances]
                       (doseq [[col val] (merge-features (featurize features indexed instance))]
                         (println (str idx "," col "," val)))))
      (binding [*out* response-o]
        (println "row,response")
        (doseq-indexed [idx instance instances]
                       (println (str idx "," (:response instance))))))))

(defn generate-input
  [num-neg num-pos]
  (println (str "Generate model input: num-neg = " num-neg "; num-pos = " num-pos))
  (let [indexed (index-features features negative-predictors positive-predictors)]
    (doseq [model models]
      (let [negs (repeat num-neg (gen-instance features negative-predictors 10 0.0))
            poss (repeat num-pos (gen-instance features positive-predictors 10 1.0))]
        (write-instances features indexed model (shuffle (concat negs poss)))))))

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
