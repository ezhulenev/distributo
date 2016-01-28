(ns distributo.util
  (:require [clojure.data :refer [diff]]
            [clojure.core.async :as async]
            [clj-time.core :as t]
            [clj-time.periodic :as periodic]
            [chime :refer [chime-ch]])
  (:import (clojure.lang PersistentQueue)))

(defn merge-common-with [f a b]
  (persistent!
    (reduce-kv (fn [out k v]
                 (if (contains? b k)
                   (assoc! out k (f (get a k) (get b k)))
                   out))
               (transient {})
               a)))

(defn roll-buffer [buffer val buffer-size]
  (let [buffer (conj buffer val)]
    (if (> (count buffer) buffer-size)
      (pop buffer)
      buffer)))

(defn make-sliding-window
  [buffer-size]
  (let [buffer (atom PersistentQueue/EMPTY)]
    (fn [val]
      (swap! buffer roll-buffer val buffer-size))))

(defn metronome
  "Core.async channel production events every n seconds"
  [^long n]
  (let [schedule (periodic/periodic-seq (t/now) (-> n t/seconds))]
    (chime-ch schedule)))

(defn nil-map [keys]
  "Creates map with keys and nil as values"
  (into {} (map (fn [k] [k nil]) keys)))

(defn remove-nil-values [m]
  "Remove keys with nil values"
  (into {} (remove (comp nil? second) m)))

(defn transition
  "Use data.diff to compute transition map from state a to state b"
  [a b]
  (let [[l r _] (diff a b)
        all-keys (concat (keys l) (keys r))
        nil-map (nil-map all-keys)]
    (merge-with
      (fn [from to] (remove-nil-values {:from from :to to}))
      (merge nil-map l)
      (merge nil-map r))))
