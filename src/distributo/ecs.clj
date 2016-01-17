(ns distributo.ecs
  (:require [chime :refer [chime-ch]]
            [clojure.core.async :as async :refer [go-loop <! >!! sliding-buffer chan]]
            [clojure.tools.logging :as log]
            [clojure.data :refer [diff]]
            [distributo.util :refer :all])
  (:import (com.amazonaws.services.ecs AmazonECSClient)
           (com.amazonaws.services.ecs.model CreateClusterRequest DescribeClustersRequest DeleteClusterRequest Cluster)
           (com.amazonaws.auth AWSCredentialsProvider)
           (com.amazonaws.regions Region Regions)))


(defn new-client
  ([^AWSCredentialsProvider cred]
   (AmazonECSClient. cred))
  ([^AWSCredentialsProvider cred region]
   (-> (AmazonECSClient. cred)
       (.setRegion (Region/getRegion (Regions/fromName region))))))

(defn cluster->map
  [^Cluster cluster]
  (let [name (-> cluster (.getClusterName))
        status (-> cluster (.getStatus) (.toLowerCase) (keyword))
        instances (-> cluster (.getRegisteredContainerInstancesCount))
        running-tasks (-> cluster (.getRunningTasksCount))
        pending-tasks (-> cluster (.getPendingTasksCount))
        active-services (-> cluster (.getActiveServicesCount))]
    {:name            name
     :status          status
     :instances       instances
     :running-tasks   running-tasks
     :pending-tasks   pending-tasks
     :active-services active-services
     })
  )

(defn create-cluster
  "Create new ECS cluster"
  [^AmazonECSClient client name]
  (-> (.createCluster client (-> (CreateClusterRequest.)
                                 (.withClusterName name)))
      (.getCluster)
      (cluster->map)))

(defn delete-cluster
  "Delete cluster"
  [^AmazonECSClient client name]
  (-> (.deleteCluster client (-> (DeleteClusterRequest.)
                                 (.withCluster name)))
      (.getCluster)
      (cluster->map)))

(defn describe-cluster
  [^AmazonECSClient client name]
  (let [result (.describeClusters client (-> (DescribeClustersRequest.)
                                             (.withClusters [name])))
        cluster (-> result (.getClusters) (seq) (first))
        failure (-> result (.getFailures) (seq) (first))]
    (cond
      (and cluster (not failure)) (cluster->map cluster)
      (and failure (not cluster)) {:failure (-> failure (.getReason))}
      ;; Should not be here
      :else (throw (IllegalStateException. "Cluster is not found nor failed")))))

(defn cluster-transition
  "Compute cluster state transition based on diff"
  [a b]
  {:pre (= (:name a) (:name b))}
  (let [[l r _] (diff a b)]
    (merge {:name (:name a)} (transition a b))))

(defn cluster-states-chan
  "Constructs core.async channel with cluster states"
  [^AmazonECSClient client metronome name]
  (let [states (chan)
        xf (map (fn [_] (describe-cluster client name)))]
    (async/pipeline 1 states xf metronome)
    states))

(defn cluster-transitions-chan
  "Constructs core.async channel with cluster state transitions"
  [^AmazonECSClient client metronome name]
  (let [transitions (chan)
        sliding-window (make-sliding-window 2)
        xf (comp
             (map (fn [_] (describe-cluster client name)))
             (map (fn [state] (seq (sliding-window state))))
             (filter #(= 2 (count %)))
             (map (fn [[a b]] (cluster-transition a b))))]
    (async/pipeline 1 transitions xf metronome)
    transitions))
