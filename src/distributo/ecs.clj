(ns distributo.ecs
  (:require [chime :refer [chime-ch]]
            [clojure.core.async :as async :refer [go-loop <! >!! sliding-buffer chan]]
            [clojure.tools.logging :as log]
            [clojure.data :refer [diff]]
            [distributo.util :refer :all])
  (:import (com.amazonaws.services.ecs AmazonECSClient)
           (com.amazonaws.services.ecs.model CreateClusterRequest DescribeClustersRequest DeleteClusterRequest Cluster DescribeTaskDefinitionRequest TaskDefinition ContainerDefinition RegisterTaskDefinitionRequest)
           (com.amazonaws.auth AWSCredentialsProvider)
           (com.amazonaws.regions Region Regions)))

;; Name of default container inside task definition

(def default-container-name "default")

;; Mapping from AWS SDK objects into clojure data structures

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
     }))

(defn container-definition->map
  [^ContainerDefinition container-definition]
  (let [name (-> container-definition (.getName))
        image (-> container-definition (.getImage))
        cpu (-> container-definition (.getCpu))
        memory (-> container-definition (.getMemory))]
    {:name   name
     :image  image
     :cpu    cpu
     :memory memory}))

(defn task-definition->map
  [^TaskDefinition task-definition]
  (let [family (-> task-definition (.getFamily))
        revision (-> task-definition (.getRevision))
        status (-> task-definition (.getStatus))
        container-definitions (-> task-definition (.getContainerDefinitions))]
    {:family                family
     :revision              revision
     :status                (keyword (.toLowerCase status))
     :container-definitions (mapv container-definition->map container-definitions)}))

;; Construct new AWS SDK objects

(defn new-client
  ([^AWSCredentialsProvider cred]
   (AmazonECSClient. cred))
  ([^AWSCredentialsProvider cred region]
   (-> (AmazonECSClient. cred)
       (.setRegion (Region/getRegion (Regions/fromName region))))))

;; AWS SDK ECS Api

(defn create-cluster
  [^AmazonECSClient client cluster]
  (let [ec2-create-request (-> (CreateClusterRequest.)
                               (.withClusterName cluster))]
    (-> client
        (.createCluster ec2-create-request)
        (.getCluster)
        (cluster->map))))

(defn delete-cluster
  [^AmazonECSClient client cluster]
  (let [ec2-delete-request (-> (DeleteClusterRequest.)
                               (.withCluster cluster))]
    (-> client
        (.deleteCluster ec2-delete-request)
        (.getCluster)
        (cluster->map))))

(defn describe-cluster
  [^AmazonECSClient client name]
  (let [ec2-describe-request (-> (DescribeClustersRequest.)
                                 (.withClusters [name]))
        result (-> client (.describeClusters ec2-describe-request))
        cluster (-> result (.getClusters) (seq) (first))
        failure (-> result (.getFailures) (seq) (first))]
    (cond
      (and cluster (not failure)) (cluster->map cluster)
      (and failure (not cluster)) {:failure (-> failure (.getReason))}
      ;; Should not be here
      :else (throw (IllegalStateException. "Cluster is not found nor failed")))))


(defn list-task-definition-families
  [^AmazonECSClient client]
  (-> client
      (.listTaskDefinitionFamilies)
      (.getFamilies)))

(defn describe-task-definition
  [^AmazonECSClient client family]
  (let [ec2-describe-request (-> (DescribeTaskDefinitionRequest.)
                                 (.withTaskDefinition family))]
    (task-definition->map
      (-> client
          (.describeTaskDefinition ec2-describe-request)
          (.getTaskDefinition)))))

(defn register-task-definition
  [^AmazonECSClient client family opts]
  (let [{:keys [image cpu memory]} opts
        ec2-container-def (-> (ContainerDefinition.)
                              (.withImage image)
                              (.withCpu (int cpu))
                              (.withMemory (int memory))
                              (.withName default-container-name))
        ec2-register-request (-> (RegisterTaskDefinitionRequest.)
                                 (.withFamily family)
                                 (.withContainerDefinitions [ec2-container-def]))]
    (log/debug "Register task definition. Family:" family "CPU:" cpu "Mem:" memory)
    (task-definition->map
      (-> client
          (.registerTaskDefinition ec2-register-request)
          (.getTaskDefinition)))))

;; Combinators on top of AWS API

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
    (async/pipeline-blocking 1 transitions xf metronome)
    transitions))
