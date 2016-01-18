(ns distributo.ecs
  (:require [chime :refer [chime-ch]]
            [clojure.core.async :as async :refer [go-loop <! >!! sliding-buffer chan]]
            [clojure.tools.logging :as log]
            [clojure.data :refer [diff]]
            [distributo.util :refer :all])
  (:import (com.amazonaws.services.ecs AmazonECSClient)
           (com.amazonaws.services.ecs.model CreateClusterRequest DescribeClustersRequest DeleteClusterRequest Cluster DescribeTaskDefinitionRequest TaskDefinition ContainerDefinition RegisterTaskDefinitionRequest ListContainerInstancesRequest DescribeContainerInstancesRequest ContainerInstance Resource Failure)
           (com.amazonaws.auth AWSCredentialsProvider)
           (com.amazonaws.regions Region Regions)))

;; Name of default container inside task definition

(def default-container-name "default")

;; Mapping from AWS SDK objects into clojure data structures

(defn cluster->map
  [^Cluster cluster]
  (let [name (-> cluster (.getClusterName))
        status (-> cluster (.getStatus) (keyword))
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
     :status                (keyword status)
     :container-definitions (mapv container-definition->map container-definitions)}))

(defn failure->map
  [^Failure failure]
  (let [arn (-> failure (.getArn))
        reason (-> failure (.getReason))]
    {:arn    arn
     :reason reason}))

(defn resource->map
  [^Resource resource]
  (let [name (-> resource (.getName) (keyword))
        type (-> resource (.getType) (keyword))
        value (condp = type
                :INTEGER (-> resource (.getIntegerValue))
                :DOUBLE (-> resource (.getDoubleValue))
                :LONG (-> resource (.getLongValue))
                :STRINGSET (-> resource (.getStringSetValue) (vec)))]
    {:name name
     :type type
     :value value
     }))

(defn container-instance->map
  [^ContainerInstance container-instance]
  (let [container-instance-arn (-> container-instance (.getContainerInstanceArn))
        ec2-instance-id (-> container-instance (.getEc2InstanceId))
        registered-resources (->> container-instance
                                  (.getRegisteredResources)
                                  (mapv resource->map))
        remaining-resources (->> container-instance
                                 (.getRemainingResources)
                                 (mapv resource->map))]
    {:container-instance-arn container-instance-arn
     :ec2-instance-id        ec2-instance-id
     :registered-resources   registered-resources
     :remaining-resources    remaining-resources}))

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
  (log/debug "Create cluster:" cluster)
  (let [ecs-create-request (-> (CreateClusterRequest.)
                               (.withClusterName cluster))]
    (-> client
        (.createCluster ecs-create-request)
        (.getCluster)
        (cluster->map))))

(defn delete-cluster
  [^AmazonECSClient client cluster]
  (log/debug "Delete cluster:" cluster)
  (let [ecs-delete-request (-> (DeleteClusterRequest.)
                               (.withCluster cluster))]
    (-> client
        (.deleteCluster ecs-delete-request)
        (.getCluster)
        (cluster->map))))

(defn describe-cluster
  [^AmazonECSClient client cluster]
  (log/debug "Describe cluster:" cluster)
  (let [ecs-describe-request (-> (DescribeClustersRequest.)
                                 (.withClusters [cluster]))
        result (-> client (.describeClusters ecs-describe-request))
        cluster (-> result (.getClusters) (seq) (first))
        failure (-> result (.getFailures) (seq) (first))]
    (cond
      (and cluster (not failure)) (cluster->map cluster)
      (and failure (not cluster)) {:failure (-> failure (.getReason))}
      ;; Should not be here
      :else (throw (IllegalStateException. "Cluster is not found nor failed")))))


(defn list-task-definition-families
  [^AmazonECSClient client]
  (log/debug "List task definition families")
  (let [ecs-response (-> client (.listTaskDefinitionFamilies))]
    ;; TODO: Handle next token value
    (when (-> ecs-response (.getNextToken))
      (throw (RuntimeException. "List task defintiion families response is multi paged")))
    (-> ecs-response (.getFamilies))))

(defn describe-task-definition
  [^AmazonECSClient client family]
  (log/debug "Describe task definition family:" family)
  (let [ecs-request (-> (DescribeTaskDefinitionRequest.)
                        (.withTaskDefinition family))
        ecs-response (-> client
                         (.describeTaskDefinition ecs-request))]
    (task-definition->map
      (-> ecs-response (.getTaskDefinition)))))

(defn register-task-definition
  [^AmazonECSClient client family opts]
  (let [{:keys [image cpu memory]} opts
        ecs-container-def (-> (ContainerDefinition.)
                              (.withImage image)
                              (.withCpu (int cpu))
                              (.withMemory (int memory))
                              (.withName default-container-name))
        ecs-request (-> (RegisterTaskDefinitionRequest.)
                                 (.withFamily family)
                                 (.withContainerDefinitions [ecs-container-def]))]
    (log/debug "Register task definition. Family:" family "CPU:" cpu "Mem:" memory)
    (task-definition->map
      (-> client
          (.registerTaskDefinition ecs-request)
          (.getTaskDefinition)))))

(defn list-container-instances
  [^AmazonECSClient client cluster]
  (log/debug "List container instances in cluster:" cluster)
  (let [ecs-request (-> (ListContainerInstancesRequest.)
                        (.withCluster cluster))
        ecs-response (-> client (.listContainerInstances ecs-request))]
    ;; TODO: Handle next token value
    (when (-> ecs-response (.getNextToken))
      (throw (RuntimeException. "List container instances response is multi paged")))
    (-> ecs-response (.getContainerInstanceArns))))

(defn describe-container-instances
  [^AmazonECSClient client cluster instances]
  (log/debug "Describe container instances in cluster:" cluster "Instances:" instances)
  (let [ecs-describe-request (-> (DescribeContainerInstancesRequest.)
                                 (.withCluster cluster)
                                 (.withContainerInstances instances))
        ecs-response (-> client (.describeContainerInstances ecs-describe-request))
        container-instances (-> ecs-response (.getContainerInstances))
        failures (-> ecs-response (.getFailures))]
    {:container-instances (mapv container-instance->map container-instances)
     :failures            (mapv failure->map failures)}))

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
