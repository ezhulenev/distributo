(ns distributo.ecs
  (:require [chime :refer [chime-ch]]
            [clojure.core.async :as async :refer [go-loop <! >!! sliding-buffer chan]]
            [clojure.tools.logging :as log]
            [clojure.data :refer [diff]]
            [distributo.util :refer :all])
  (:import (com.amazonaws.services.ecs AmazonECSClient)
           (com.amazonaws.services.ecs.model CreateClusterRequest DescribeClustersRequest DeleteClusterRequest Cluster DescribeTaskDefinitionRequest TaskDefinition ContainerDefinition RegisterTaskDefinitionRequest ListContainerInstancesRequest DescribeContainerInstancesRequest ContainerInstance Resource Failure StartTaskRequest TaskOverride ContainerOverride KeyValuePair Task Container DescribeTasksRequest ListTasksRequest)
           (com.amazonaws.auth AWSCredentialsProvider)
           (com.amazonaws.regions Region Regions)))

;; Name of default container inside task definition

(def default-container-name "default")

;; Mapping from AWS SDK objects into clojure data structures

(defn failure->map
  [^Failure failure]
  (let [arn (-> failure (.getArn))
        reason (-> failure (.getReason))]
    {:arn    arn
     :reason reason}))

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
        container-definition (first (-> task-definition (.getContainerDefinitions)))
        environment (-> container-definition (.getEnvironment))]
    {:family               family
     :revision             revision
     :status               (keyword status)
     :container-definition (container-definition->map container-definition)
     :environment          (into {} (map (fn [kv-pair]
                                           (let [k (keyword (-> kv-pair (.getName)))
                                                 v (-> kv-pair (.getValue))]
                                             [k v]))
                                         environment))}))

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

(defn container->map
  [^Container container]
  (let [name (-> container (.getName))
        last-status (-> container (.getLastStatus))
        exit-code (-> container (.getExitCode))]
    {:name        name
     :last-status (keyword last-status)
     :exit-code   exit-code}))

(defn task->map
  [^Task task]
  (let [task-arn (-> task (.getTaskArn))
        container-instance-arn (-> task (.getContainerInstanceArn))
        container (-> task (.getContainers) (first))
        command (-> task (.getOverrides) (.getContainerOverrides) (first) (.getCommand))
        last-status (-> task (.getLastStatus))
        desired-status (-> task (.getDesiredStatus))]
    {:task-arn task-arn
     :container-instance-arn container-instance-arn
     :container (container->map container)
     :command (vec command)
     :last-status (keyword last-status)
     :desired-status (keyword desired-status)}))

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
        (.getClusterArn))))

(defn delete-cluster
  [^AmazonECSClient client cluster]
  (log/debug "Delete cluster:" cluster)
  (let [ecs-delete-request (-> (DeleteClusterRequest.)
                               (.withCluster cluster))]
    (-> client
        (.deleteCluster ecs-delete-request)
        (.getCluster)
        (.getClusterArn))))

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
  [^AmazonECSClient client task-def]
  (log/debug "Describe task definition:" task-def)
  (let [ecs-request (-> (DescribeTaskDefinitionRequest.)
                        (.withTaskDefinition task-def))
        ecs-response (-> client
                         (.describeTaskDefinition ecs-request))]
    (-> ecs-response
        (.getTaskDefinition)
        (task-definition->map))))

(defn register-task-definition
  [^AmazonECSClient client family opts]
  (let [{:keys [image cpu memory environment]} opts
        env-kv-pairs (map (fn [[k v]] (-> (KeyValuePair.)
                                          (.withName (name k))
                                          (.withValue v)))
                          environment)
        ecs-container-def (-> (ContainerDefinition.)
                              (.withImage image)
                              (.withCpu (int cpu))
                              (.withMemory (int memory))
                              (.withEnvironment env-kv-pairs)
                              (.withName default-container-name))
        ecs-request (-> (RegisterTaskDefinitionRequest.)
                        (.withFamily family)
                        (.withContainerDefinitions [ecs-container-def]))]
    (log/debug "Register task definition. Family:" family
               "CPU:" cpu "Mem:" memory
               "Environment:" environment)
    (-> client
        (.registerTaskDefinition ecs-request)
        (.getTaskDefinition)
        (.getTaskDefinitionArn))))

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
  [^AmazonECSClient client cluster instances-arns]
  (log/debug "Describe container instances in cluster:" cluster
             "Instances:" (vec instances-arns))
  (let [ecs-requests (-> (DescribeContainerInstancesRequest.)
                         (.withCluster cluster)
                         (.withContainerInstances instances-arns))
        ecs-response (-> client (.describeContainerInstances ecs-requests))
        container-instances (-> ecs-response (.getContainerInstances))
        failures (-> ecs-response (.getFailures))]
    {:container-instances (mapv container-instance->map container-instances)
     :failures            (mapv failure->map failures)}))

(defn list-tasks
  [^AmazonECSClient client cluster]
  (log/debug "List tasks in cluster:" cluster)
  (let [ecs-request (-> (ListTasksRequest.)
                        (.withCluster cluster))
        ecs-response (-> client (.listTasks ecs-request))]
    ;; TODO: Handle next token value
    (when (-> ecs-response (.getNextToken))
      (throw (RuntimeException. "List tasks response is multi paged")))
    (-> ecs-response (.getTaskArns))))

(defn describe-tasks
  [^AmazonECSClient client cluster tasks-arns]
  (log/debug "Describe tasks in cluster:" cluster
             "Tasks:" (vec tasks-arns))
  (let [ecs-request (-> (DescribeTasksRequest.)
                        (.withCluster cluster)
                        (.withTasks tasks-arns))
        ecs-response (-> client (.describeTasks ecs-request))
        tasks (-> ecs-response (.getTasks))
        failures (-> ecs-response (.getFailures))]
    {:tasks    (mapv task->map tasks)
     :failures (mapv failure->map failures)}))

(defn start-task
  [^AmazonECSClient client cluster container-instance-arn task-definition command]
  (let [container-override (-> (ContainerOverride.)
                               (.withName default-container-name)
                               (.withCommand command))
        task-overrides (-> (TaskOverride.)
                           (.withContainerOverrides [container-override]))
        ecs-request (-> (StartTaskRequest.)
                        (.withCluster cluster)
                        (.withContainerInstances [container-instance-arn])
                        (.withTaskDefinition task-definition)
                        (.withOverrides task-overrides))]
    (log/debug "Start task on:" container-instance-arn
               "Task definition:" task-definition
               "Command:" (vec command))
    (let [ecs-response (-> client (.startTask ecs-request))
          task (-> ecs-response (.getTasks) (seq) (first))
          failure (-> ecs-response (.getFailures) (seq) (first))]
      (cond
        (and task (not failure)) {:task (-> task (.getTaskArn))}
        (and failure (not task)) {:failure (-> failure (.getReason))}
        ;; Should not be here
        :else (throw (IllegalStateException. "Illegal StartTaskResponse:" ecs-response))))))

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
