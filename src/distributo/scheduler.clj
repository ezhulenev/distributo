(ns distributo.scheduler
  (:import (com.amazonaws.services.ecs AmazonECSClient))
  (:require [distributo.ecs :as ecs]))

"Scheduler manages executing jobs (tasks) on available ECS container instances"

(defrecord Job [task-definition command])

(defrecord State [jobs])

(defn load-job-required-resources
  [^AmazonECSClient client ^Job job]
  (let [{:keys [task-definition]} job
        task-def (ecs/describe-task-definition client task-definition)
        container-def (:container-definition task-def)
        status (:status task-def)]
    (assert (= status :active) (str "Wrong task definition status: " status
                                     ". Task definition: " task-definition))
    (assoc job :required-resource (select-keys container-def [:cpu :memory]))))
