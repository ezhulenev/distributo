(ns distributo.scheduler
  (:import (com.amazonaws.services.ecs AmazonECSClient))
  (:require [distributo.ecs :as ecs]
            [com.rpl.specter :refer :all]))

"Scheduler manages executing jobs (tasks) on available ECS container instances"

(defrecord Job [name task-definition command required-resources status task])

(defrecord State [jobs])

(defn required-resources
  [^AmazonECSClient client task-definition]
  (let [task-def (ecs/describe-task-definition client task-definition)
        container-def (:container-definition task-def)
        status (:status task-def)]
    (assert (= status :active) (str "Wrong task definition status: " status
                                    ". Task definition: " task-definition))
    (select-keys container-def [:cpu :memory])))

(defn new-job
  [^AmazonECSClient client name task-definition command]
  map->Job {:name               name
            :task-definition    task-definition
            :command            command
            :required-resources (required-resources client task-definition)
            :status             :pending})

;; Concept of fitness calculattion for task placing is borrowed
;; from Netflix/Fenzo: https://github.com/Netflix/Fenzo/wiki/Fitness-Calculators

(defn resource-fitness
  [resource required-resources registered-resources remaining-resources]
  (let [required (get required-resources resource)
        registered (get registered-resources resource)
        remaining (get remaining-resources resource)
        used (- registered remaining)]
    (double (/ (+ used required) registered))))

(def cpu-fitness (partial resource-fitness :cpu))

(def memory-fitness (partial resource-fitness :memory))

(defn cpu-and-memory-fitness
  [required-resources registered-resources remaining-resources]
  (/ (+ (cpu-fitness required-resources registered-resources remaining-resources)
        (memory-fitness required-resources registered-resources remaining-resources))
     2))

(defn fits?
  "Check that required resoruces fits given remaining resources"
  [required-resources remaining-resources]
  (let [resources (merge-with - remaining-resources required-resources)]
    (and (< 0 (:cpu resources)) (< 0 (:memory resources)))))

(defn start-jobs!
  [^State state initital-container-instances]
  (let [container-instances (atom initital-container-instances)
        pending? #(= (:status %) :pending)
        start-job (fn [job]
                    [job []])
        [state' tasks] (replace-in [:jobs ALL pending?] start-job state)]
    {:state state'
     :tasks tasks}))
