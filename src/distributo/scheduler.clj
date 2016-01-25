(ns distributo.scheduler
  (:import (com.amazonaws.services.ecs AmazonECSClient))
  (:require [distributo.ecs :as ecs]
            [com.rpl.specter :refer :all]
            [clojure.tools.logging :as log]))

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

(defn mk-job
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
    (and (<= 0 (:cpu resources)) (<= 0 (:memory resources)))))

(defn best-fit
  "Finds best container instance to place job based on fitness"
  [required-resources container-instances]
  (let [fits-into (filter #(fits?
                            required-resources
                            (:remaining-resources %))
                          container-instances)
        sorted (sort-by #(cpu-and-memory-fitness
                          required-resources
                          (:registered-resources %)
                          (:remaining-resources %))
                        fits-into)]
    (first (reverse sorted))))

(defn retract-resources
  [resources container-instance-arn container-instances]
  (transform [ALL #(= container-instance-arn (:container-instance-arn %))]
             (fn [container-instance]
               ((comp
                  #(update-in % [:remaining-resources :cpu] - (:cpu resources))
                  #(update-in % [:remaining-resources :memory] - (:memory resources)))
                 container-instance))
             container-instances))

(defn start-jobs!
  [^AmazonECSClient client cluster ^State state initital-container-instances]
  (let [container-instances (atom initital-container-instances)
        pending? #(= (:status %) :pending)
        start-job (fn [job]
                    (if-let [best-fit (best-fit (:required-resources job) @container-instances)]
                      ;; Found container instance to run job task
                      (do
                        (log/trace "Found resources to start job:" (:name job)
                                   "Container instance:" (:container-instance-arn best-fit))
                        (let [started (ecs/start-task!
                                        client
                                        cluster
                                        (:container-instance-arn best-fit)
                                        (:task-definition job) (:command job))
                              task-arn (:task-arn started)
                              failure (:failure started)]
                          (cond
                            ;; Task was successfully started
                            task-arn (do
                                       ;; Update remaining resources
                                       (swap! container-instances
                                              (partial retract-resources
                                                       (:required-resources job)
                                                       (:container-instance-arn best-fit)))
                                       [(assoc job :status :running :task-arn task-arn)
                                        [task-arn]])
                            ;; Task failed to start for whatever reason
                            failure [(assoc job :status :failed :reason failure)
                                     []])))
                      ;; No resources available for starting job task
                      (do
                        (log/trace "Cant't find resources to start job:" (:name job))
                        (job []))))
        [state' tasks] (replace-in [:jobs ALL pending?] start-job state)]
    {:state state'
     :tasks tasks}))
