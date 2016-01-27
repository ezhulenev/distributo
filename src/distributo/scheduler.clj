(ns distributo.scheduler
  (:import (com.amazonaws.services.ecs AmazonECSClient))
  (:require [com.rpl.specter :refer :all]
            [clojure.core.async :as async]
            [distributo.ecs :as ecs]
            [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [clj-time.periodic :as periodic]
            [chime :refer [chime-at]]))

"Scheduler manages executing jobs (tasks) on available ECS container instances."

;; Job Life Cycle: (* terminal states)
;;
;; :pending -> :started
;;               / \
;;             /    \
;;     :failed*      :finished*
;;
;; 1. :waiting   - waiting for resources to start ECS task
;; 2. :started   - ECS task started
;; 3. :failed    - ECS task failed
;; 4. :finished  - ECS task finished
;;
;; Job can go from :started into :pending state it instance that was used
;; for running task was terminated. In this case :task-arn dissoced
;; from job, to remove confusion

(defrecord Job [name task-definition command required-resources status task])

(defrecord State [jobs])

(def default-scheduler-opts {:update-interval (-> 10 t/seconds)})

(defn required-resources
  [^AmazonECSClient client task-definition]
  (let [task-def (ecs/describe-task-definition client task-definition)
        container-def (:container-definition task-def)
        status (:status task-def)]
    (assert (= status :active) (str "Wrong task definition status: " status
                                    ". Task definition: " task-definition))
    (select-keys container-def [:cpu :memory])))

(def required-resources-memo (memoize required-resources))

(defn mk-job
  [^AmazonECSClient client name task-definition command]
  map->Job {:name               name
            :task-definition    task-definition
            :command            command
            :required-resources (required-resources-memo client task-definition)
            :status             :waiting})

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
  (let [container-instances-atom (atom initital-container-instances)
        waiting? #(= (:status %) :waiting)
        start-job (fn [job]
                    (if-let [best-fit (best-fit (:required-resources job) @container-instances-atom)]
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
                                       (swap! container-instances-atom
                                              (partial retract-resources
                                                       (:required-resources job)
                                                       (:container-instance-arn best-fit)))
                                       [(assoc job :status :started :task-arn task-arn)
                                        [task-arn]])
                            ;; Task failed to start for whatever reason
                            failure [(assoc job :status :failed :reason failure)
                                     []])))
                      ;; No resources available for starting job task
                      (do
                        (log/trace "Cant't find resources to start job:" (:name job))
                        (job []))))
        [state' tasks] (replace-in [:jobs ALL waiting?] start-job state)]
    {:state state'
     :tasks tasks}))

(defn update-state
  "Update state with latest task information"
  [^State state tasks]
  (let [arn->task (into {} (for [task tasks] [(:task-arn task) task]))
        updated? #(contains? arn->task (:task-arn %))
        update (fn [job]
                 (let [name (:name job)
                       task (get arn->task (:task-arn job))
                       last-status (:last-status task)
                       desired-status (:desired-status task)
                       container-last-status (get-in task [:container :last-status])
                       exit-code (get-in task [:container :exit-code])]
                   (log/trace "Update job" name "status with task status:" {:last-status           last-status
                                                                            :desired-status        desired-status
                                                                            :container-last-status container-last-status})
                   (condp = [last-status desired-status container-last-status]
                     ;; Task pending: downloading container, etc...
                     [:pending :running :pending] (-> job
                                                      (assoc :status :started))
                     ;; Task running
                     [:running :running :running] (-> job
                                                      (assoc :status :started))
                     ;; TODO: Limit max retry number?
                     ;; Instance terminated before container started
                     [:stopped :stopped :pending] (do
                                                    (log/warn "Job" name "stopped. Instance terminated before container started")
                                                    (-> job
                                                        (assoc :status :waiting)
                                                        (dissoc :task-arn)))
                     ;; Instance terminated while container was running
                     [:stopped :stopped :running] (do
                                                    (log/warn "Job" name "stopped. Instance terminated while container was running")
                                                    (-> job
                                                        (assoc :status :waiting)
                                                        (dissoc :task-arn)))
                     ;; Task stopped
                     [:stopped :stopped :stopped] (cond
                                                    ;; Docker container never started (wrong image, etc...)
                                                    (nil? exit-code) (do
                                                                       (log/warn "Job" name "stopped without any exit code")
                                                                       (-> job
                                                                           (assoc :status :failed)
                                                                           (assoc :reason "No exit code available. Possibly Docker image not found")))
                                                    ;; Container finished successfully
                                                    (= 0 exit-code) (do
                                                                      (log/debug "Job" name "successfully finished")
                                                                      (-> job
                                                                          (assoc :status :finished)))
                                                    ;; It started and failed
                                                    :else (do
                                                            (log/warn "Job" job "stopped with non zero exit code:" exit-code)
                                                            (-> job
                                                                (assoc :status :failed)
                                                                (assoc :reason (str "Non zero exit code: " exit-code)))))
                     ;; Unexpected status treated as job failed
                     :else (-> job
                               (assoc :status :failed)
                               (assoc :reason :reason "Unknown task status")))))]
    (transform [:jobs ALL updated?] update state)))

(defn run-scheduler!
  "Run schedulder until all jobs completed (finished or failed)"
  ([^AmazonECSClient client cluster jobs]
   (run-scheduler! client cluster jobs {}))
  ([^AmazonECSClient client cluster jobs opts]
   (let [{:keys [update-interval]} (merge default-scheduler-opts opts)
         state-atom (atom (->State jobs))
         done-ch (async/chan)
         non-terminal-state? (fn [job]
                               (or (= (:status job) :waiting)
                                   (= (:status job) :started)))
         shutdown (chime-at (periodic/periodic-seq (t/now) update-interval)
                            (fn [_]
                              (let [job-started? (fn [job] (= (:status job) :started))
                                    started-task-arns (map :task-arn (filter job-started? (:jobs @state-atom)))
                                    started-tasts (ecs/describe-tasks client cluster started-task-arns)
                                    container-instances (ecs/describe-container-instances
                                                          client
                                                          cluster
                                                          (ecs/list-container-instances client cluster))]
                                ;; TODO: Better failures handling?
                                (when (seq (:failures started-tasts))
                                  (log/warn "Got failures trying to describe tasks:" (:failures started-tasts)))
                                (when (seq (:failures container-instances))
                                  (log/warn "Got failures trying to describe container instances:" (:failures container-instances)))
                                ;; Update state based on new ECS cluster state
                                (swap! state-atom (fn [state]
                                                    (let [state' (update-state
                                                                   state
                                                                   (:tasks started-tasts))
                                                          started (start-jobs!
                                                                    client
                                                                    cluster
                                                                    state'
                                                                    (:container-instances container-instances))]
                                                      (when (seq (:tasks started))
                                                        (log/trace "Started" (count (:tasks started)) "new tasks"))
                                                      (:state started))))
                                ;; Finish when all jobs are in terminal state
                                (when (empty? (filter non-terminal-state? (:jobs @state-atom)))
                                  (async/close! done-ch)))))]
     ;; Wait until all jobs completed
     (async/<!! done-ch)
     (shutdown)
     (deref state-atom))))
