(ns distributo.test.scheduler
  (:use clojure.test)
  (:require [distributo.scheduler :refer :all]))

(def job {:name               "test-job",
          :task-definition    "test-task",
          :command            ["start"],
          :required-resources {:cpu 1000, :memory 1024},
          :status             :pending})

(def m4-large-container-instance {:container-instance-arn "arn:aws:ecs:us-east-1:050689537474:container-instance/some-random-arn",
                                  :ec2-instance-id        "i-123456789",
                                  :status                 :active,
                                  :registered-resources   {:cpu 2048, :memory 7987},
                                  :remaining-resources    {:cpu 2048, :memory 7987}})

(defn mk-jobs
  [names]
  (mapv (fn [name] (assoc job :name name)) names))

(defn absolute-difference ^double [^double x ^double y]
  (Math/abs (double (- x y))))

(defn approx= [tolerance x y]
  (< (absolute-difference x y) tolerance))

(deftest compute-cpu-fitess
  (is (= 0.9 (cpu-fitness {:cpu 500}
                          {:cpu 1000}
                          {:cpu 600}))))

(deftest compute-memory-fitess
  (is (= 0.8 (memory-fitness {:memory 400}
                             {:memory 1000}
                             {:memory 600}))))

(deftest compute-cpu-and-memory-fitness
  (is (approx= 0.0001 0.85 (cpu-and-memory-fitness {:cpu 500 :memory 400}     ;; required
                                                   {:cpu 1000 :memory 1000}   ;; registered
                                                   {:cpu 600 :memory 600})))) ;; remaining

(deftest fits-works-correctly
  (is (fits? {:cpu 100 :memory 100} {:cpu 500 :memory 1000}))
  (is (not (fits? {:cpu 1000 :memory 100} {:cpu 500 :memory 1000}))))

(deftest finds-best-fit
  (let [required-resource {:cpu 500 :memory 500}
        ;; Instance is too small
        instance-1 {:registered-resources {:cpu 400 :memory 1000}
                    :remaining-resources  {:cpu 400 :memory 1000}}
        instance-2 {:registered-resources {:cpu 1000 :memory 1000}
                    :remaining-resources  {:cpu 800 :memory 800}}
        ;; instance-3 has higher utilization/fitness
        instance-3 {:registered-resources {:cpu 1000 :memory 1000}
                    :remaining-resources  {:cpu 500 :memory 500}}]
    (is (nil? (best-fit required-resource [])))
    (is (nil? (best-fit required-resource [instance-1])))
    (is (nil? (best-fit {:cpu 2000 :memory 500} [instance-1 instance-2 instance-3])))
    (is (= instance-2 (best-fit required-resource [instance-1 instance-2])))
    (is (= instance-3 (best-fit required-resource [instance-1 instance-2 instance-3])))))

(deftest retract-resources-works
  (let [resources {:cpu 100 :memory 200}
        instance-1 {:container-instance-arn "arn-1"
                    :remaining-resources    {:cpu 400 :memory 1000}}
        instance-2 {:container-instance-arn "arn-2"
                    :remaining-resources    {:cpu 400 :memory 1000}}
        expected {:container-instance-arn "arn-1"
                  :remaining-resources    {:cpu 300 :memory 800}}]
    (is (= [expected] (retract-resources resources "arn-1", [instance-1])))
    (is (= [expected instance-2] (retract-resources resources "arn-1", [instance-1 instance-2])))))

(deftest skip-start-jobs-when-no-container-instances
  (let [client nil
        cluster "test"
        state (->State [job])
        container-instances []]
    (let [started (start-jobs! client cluster state container-instances)]
      (is (empty? (:tasks started)))
      (is (= state (:state started))))))

(deftest skip-start-jobs-when-no-resources-available
  (let [client nil
        cluster "test"
        state (->State [job])
        container-instances [(merge m4-large-container-instance {:remaining-resources {:cpu 100 :memory 100}})]]
    (let [start-res (start-jobs! client cluster state container-instances)]
      (is (empty? (:tasks start-res)))
      (is (= state (:state start-res))))))

(deftest start-one-job
  (let [client nil
        cluster "test"
        state (->State [job])
        container-instances [m4-large-container-instance]]
    (with-redefs-fn {#'distributo.ecs/start-task!
                     (fn [_ _ _ _ _]
                       ;; Ignores input and returns started task arn
                       {:task-arn "started-task-arn#1"})}
      #(let [start-res (start-jobs! client cluster state container-instances)
             job' (assoc job :status :running :task-arn "started-task-arn#1")]
        (is (= ["started-task-arn#1"] (:tasks start-res)))
        (is (= (->State [job']) (:state start-res)))))))

(deftest start-one-job-and-fail-second-job
  (let [client nil
        cluster "test"
        state (->State (mk-jobs ["job#1" "job#2"]))
        container-instances [(assoc m4-large-container-instance :container-instance-arn "arn-1")]
        ;; atom counting calls to ecs/start-task!
        counter (atom 0)]
    (with-redefs-fn {#'distributo.ecs/start-task!
                     (fn [_ _ _ _ _]
                       ;; Ignores input and returns started task arn
                       (swap! counter inc)
                       (if (= 1 @counter)
                         {:task-arn "started-task-arn#1"}
                         {:failure "ERROR"}))}
      #(let [start-res (start-jobs! client cluster state container-instances)
             job-1' (assoc job :name "job#1" :status :running :task-arn "started-task-arn#1")
             job-2' (assoc job :name "job#2" :status :failed :reason "ERROR")]
        (is (= ["started-task-arn#1"] (:tasks start-res)))
        (is (= (->State [job-1' job-2']) (:state start-res)))))))

(deftest start-four-out-of-five-jobs
  (let [client nil
        cluster "test"
        ;; 5 jobs
        state (->State (mk-jobs ["job#1" "job#2" "job#3" "job#4" "job#5"]))
        ;; and only 2 instances, one of each can fit only 2 jobs
        container-instances [(assoc m4-large-container-instance :container-instance-arn "arn-1")
                             (assoc m4-large-container-instance :container-instance-arn "arn-2")]
        ;; atom counting calls to ecs/start-task!
        counter (atom 0)]
    (with-redefs-fn {#'distributo.ecs/start-task!
                     (fn [_ _ _ _ _]
                       ;; Ignores input and returns started task arn
                       (swap! counter inc)
                       {:task-arn (str "started-task-arn#" @counter)})}
      #(let [start-res (start-jobs! client cluster state container-instances)
             job-1' (assoc job :name "job#1" :status :running :task-arn "started-task-arn#1")
             job-2' (assoc job :name "job#2" :status :running :task-arn "started-task-arn#2")
             job-3' (assoc job :name "job#3" :status :running :task-arn "started-task-arn#3")
             job-4' (assoc job :name "job#4" :status :running :task-arn "started-task-arn#4")
             job-5' (assoc job :name "job#5")
             state' (->State [job-1' job-2' job-3' job-4' job-5'])]
        (is (= ["started-task-arn#1" "started-task-arn#2" "started-task-arn#3" "started-task-arn#4"] (:tasks start-res)))
        (is (= state' (:state start-res)))))))
