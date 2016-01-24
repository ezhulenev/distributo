(ns distributo.test.scheduler
  (:use clojure.test)
  (:require [distributo.scheduler :refer :all]))

(def job {:name               "test-job",
          :task-definition    "test-task",
          :command            ["start"],
          :required-resources {:cpu 1000, :memory 1024},
          :status             :pending})

;; m4.large instance
(def container-instance {:container-instance-arn "arn:aws:ecs:us-east-1:050689537474:container-instance/some-random-arn",
                         :ec2-instance-id "i-123456789",
                         :status :active,
                         :registered-resources {:cpu 2048, :memory 7987},
                         :remaining-resources {:cpu 2048, :memory 7987}})

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

(deftest skip-jobs-when-no-resources
  (let [state (->State [job])
        container-instances []]
    (let [started (start-jobs! state container-instances)]
      (is (empty? (:tasks started)))
      (is (= state (:state started))))))
