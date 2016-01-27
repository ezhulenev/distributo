(ns distributo.allocator
  (:import (com.amazonaws.services.ec2 AmazonEC2Client))
  (:require [com.rpl.specter :refer :all]
            [clojure.core.async :as async]
            [distributo.ec2 :as ec2]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [clj-time.periodic :as periodic]
            [chime :refer [chime-at]]))

"Allocator is responsible for allocating resources for ECS cluster: EC2 spot instances

 Current dummy implementation can only maintain fixed number of spot requests inside one availability zone.

 More clever implementation should pick intance types, regions and availability zones based on
 current situation on spot market and amount or resources required for pending jobs"

(def default-await-options {:fulfill-timeout 10000
                            :update-interval (-> 1 t/seconds)})

(defn allocate-cluster-resources!
  ([^AmazonEC2Client client cluster n bid-request]
   (allocate-cluster-resources! client cluster n bid-request (ec2/describe-spot-requests client cluster)))
  ([^AmazonEC2Client client cluster n bid-request spot-requests]
   (let [open (->> spot-requests
                   (filter #(or (= :open (:state %))
                                (= :active (:state %)))))
         num-open (count open)]
     (log/info "Allocate cluster resources:" cluster
               "Number of desired spot requests:" n
               "Instance type:" (get-in bid-request [:launch-specification :instance-type]))
     ;; TODO: 1. I'm not checking that existing spot-requests are
     ;; TODO:    actually of the same instance type as in new bid request
     ;; TODO: 2. Handle cluster downscaling?
     (cond
       ;; Perfect
       (= n num-open) (log/info "Cluster" cluster "already has desired number of spot requests")
       ;; Under-allocated
       (> n num-open) (do
                        (log/info "Cluster" cluster "size needs to be increased by" (- n num-open) "spot requests")
                        (ec2/request-and-tag-spot-instances! client cluster (merge bid-request {:count (- n num-open)})))
       ;; Over-allocated
       (< n num-open) (log/warn "Cluster" cluster "has more open spot requests than required")))))

(defn await-fulfillment
  "Waits until minimum number of spot requests fulfilled"
  ([^AmazonEC2Client client cluster n]
    (await-fulfillment client cluster n {}))
  ([^AmazonEC2Client client cluster n opts]
   (let [{:keys [update-interval fulfill-timeout]} (merge default-await-options opts)
         timeout-ch (async/timeout fulfill-timeout)
         fulfilled-ch (async/chan)
         shutdown (chime-at (periodic/periodic-seq (t/now) update-interval)
                            (fn [_]
                              (let [spot-requests (ec2/describe-spot-requests client cluster)
                                    fulfilled (->> spot-requests
                                                   (filter #(or (= :open (:state %))
                                                                (= :active (:state %))))
                                                   (filter #(= :fulfilled (:status %))))]
                                (when (<= n (count fulfilled))
                                  (async/close! fulfilled-ch)))))]
     (let [result (async/alt!!
                    fulfilled-ch :fulfilled
                    timeout-ch :timed-out)]
       (shutdown)
       (condp = result
         :fulfilled (log/trace "Successfully fulfilled minimum requirement of" n "instances")
         :timed-out (throw (RuntimeException. "Timed out awaiting mimimum fulfillment")))))))

(defn free-cluster-resources!
  [^AmazonEC2Client client cluster]
  (log/info "Free cluster resources:" cluster)
  (loop [spot-requests (ec2/describe-spot-requests client cluster)
         cancelled #{}
         terminated #{}]
    (let [open (->> spot-requests
                    (filter #(or (= :open (:state %))
                                 (= :active (:state %))))
                    (filter #(not (contains? cancelled (:spot-instance-request-id %)))))
          fulfilled (->> spot-requests
                         (filter #(= :fulfilled (:status %)))
                         (filter #(not (contains? terminated (:instance-id %)))))
          cancelled' (into #{} (map :spot-instance-request-id open))
          terminated' (into #{} (map :instance-id fulfilled))]
      ;; Cancel all open spot requests that are not already cancelled
      (when (seq cancelled')
        (ec2/cancel-spot-requests! client cluster cancelled'))
      ;; Make sure that all fulfilled requests has it's instances terminated
      (when (seq terminated')
        (ec2/terminate-instances! client terminated')
        (recur
          (ec2/describe-spot-requests client cluster)
          (set/union cancelled cancelled')
          (set/union terminated terminated'))))))
