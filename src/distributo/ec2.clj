(ns distributo.ec2
  (:require [clojure.core.async :as async :refer [go-loop <! >!! sliding-buffer chan]]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [base64-clj.core :as base64]
            [distributo.util :refer :all])
  (:import (com.amazonaws.services.ec2.model LaunchSpecification IamInstanceProfileSpecification RequestSpotInstancesRequest SpotInstanceRequest)
           (com.amazonaws.services.ec2 AmazonEC2Client)
           (com.amazonaws.auth AWSCredentialsProvider)
           (com.amazonaws.regions Region Regions)))

;; Mapping for AWS ECS enabled AMIs

(def ami {:us-east-1      "ami-2b3b6041"
          :us-west-1      "ami-bfe095df"
          :us-west-2      "ami-ac6872cd"
          :eu-west-1      "ami-03238b70"
          :eu-central-1   "ami-e1e6f88d"
          :ap-northeast-1 "ami-fb2f1295"
          :ap-southeast-1 "ami-c78f43a4"
          :ap-southeast-2 "ami-43547120"})

(def instance-defaults {:region         :us-east-1
                        :instance-type  :m4.large
                        :security-group "default"
                        :iam-role       "ecsInstanceRole"
                        :key-name       "default"})

(defn new-client
  ([^AWSCredentialsProvider cred]
   (AmazonEC2Client. cred))
  ([^AWSCredentialsProvider cred region]
   (-> (AmazonEC2Client. cred)
       (.setRegion (Region/getRegion (Regions/fromName region))))))

(defn launch-specification
  ([cluster] (launch-specification cluster {}))
  ([cluster opts]
   (let [{:keys [region instance-type security-group iam-role key-name]} (merge instance-defaults opts)
         user-data (str/join ["#!/bin/bash" "\n"
                              "echo ECS_CLUSTER=" cluster " >> /etc/ecs/ecs.config"])]
     (-> (LaunchSpecification.)
         (.withImageId (region ami))
         (.withInstanceType (name instance-type))
         (.withSecurityGroups [security-group])
         (.withKeyName key-name)
         (.withUserData (base64/encode user-data))
         (.withIamInstanceProfile (-> (IamInstanceProfileSpecification.)
                                      (.withName iam-role)))))))

(defn spot-instances-request->map
  [^SpotInstanceRequest req]
  (let [spot-instance-request-id (-> req (.getSpotInstanceRequestId))
        spot-price (-> req (.getSpotPrice) (Double/valueOf))
        type (-> req (.getType) (keyword))
        state (-> req (.getState) (keyword))
        status (-> req (.getStatus) (.getCode) (keyword))
        instance-id (-> req (.getInstanceId) (keyword))]
    {:spot-instance-request-id spot-instance-request-id
     :spot-price               spot-price
     :type                     type
     :state                    state
     :status                   status
     :instance-id              instance-id
     }))

(defn spot-instances-request
  [price n spec]
  (-> (RequestSpotInstancesRequest.)
      (.withSpotPrice (.toString price))
      (.withInstanceCount (int n))
      (.withLaunchSpecification spec)))

(defn request-spot-instances
  [^AmazonEC2Client client req]
  (log/debug "Request spot instance:" req)
  (-> client (.requestSpotInstances req)))

(defn describe-spot-requests
  "Get spot requests state map"
  [^AmazonEC2Client client]
  (let [req->tuple (fn [req] [(keyword (:spot-instance-request-id req)) (dissoc req :spot-instance-request-id)])]
    (into {} (map req->tuple
                  (->> client
                       (.describeSpotInstanceRequests)
                       (.getSpotInstanceRequests)
                       (map spot-instances-request->map))))))

(defn spot-requests-transition
  "Cumpute transition map between two spot requests states"
  [a b]
  (let [all-reqs (concat (keys a) (keys b))
        nil-map (nil-map all-reqs)]
    (merge-with transition (merge nil-map a) (merge nil-map b))))

(defn spot-requests-chan
  "Constructs core.async channel with spot requests states"
  [^AmazonEC2Client client metronome]
  (let [requests (chan)
        xf (map (fn [_] (describe-spot-requests client)))]
    (async/pipeline 1 requests xf metronome)
    requests))

(defn spot-requests-transitions-chan
  "Constructs core.async channel with spot requests transitions"
  [^AmazonEC2Client client metronome]
  (let [transitions (chan)
        sliding-window (make-sliding-window 2)
        xf (comp
             (map (fn [_] (describe-spot-requests client)))
             (map (fn [state] (seq (sliding-window state))))
             (filter #(= 2 (count %)))
             (map (fn [[a b]] (spot-requests-transition a b))))]
    (async/pipeline 1 transitions xf metronome)
    transitions))
