(ns distributo.ec2
  (:require [clojure.core.async :as async :refer [go-loop <! >!! sliding-buffer chan]]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [base64-clj.core :as base64]
            [distributo.util :refer :all])
  (:import (com.amazonaws.services.ec2.model LaunchSpecification IamInstanceProfileSpecification RequestSpotInstancesRequest SpotInstanceRequest CreateTagsRequest Tag DescribeSpotInstanceRequestsRequest Filter SpotInstanceType)
           (com.amazonaws.services.ec2 AmazonEC2Client)
           (com.amazonaws.auth AWSCredentialsProvider)
           (com.amazonaws.regions Region Regions)))

;; Tag EC2 objects with cluster name

(def cluster-tag-name "DistributoCluster")

;; Mapping for AWS ECS enabled AMIs

(def ami {:us-east-1      "ami-2b3b6041"
          :us-west-1      "ami-bfe095df"
          :us-west-2      "ami-ac6872cd"
          :eu-west-1      "ami-03238b70"
          :eu-central-1   "ami-e1e6f88d"
          :ap-northeast-1 "ami-fb2f1295"
          :ap-southeast-1 "ami-c78f43a4"
          :ap-southeast-2 "ami-43547120"})

(def launch-specification-defaults {:region         :us-east-1
                                    :instance-type  :m4.large
                                    :security-group "default"
                                    :iam-role       "ecsInstanceRole"
                                    :key-name       "default"})

;; Mapping from AWS SDK objects into clojure data structures

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

;; Construct new AWS SDK objects

(defn new-client
  ([^AWSCredentialsProvider cred]
   (AmazonEC2Client. cred))
  ([^AWSCredentialsProvider cred region]
   (-> (AmazonEC2Client. cred)
       (.setRegion (Region/getRegion (Regions/fromName region))))))

(defn new-launch-specification
  ([cluster] (new-launch-specification cluster {}))
  ([cluster opts]
   (let [{:keys [region instance-type security-group iam-role key-name]} (merge launch-specification-defaults opts)
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

(defn new-spot-instances-request
  [cluster opts]
  (let [{:keys [spot-price count launch-specification]} opts
        spec (new-launch-specification cluster launch-specification)]
    (-> (RequestSpotInstancesRequest.)
        (.withSpotPrice (.toString spot-price))
        (.withType (SpotInstanceType/Persistent))
        (.withInstanceCount (int count))
        (.withLaunchSpecification spec))))

;; AWS SDK EC2 Api

(defn request-spot-instances
  [^AmazonEC2Client client cluster request]
  (log/debug "Request spot instance. Cluster:" cluster "Request:" request)
  (let [ec2-spot-request (new-spot-instances-request cluster request)]
    (map spot-instances-request->map
         (-> client
             (.requestSpotInstances ec2-spot-request)
             (.getSpotInstanceRequests)))))

(defn tag-spot-instance-requests
  [^AmazonEC2Client client cluster requests]
  (log/debug "Tag spot requests with cluster name:" cluster
             "Requests:" (mapv :spot-instance-request-id requests))
  (let [tag (Tag. cluster-tag-name cluster)
        ec2-tag-request (-> (CreateTagsRequest.)
                            (.withResources (map :spot-instance-request-id requests))
                            (.withTags [tag]))]
    (-> client (.createTags ec2-tag-request))))

(defn describe-spot-requests
  [^AmazonEC2Client client cluster]
  (let [tag-filter (-> (Filter.)
                       (.withName (str "tag:" cluster-tag-name))
                       (.withValues [cluster]))
        ec2-describe-request (-> (DescribeSpotInstanceRequestsRequest.)
                                 (.withFilters [tag-filter]))]
    (map spot-instances-request->map
         (-> client
             (.describeSpotInstanceRequests ec2-describe-request)
             (.getSpotInstanceRequests)))))

;; Combinators on top of low lever AWS SDK

(defn spot-requests-to-map
  "Converts seq of spot requests into map"
  [requests]
  (let [req->tuple (fn [req] [(keyword (:spot-instance-request-id req)) (dissoc req :spot-instance-request-id)])]
    (into {} (map req->tuple requests))))

(defn spot-requests-transition
  "Cumpute transition map between two spot requests states"
  [a b]
  (let [all-reqs (concat (keys a) (keys b))
        nil-map (nil-map all-reqs)]
    (merge-with transition (merge nil-map a) (merge nil-map b))))

(defn spot-requests-chan
  "Constructs core.async channel with spot requests states"
  [^AmazonEC2Client client cluster metronome]
  (let [requests (chan)
        xf (comp
             (map (fn [_] (describe-spot-requests client cluster)))
             (map spot-requests-to-map))]
    (async/pipeline-blocking 1 requests xf metronome)
    requests))

(defn spot-requests-transitions-chan
  "Constructs core.async channel with spot requests transitions"
  [^AmazonEC2Client client cluster metronome]
  (let [transitions (chan)
        sliding-window (make-sliding-window 2)
        xf (comp
             (map (fn [_] (describe-spot-requests client cluster)))
             (map spot-requests-to-map)
             (map (fn [state] (seq (sliding-window state))))
             (filter #(= 2 (count %)))
             (map (fn [[a b]] (spot-requests-transition a b))))]
    (async/pipeline-blocking 1 transitions xf metronome)
    transitions))
