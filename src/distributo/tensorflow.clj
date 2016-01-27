(ns distributo.tensorflow
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [distributo.ecs :as ecs]
            [distributo.ec2 :as ec2]
            [distributo.scheduler :as sched]
            [distributo.allocator :as alloc]
            [distributo.util :refer :all]
            [distributo.aws :refer :all]
            [clj-time.core :as t]))

"Deep Learning with TensorFlow on EC2 Spot Instances and EC2 Container Service"

(defn run-inference
  [cluster num-instances batch-size num-batches output]

  (log/info "Run TensorFlow in cluster" cluster
            "with" num-instances "instances,"
            "with batch size:" batch-size
            "and total batches:" num-batches)

  (let [ec2 (ec2/new-client default-credential-provider-chain)
        ecs (ecs/new-client default-credential-provider-chain)
        cred (-> default-credential-provider-chain (.getCredentials))]

    ;; Register task definition for TensorFlow example container
    (ecs/register-task-definition!
      ecs
      "distributo-tensorflow-example"
      {:image       "ezhulenev/distributo-tensorflow-example"
       :cpu         1000
       :memory      1000
       :environment {:AWS_ACCESS_KEY_ID     (-> cred (.getAWSAccessKeyId))
                     :AWS_SECRET_ACCESS_KEY (-> cred (.getAWSSecretKey))}})

    ;; Allocate EC2 resources on Spot Market
    (alloc/allocate-cluster-resources! ec2 cluster num-instances ec2/default-spot-instance-request)

    ;; Await for at least one request fulfilled
    (alloc/await-fulfillment ec2 cluster 1)

    (let [batches (for [i (range num-batches)] [(* i batch-size) (* (+ 1 i) batch-size)])
          jobs (for [[start end] batches]
                 (sched/mk-job ecs
                               (str "Imagenet:" start ":" end)
                               "distributo-tensorflow-example"
                               [(str start ":" end) (str output "inferred-" start "-" end ".txt")]))]

      (log/info "Number of jobs to run:" (count jobs))

      (sched/run-scheduler! ecs cluster jobs {:update-interval (t/seconds 10)})

      ;;(alloc/free-cluster-resources! ec2 cluster)
      )))

(def cli-options
  [["-i" "--inference" "Run inference on ECS Cluster"]
   ["-c" "--cluster" "EC2 Container Service cluster name"
    :default "distributo"]
   ["-n" "--num-instances COUNT" "Number of EC2 instances to launch"
    :default 1
    :parse-fn #(Long/parseLong %)]
   ["-s" "--batch-size COUNT" "Images batch size"
    :default 100
    :parse-fn #(Long/parseLong %)]
   ["-b" "--num-batches COUNT" "Number of batches to process"
    :default 10
    :parse-fn #(Long/parseLong %)]
   ["-o" "--output" "S3 path to output inferred labels"
    :default "s3://distributo-example/imagenet/"]])

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (:inference options) (run-inference (:cluster options)
                                          (:num-instances options)
                                          (:batch-size options)
                                          (:num-batches options)
                                          (:output options))
      :else (println summary))))
