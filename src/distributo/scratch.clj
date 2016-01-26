;(ns distributo.scratch)
;
;(require '[distributo.ecs :as ecs]
;         '[distributo.ec2 :as ec2]
;         '[distributo.scheduler :as sched]
;         '[distributo.util :refer :all]
;         '[distributo.aws :refer :all])
;
;(def ec2 (ec2/new-client default-credential-provider-chain))
;
;(def ecs (ecs/new-client default-credential-provider-chain))
;
;(ecs/register-task-definition!
;  ecs
;  "distributo-tensorflow-example"
;  {:image "ezhulenev/distributo-tensorflow-example"
;   :cpu 1000
;   :memory 1000
;   :environment {:AWS_ACCESS_KEY_ID "..."
;                 :AWS_SECRET_ACCESS_KEY "..."}})
;
;(def started-task
;  (ecs/start-task!
;    ecs
;    "distributo"
;    "arn:aws:ecs:us-east-1:050689537474:container-instance/281c1722-c51c-410a-ba2e-e10e6d454e20"
;    "distributo-tensorflow-example"
;    ["0:1000" "s3://distributo-example/imagenet/inferred-0-100.txt"]))
;
;(ecs/describe-tasks ecs "distributo" [(:task-arn started-task)])
