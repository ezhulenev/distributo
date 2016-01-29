# Distributo

[![Build Status](https://travis-ci.org/ezhulenev/distributo.svg?branch=master)](https://travis-ci.org/ezhulenev/distributo)

Resource allocator and scheduler for running Dockerized jobs on top of Amazon EC2 Container Service 
cluster using EC2 Spot Instances.

This project is not even alpha version, it's just a proof of concept with example showing
how to run with it Deep Learning with TensorFlow using cheap EC2 spot instances as computation resource.

# Overview

### Resource Allocator

Resource allocator is responsible for allocating compute resources in EC2 based in outstanding 
jobs resource requirements. Right now it's only dummy implementation that can support fixed
size ECS cluster built from spot instances. You need to define upfront how many instances do you need.

### Scheduler

Scheduler decides on what available container instance to start pending jobs. It's using bin-packing 
with fitness calculators (concept borrowed from [Netflix/Fenzo](https://github.com/Netflix/Fenzo)) to 
choose best instance to start new task. It's the main difference from default ECS scheduler that
places tasks on random instances.

# Building

This project is written in Clojure and use [Leiningen](http://leiningen.org/) build tool.

    lein compile
    lein test

# Examples

## Deep Learning with TensorFlow on EC2 Spot Instances

### TensorFlow Docker Image

[Docker image](https://github.com/ezhulenev/distributo/tree/master/example/docker-tensorflow) based on official TensorFlow Docker image 
and [Image Recognition Tutorial](https://www.tensorflow.org/versions/0.6.0/tutorials/image_recognition/index.html).

It takes 2 arguments: 
 1. Range of images that needs to be classified: 0:1000 - first 10000 images from http://image-net.org/imagenet_data/urls/imagenet_fall11_urls.tgz
 2. S3 path where to put classification result: s3://distributo-example/imagenet/inferred-0-1000.txt
 
In order to run it you'll also have to provide your AWS credentials. If you will not provide credentials it will still
run inference, but will fail at the very end trying to upload final file.

    docker run -it -e 'AWS_ACCESS_KEY_ID=...' -e 'AWS_SECRET_ACCESS_KEY=...' \
        ezhulenev/distributo-tensorflow-example \
        0:1000 s3://distributo-example/imagenet/inferred-0-1000.txt
        

### Run TensorFlow with Distributo

Distributo uses AWS JAVA SDK to access your AWS credentials. If you don't have them already configured you
can do it with AWS CLI

    aws configure
    
After that you can start you cluster and run TensorFlow inference with this command:    

    lein run --inference \
      --num-instances 1 \
      --batch-size 100 \
      --num-batches 10 \
      --output s3://distributo-example/imagenet/
       
Distributo doesn't free resources after it's done with inference, to be able to do multiple runs
one by one. If you are done, don't forget to clean resources:

    lein run --free-resources
        
# Future Work

Resource allocator and scheduler could be much more clever about their choices of regions, availability zones 
and instance types to be able to build most price-effective cluster out of resources currently 
available on spot market.
        
# License

Copyright 2016 Eugene Zhulenev. Distributo is licensed under Apache License v2.0.
