# Image classification with TensorFlow ImageNet model

Example of running image classification using pre-trained ImageNet model for TensorFlow 

Container takes 2 arguments: 
 1. Range of images that needs to be classified from: http://image-net.org/imagenet_data/urls/imagenet_fall11_urls.tgz
 2. S3 path for inference output

### Example: Classify first 1000 images

AWS credentials required to access S3.

    docker run -it \
      -e 'AWS_ACCESS_KEY_ID=<...>' \
      -e 'AWS_SECRET_ACCESS_KEY=<...>' \
      ezhulenev/distributo-tensorflow-example 0:1000 s3://distributo-example/imagenet/inferred-0-100.txt
