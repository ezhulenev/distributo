# Docker container with R and glmnet

Example of simple binomial model with R and glmnet, packaged into Docker container. 

R script takes 2 arguments: 
 1. S3 path to input files
 2. S3 path where to put output

### Example

AWS credentials required to access S3

    docker run -it \
      -e 'AWS_ACCESS_KEY_ID=<...>' \
      -e 'AWS_SECRET_ACCESS_KEY=<...>' \
      ezhulenev/distributo-glmnet-example s3://<input> s3://<output>
