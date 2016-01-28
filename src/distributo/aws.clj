(ns distributo.aws
  (:import (com.amazonaws.auth EnvironmentVariableCredentialsProvider DefaultAWSCredentialsProviderChain AWSCredentialsProvider)))

(def env-credentials-provider ^AWSCredentialsProvider
  (EnvironmentVariableCredentialsProvider.))

(def default-credential-provider-chain ^AWSCredentialsProvider
  (DefaultAWSCredentialsProviderChain.))
