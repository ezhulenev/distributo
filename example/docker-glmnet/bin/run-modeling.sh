#!/usr/bin/env bash

set -e

die() {
  echo "$@" 1>&2
  exit 1
}

if [ "$#" -ne 2 ]; then
    echo "Usage: run-modeling.sh s3://<input directory> s3://<output directory>"
    die "Illegal number of parameters"
fi

IN=$1
OUT=$2

echo "Input path: $IN. Output path: $OUT"

echo "Prepare model input from: $IN"
aws s3 cp --recursive "$IN" /model/input

mkdir /model/output
Rscript /model/run-modeling.R /model/input /model/output

echo "Upload model results to: $OUT"
aws s3 cp --recursive /model/output "$OUT"
