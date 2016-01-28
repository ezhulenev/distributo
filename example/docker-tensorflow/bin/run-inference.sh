#!/usr/bin/env bash

set -e

die() {
  echo "$@" 1>&2
  exit 1
}

if [ "$#" -ne 2 ]; then
    echo "Usage: run-inference.sh start:end s3://<output path>"
    die "Illegal number of parameters"
fi

RANGE=$1
OUT=$2

echo "Images range: $RANGE. Output path: $OUT"

python /tensorflow/imagenet/classify_image.py --images_range $RANGE

echo "Upload model results to: $OUT"
aws s3 cp /inferred.txt "$OUT"
