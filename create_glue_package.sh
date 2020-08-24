#!/usr/bin/env bash

mkdir -p libs dist
rsync -avz spark_main/ libs/ --exclude=__pycache__
# pip install -r prod_requirements.txt -t libs
(cd libs; zip -r ../dist/aws_glue_pyspark.zip *)
# rm -rf libs
