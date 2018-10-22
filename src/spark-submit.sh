#!bin/bash
spark-submit --master spark://ec2-52-204-67-201.compute-1.amazonaws.com:7077 --jars ~/flint/target/flint-assembly-0.6.0-SNAPSHOT.jar --num-executors 4 --total-executor-cores 4 --driver-memory 5g --executor-memory 5g /home/ubuntu/Epoch/src/main.py > out.txt

