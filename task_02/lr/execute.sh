$SPARK_HOME/bin/spark-submit \
  --class "LR" \
  --master yarn \
  --driver-memory 4g \
  target/scala-2.10/lr_2.10-1.0.jar