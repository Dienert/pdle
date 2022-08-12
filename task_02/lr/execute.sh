$SPARK_HOME/bin/spark-submit \
  --class "LR" \
  --master yarn \
  --driver-memory 4g \
  target/scala-2.11/lr_2.11-1.0.jar