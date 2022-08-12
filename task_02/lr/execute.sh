$SPARK_HOME/bin/spark-submit \
  --class "Encoding" \
  --master yarn \
  --deploy-mode cluster \
  target/scala-2.10/lr_2.10-1.0.jar