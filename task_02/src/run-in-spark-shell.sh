spark-shell \
--master spark://master:7077 \
--num-executors 2 \
--driver-memory 8G \
--executor-memory 4G \
--executor-cores 2 \
--conf spark.dynamicAllocation.enabled=false \
-i /user_data/projeto/pdle/task_02/src/main/scala/02_lr.scala