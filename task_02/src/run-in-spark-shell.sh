spark-shell
--master spark://master:7077
--num-executors 2
--driver-memory 8G
--executor-memory 8G
--executor-cores 4
-i /user_data/projeto/pdle/task_02/src/main/scala/02_lr.scala