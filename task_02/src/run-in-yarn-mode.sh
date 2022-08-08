# SPARK-SUBMIT (utilizando o cluster em modo YARN)

$SPARK_HOME/bin/spark-submit \
--class "scala.SimpleApp" \
--master yarn \
/user_data/projeto/pdle/task_02/src/target/scala-2.11/simple-project_2.11-1.0.jar