# Large scale data processing project

1. Assemble the cluster
1. In the master command shell, install git:
```
apt install git -y
```
3. Change the folder to /user_data and clone the project:
```
cd /user_data
git clone https://github.com/Dienert/pdle.git
```

4. Restart the cluster
```
/user_data/admin/avertlux.sh
/user_data/admin/fiatlux.sh
```

<!-- spark-shell
// scala version
util.Properties.versionMsg
// spark version
sc.version  -->

## Task 01

1. Listing the folders of the HDFS to see if something is there:
```
hadoop fs -ls /
```

2. Since there isn't anything there, let's create the folder we will work and verify it has been created:
```
hadoop fs -mkdir -p /bigdata/
hadoop fs -ls /
```

3. Copying files to HDFS
```
hadoop fs -put /user_data/pdle/data/pt7-raw hdfs://master:8020/bigdata/
```

4. Verifying the files have been copied
```
hadoop fs -ls /bigdata/pt7-raw/
```

5. Processing the first job: labels-pt7-raw.scala
```
spark-shell --master spark://master:7077 -i /user_data/pdle/task_01/labels-pt7-raw.scala
```

6. As the command above has opened the scala shell, we can verify the script has been executed showing the transformed data
```scala
val df = { 
	spark.read
	.format("parquet")
	.load("hdfs://master:8020/bigdata/pt7-multilabel")
	.withColumnRenamed("_c0","label")
	.withColumnRenamed("_c1","url")
	.withColumnRenamed("_c3","text64byte")
};df.show()
```

This is the expected result for this command, as shown in the project specification of Task 01.

<img src="images/output_01.png" alt="Output from item 6"/>


<!-- // sem truncar
//df.show(false) 

// Limpando todas as variáveis do spark-shell
:reset -->

7. Still in the scala shell, we can count the number of registers by label
```scala
df.groupBy("label").count.show()
```

8. Or we can count the number of registers by label and order them in a decreasing way
```scala
df.groupBy("label").count.sort(desc("count")).show()
```
Expected result for this command:

<img src="images/output_02.png" alt="Output from item 8"/>


9. Processing the second job: etl-pt7.scala
```
spark-shell --master spark://master:7077 -i /user_data/pdle/task_01/etl-pt7.scala
```

// If the spark-shell is already active, we can execute the same comand in the following maner:
:load /user_data/pdle/task_01/etl-pt7.scala

10. To verify the script result, we can do the following code in spark-shell (scala):
```scala
val df2 = { 
	spark.read
	.format("parquet")
	.load("hdfs://master:8020/bigdata/pt7-hash.parquet")
};df2.show()
```

<img src="images/output_03.png" alt="Output from item 10"/>
This is the expected result for this command, as shown in the project specification of Task 01 for the second job

<br />

> **Warning**
> To avoid Java Heap Space, we suggest restarting the cluster to continue to the next task with the following commands as in section 4:
```
/user_data/admin/avertlux.sh
/user_data/admin/fiatlux.sh
```

## Task 02

/user_data/pdle/task_02/src/run-in-spark-shell.sh /user_data/pdle/task_02/src/main/scala/01_encoding.scala

1. Install the sbt command in the cluster. This command will generate a jar so each scala script can be executed in the Yarn Cluster.
```
sh /user_data/pdle/task_02/install_sbt.sh
```

hadoop fs -mkdir /logs

2. Build the package of the enconding
```
cd /user_data/pdle/task_02/encoding
sbt package
```

1. Transforming the labels into numbers and the features as VectorAssembler
```
spark-shell --master spark://master:7077 -i /user_data/pdle/task_02/src/main/scala/01_encoding.scala
```

or the following command with the spark-shell is already open:
```
:load /user_data/logisticRegression.scala
```

> **Warning**
> If you get the following error message:

```scala
org.apache.spark.sql.AnalysisException: path hdfs://master:8020/bigdata/pt7_indexed_enconded_data already exists.
```

Execute the following command to delete the existing folder in normal shell, not in spark-shell (open a new one or excute ctrl+c to leave spark-shell):
```
hadoop fs -rm -r -f /bigdata/pt7_indexed_enconded_data
```

The command was successful if the following output has been shown:
```
Deleted /bigdata/pt7_indexed_enconded_data
```

Now repeat the command 1 of Task 02.

02_lr.scala



import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}

val df = spark.read.format("parquet").load("hdfs://master:8020/bigdata/pt7_indexed_enconded_data")

val lr = new LogisticRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

val pipeline = new Pipeline().setStages(Array(lr))

val paramGrid = new ParamGridBuilder()
  .addGrid(lr.regParam, Array(0.1, 0.01))
  .build()

val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(new BinaryClassificationEvaluator)
//   .setEvaluator(new MulticlassClassificationEvaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(2)  // Use 3+ in practice
  .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel

val cvModel = cv.fit(trainingData)


* References

https://stackoverflow.com/questions/50384279/why-paramgridbuilder-scala-error-with-countvectorizer

https://spark.apache.org/docs/latest/ml-pipeline.html

https://spark.apache.org/docs/latest/ml-pipeline.html#model-selection-hyperparameter-tuning

https://spark.apache.org/docs/latest/ml-classification-regression.html#logistic-regression

https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/evaluation/MulticlassClassificationEvaluator.html

https://spark.apache.org/docs/latest/ml-tuning.html

https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html

https://spark.apache.org/docs/1.2.0/quick-start.html#self-contained-applications
