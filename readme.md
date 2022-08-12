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

This is the expected result of this command:

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


<img src="images/output_01.png" alt="Output from item 6"/>


// Executando segundo script
// spark-shell --master spark://master:7077 -i /user_data/etl-pt7.scala
// com o spark-shell já ativo, pode-se rodar da seguinte maneira:
:load /user_data/etl-pt7.scala

// Vendo resultado do segundo script

val df2 = { 
	spark.read
	.format("parquet")
	.load("hdfs://master:8020/bigdata/pt7-hash.parquet")
}

df2.show()

df2.groupBy("label").count.sort(desc("count")).show()

spark-shell --master spark://master:7077 -i /user_data/logisticRegression.scala

:load /user_data/encoding.scala

:load /user_data/logisticRegression.scala






01_encoding.scala

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder

val raw_DF = spark.read.format("parquet").load("hdfs://master:8020/bigdata/pt7-hash.parquet")
//raw_DF.groupBy("label").count.sort(desc("count")).show()
val textDF = raw_DF.select("label")

val originalColumns = raw_DF.columns
val allIndexedCategoricalColumns = textDF.columns
val originalColumns = allIndexedCategoricalColumns

val indexer: Array[PipelineStage] = Array(new StringIndexer().setInputCol("label").setOutputCol("label_index").setHandleInvalid("skip"));
var one_hot_encoder: Array[PipelineStage] = Array(new OneHotEncoder().setInputCol("label_index").setOutputCol("label_vec"))

val pipelineTmp = new Pipeline().setStages(indexer ++ one_hot_encoder)
val df = pipelineTmp.fit(raw_DF).transform(raw_DF)

val output = df.select("label_vec", "features").withColumnRenamed("label_vec", "label")
output.write.format("parquet").save("hdfs://master:8020/bigdata/pt7_indexed_enconded_data")   


hadoop fs -rm -r -f /bigdata/pt7_indexed_enconded_data



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
