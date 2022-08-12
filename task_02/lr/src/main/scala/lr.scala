import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.feature.StringIndexer

val df = spark.read.format("parquet").load("hdfs://master:8020/bigdata/pt7_indexed_enconded_data")

val labelIndexer: StringIndexer = new StringIndexer().
  setInputCol("features").
  setOutputCol("label")


val lr = new LogisticRegression().
  setMaxIter(10).
  setRegParam(0.3).
  setElasticNetParam(0.8)

val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

val pipeline = new Pipeline().
  setStages(Array(lr))

val paramGrid = new ParamGridBuilder().
  addGrid(lr.regParam, Array(0.1, 0.01)).
  build()

val evaluator = new MulticlassClassificationEvaluator().setLabelCol(labelIndexer.getOutputCol)

val cv = new CrossValidator().
  setEstimator(pipeline).
  setEvaluator(evaluator).
//   .setEvaluator(new MulticlassClassificationEvaluator)
  setEstimatorParamMaps(paramGrid).
  setNumFolds(2).  // Use 3+ in practice
  setParallelism(2)  // Evaluate up to 2 parameter settings in parallel

val cvModel = cv.fit(trainingData)