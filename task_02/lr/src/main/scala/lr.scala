import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.{SQLContext}

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.feature.StringIndexer

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object LR {
  def main(args: Array[String]) {
    val logFile = "/logs/simple.log" // Should be some file on your system
    val conf = new SparkConf().setAppName("Encoding")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.parquet("hdfs://master:8020/bigdata/pt7_indexed_enconded_data")

    val labelIndexer: StringIndexer = new StringIndexer().
      setInputCol("features").
      setOutputCol("label")

    val lr = new LogisticRegression().
      setMaxIter(10).
      setRegParam(0.3).
      setElasticNetParam(0.8)

    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3), seed = 11L)

    val pipeline = new Pipeline().
      setStages(Array(lr))

    val paramGrid = new ParamGridBuilder().
      addGrid(lr.regParam, Array(0.1, 0.01)).
      build()

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol(labelIndexer.getOutputCol)

    val cv = new CrossValidator().
      setEstimator(pipeline).
      setEvaluator(evaluator).
      setEstimatorParamMaps(paramGrid).
      setNumFolds(2)  // Use 3+ in practice

    val cvModel = cv.fit(trainingData)

    val results =  cvModel.
      transform(testData).
      select("prediction", "label")

    // Compute raw scores on the test set
    val predictionAndLabels = results.
      rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))

    // Instantiate metrics object
    val metrics = new MulticlassMetrics(predictionAndLabels)

    // Confusion matrix
    println("Confusion matrix:")
    println(metrics.confusionMatrix)

    // Overall Statistics
    val accuracy = metrics.accuracy
    println("Summary Statistics")
    println(s"Accuracy = $accuracy")

    // Precision by label
    val labels = metrics.labels
    labels.foreach { l =>
      println(s"Precision($l) = " + metrics.precision(l))
    }

    // Recall by label
    labels.foreach { l =>
      println(s"Recall($l) = " + metrics.recall(l))
    }

    // False positive rate by label
    labels.foreach { l =>
      println(s"FPR($l) = " + metrics.falsePositiveRate(l))
    }

    // F-measure by label
    labels.foreach { l =>
      println(s"F1-Score($l) = " + metrics.fMeasure(l))
    }

    // Weighted stats
    println(s"Weighted precision: ${metrics.weightedPrecision}")
    println(s"Weighted recall: ${metrics.weightedRecall}")
    println(s"Weighted F1 score: ${metrics.weightedFMeasure}")
    println(s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}")

    cvModel.save("hdfs://master:8020/bigdata/models/lr")

    }
}