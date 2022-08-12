import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.{SQLContext}
import org.apache.spark.ml.{PipelineStage, Pipeline}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

object Encoding {
  def main(args: Array[String]) {
    val logFile = "/logs/simple.log" // Should be some file on your system
    val conf = new SparkConf().setAppName("Encoding")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    
    val sqlContext = new SQLContext(sc)
    val raw_DF = sqlContext.read.parquet("hdfs://master:8020/bigdata/pt7-hash.parquet")
    val textDF = raw_DF.select("label")

    val indexer: Array[PipelineStage] = Array(new StringIndexer().
        setInputCol("label").
        setOutputCol("label_index"));

    val pipelineTmp = new Pipeline().
        setStages(indexer)

    val df = pipelineTmp.fit(raw_DF).
        transform(raw_DF).
        select("label_index", "features")

    val assembler = new VectorAssembler().
        setInputCols(Array("features")).
        setOutputCol("features_vec")

    val output = assembler.transform(df).
        select("label_index", "features_vec")

    val final_output = output.withColumnRenamed("label_index", "label").withColumnRenamed("features_vec", "features")
        
    final_output.write.format("parquet").
        save("hdfs://master:8020/bigdata/pt7_indexed_enconded_data")   

    }
}