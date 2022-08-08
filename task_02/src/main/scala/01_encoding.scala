import org.apache.spark.ml.{PipelineStage, Pipeline}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}

val raw_DF = spark.read.format("parquet").
    load("hdfs://master:8020/bigdata/pt7-hash.parquet")
//raw_DF.groupBy("label").count.sort(desc("count")).show()
val textDF = raw_DF.select("label")

val originalColumns = raw_DF.columns
val allIndexedCategoricalColumns = textDF.columns
val originalColumns = allIndexedCategoricalColumns

val indexer: Array[PipelineStage] = Array(new StringIndexer().
    setInputCol("label").
    setOutputCol("label_index").
    setHandleInvalid("skip"));

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