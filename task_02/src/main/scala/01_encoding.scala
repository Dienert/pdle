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

var one_hot_encoder: Array[PipelineStage] = Array(new OneHotEncoder().
    setInputCol("label_index").
    setOutputCol("label_vec"))

val pipelineTmp = new Pipeline().
    setStages(indexer ++ one_hot_encoder)

val df = pipelineTmp.fit(raw_DF).
    transform(raw_DF).
    select("label_vec", "features")

val assembler = new VectorAssembler().
    setInputCols(Array("label_vec")).
    setOutputCol("label")

val output = assembler.transform(df).
    select("label", "features")
    
output.write.format("parquet").
    save("hdfs://master:8020/bigdata/pt7_indexed_enconded_data")   