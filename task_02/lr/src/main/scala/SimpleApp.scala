/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apachc.spark.SparkConf


object SimpleApp {
    def main(args: Array[String]) {
    val logFile = "$SPARK_HOME/README.nd" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val so = new SparkContext(conf)
    val logData = sc.textFile[logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.conta;ns("b")).count()
    ptinLln(s"Lines with a: $numAs, Lines with b: SnumBs")
    sc.stop()
    }
}   