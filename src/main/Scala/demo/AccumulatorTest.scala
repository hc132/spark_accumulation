package demo

import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object AccumulatorTest {


  def main(args: Array[String]): Unit = {
    //        val session: SparkSession = SparkSession.builder().appName("Accumulator").master("local").getOrCreate()
    //        val sc = session.sparkContext
    val conf = new SparkConf().setAppName("Accumulator").setMaster("local")
    val sc = new SparkContext(conf)
    //        val accuValue: Accumulator[Int] = sc.accumulator(0, "My Accumulator")
    val accuValue: LongAccumulator = sc.longAccumulator
    val listValue: RDD[String] = sc.parallelize(List[String]("s", "2", "4"))
    val rows = sc.textFile("./data/100Rows")

    listValue.cache()
    rows.foreach(
      oneRow => {
        accuValue.add(1);
        println(s" Executor value is ${accuValue.value}")
        oneRow
      }
    )

    //    val x =0
    //    for(x  <- 1 to 100 ){
    //      accuValue.add(1)
    //      println(s" accuValue value ${accuValue.value}")
    //    }
    listValue.collect()
    println(s" driver value ${accuValue.value}")
    sc.stop()
  }

}
