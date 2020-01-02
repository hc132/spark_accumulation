package demo

import org.apache.spark.{SparkConf, SparkContext}

object CountUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("CountUV")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("userLog")
    val result1 = rdd.filter(!_.split(" ")(2).contains("null"))

    val result2 = result1.map(x => {
      (x.split(" ")(3), x.split(" ")(2))
    })
    println(result2.collect().toList)
    val allResult = result2.distinct().countByKey()

    println(allResult.toList)
    allResult.foreach(x => {
      println("PageId: " + x._1 + "\tUV: " + x._2)
    })
    sc.stop();
  }
}
