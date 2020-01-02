package PV_UV

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CountPV_UV {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("pv_uv");

    var sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Error")
    val dataRDD = sc.textFile("./data/pvuvdata")

    dataRDD.persist(StorageLevel.MEMORY_ONLY_SER)
    //统计pv  统计每一个页面的一天时间内的浏览个数
    // 统计文件内 每个网址的去重之后的IP个数
    dataRDD.take(10).foreach(println)
    println("--------------------------------------------")
    val viewRDD: RDD[String] = dataRDD.map(line => {
      val strings: Array[String] = line.split("\t")
      val viewString = strings.filter(f => {
        f.equals("View") // 这里过滤时间
      })
      strings(5)
    })
    val restult: RDD[(String, Int)] = viewRDD.map(x =>{ (x,1)}).reduceByKey(_+_).sortBy(_._2)
    restult.foreach(println)
  }
}
