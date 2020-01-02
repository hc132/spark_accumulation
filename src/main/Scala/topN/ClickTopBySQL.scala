package topN

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

class ClickTopBySQL {

}
object ClickTopBySQL{
/*  // spark 普通SQL版本
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("clicksql").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("error")
    val ucDataFrame: DataFrame = spark.read.csv("./data/uData.csv").toDF("uid","pid")
    println("******************************************")
    ucDataFrame.show()
    ucDataFrame.createTempView("ucTable")
    println("------------------------------------------")
    val pcDataFrame: DataFrame = spark.read.csv("./data/pData.csv").toDF("ppid","cid")
    pcDataFrame.show()
    pcDataFrame.createTempView("pcTable")
    val resultFrame: DataFrame = spark.sql("select * from ucTable inner join pctable on pid=ppid where  cid is not null")
    resultFrame.show()
  }
*/

/*
  def main(args: Array[String]): Unit = {

    // 快速求出用户点击过的商品类目
    val conf: SparkConf = new SparkConf().setAppName("getClick").setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Error")
    //广播变量方式实现join
    val uData: RDD[String] = sc.textFile("./data/uData.csv")
    val pData: RDD[String] = sc.textFile("./data/pData.csv")
    val uRDD: RDD[(String, String)] = uData.map(line => {  line.split(",") }).map(arr => {   Tuple2(arr(1), arr(0))    })
    val pRddMap: collection.Map[String, String] = pData.map(line => {  line.split(",") }).map(arr => {   Tuple2(arr(0), arr(1))    }).collectAsMap()
    val broadcastValue: Broadcast[collection.Map[String, String]] = sc.broadcast(pRddMap)
    println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    val resultValue: RDD[(String, String)] = uRDD.filter((onePair: (String, String)) => {
      val pRddMapValue: collection.Map[String, String] = broadcastValue.value
      val joinKey: String = onePair._1
      val maybeString: Option[String] = pRddMapValue.get(joinKey)
      null != maybeString && maybeString.isDefined
    })
    println("_______________________________________")
    resultValue.foreach(println)
    println("+++++++++++++++++++++++++++++++++++++++++++++")
    val restult = resultValue.map(pair => {
      val pRddMapValue: collection.Map[String, String] = broadcastValue.value
      (pair._1, pRddMapValue.get(pair._1))
    })
    restult.foreach(println)
  }*/
  //spark sql 实现mapjoin
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("clicksql").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("error")
    val ucDataFrame: DataFrame = spark.read.csv("./data/uData.csv").toDF("uid","pid")
    println("******************************************")
    ucDataFrame.show()
    ucDataFrame.createTempView("ucTable")
    println("------------------------------------------")
    val pcDataFrame: DataFrame = spark.read.csv("./data/pData.csv").toDF("ppid","cid")
    pcDataFrame.show()
    pcDataFrame.createTempView("pcTable")
    spark.sql("CACHE TABLE t_Table AS SELECT ppid,cid FROM pcTable")
    println("++++++++++++++++++++++++++++++++++++++++++++++")
    val frame: DataFrame = spark.sql("select * from  t_Table ")
    frame.show()
    val resultFrame: DataFrame = spark.sql("select * from ucTable inner join t_Table on pid=ppid where  cid is not null")
    resultFrame.show()
  }
}