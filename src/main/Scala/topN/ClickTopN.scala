package topN

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ClickTopN {
  def main(args: Array[String]): Unit = {
    /*
    *   userud , productid
    *   17654920,150332988
    *   17654920,150440980
    *   17008942,155347895
    *
    *   producid,category_id
    *   150332988,2900345
    *   155347895,2983345
    * */

    // 快速求出用户点击过的商品类目

    val conf: SparkConf = new SparkConf().setAppName("getClick").setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Error")

        //读取文件方式创建RDD
        val uData = sc.textFile("./data/uData.csv")
        val pData = sc.textFile("./data/pData.csv")

        val uRDD: RDD[(String, String)] = uData.map(line => {  line.split(",") }).map(arr => {   Tuple2(arr(1), arr(0))    })
        val pRDD: RDD[(String, String)] = pData.map(line => {  line.split(",") }).map(arr => {   Tuple2(arr(0), arr(1))    })
        val result= uRDD.join(pRDD)
        println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        result.foreach(println)
        println("_______________________________________")
        result.map(clickResult => {
            clickResult._2._2
        })foreach(println)

    // 集合转化RDD方式
//    val ucRDD: RDD[(Int, Int)] = sc.parallelize(List[(Int, Int)]((17654920, 150332988), (17654920, 150440980), (17008942, 155347895)))
//    val pcRDD: RDD[(Int, Int)] = sc.parallelize(List[(Int, Int)]((150332988, 2900345), (155347895, 2983345)))
//    val ucPariRDD: RDD[(Int, Int)] = ucRDD.map(pair => { (pair._2, pair._1)  })
//    println("_-------------------------------------------------------")
//    ucPariRDD.join(pcRDD).map(joinResult => {
//      joinResult._2._2
//    }).foreach(println)


  }

}
