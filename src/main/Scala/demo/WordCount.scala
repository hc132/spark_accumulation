
package demo

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC")
    val sc = new SparkContext(conf)
    val text = sc.textFile("word.txt")

    val word = text.flatMap(line => (line.split(" ")))
    val wordKV = word.map(word => (word, 1))
    val array = wordKV.reduceByKey(_ + _)
    array.foreach(println _)
//    println(array.sortBy(_._2, false).collect().toList)
    val topN = array.map(x => (x._2, x._1)).sortByKey(false).top(3).map(y => (y._2, y._1))
    println(topN.toList)
    sc.stop()
  }

}
