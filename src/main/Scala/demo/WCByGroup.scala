
package demo

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
/**
  * Created on 2019/8/26 15:34
  *
  * @author : HC
  */

  object WCByGroup {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("WC1").setMaster("local[1]")
      val sc = new SparkContext(conf)
      val text = sc.textFile("result.txt")
      val result = text.map(i => {
        //切分记录的各个字段, 并将记录转换成kv对
        val cols = i.split(" ")
        (cols(0), cols(1).toInt)
      })
        .partitionBy(new HashPartitioner(2))	//将记录按key重新分区, 相同的key发送的同一个partition
        .mapPartitions(p => {
        p.toList
          .groupBy(_._1)	//分区内按key分组
          .mapValues(_.sortBy(_._2).reverse.slice(0, 3))	//组内按排序列(假设第二列)降序排序, 并取TOPN(假设top3)
          .values.reduce(_:::_).iterator	//将各组key的值拼接在一起作为分区结果返回
      })
      println("================>>> " + result.collect().toList)

    }

  }
