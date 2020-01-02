package PV_UV

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SparkPVUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("./data/pvuvdata")


    //每个网址的每个地区访问量 ，由大到小排序
/*    数据示例:
      102.72.34.59	吉林	2019-11-20	1574219099783	8858063761240834596	www.baidu.com	Regist
      30.206.174.123	青海	2019-11-20	1574219099783	6822798932447092200	www.taobao.com	Buy
      214.183.42.47	重庆	2019-11-20	1574219099783	5110610430805046079	www.mi.com	Comment*/
    // site_local  [( www.baidu.com,102.72.34.59 ),(www.taobao.com,30.206.174.123)]
    val site_local: RDD[(String, String)] = lines.map(line => { (line.split("\t")(5), line.split("\t")(1))})

    val site_localIterable = site_local.groupByKey()
    val result = site_localIterable.map(one => {

      val localMap = mutable.Map[String, Int]()
      val site = one._1
      val localIter = one._2.iterator
      while (localIter.hasNext) {
        val local = localIter.next()
        if (localMap.contains(local)) {
          val value = localMap.get(local).get
          localMap.put(local, value + 1)
        } else {
          localMap.put(local, 1)
        }
      }

      val tuples: List[(String, Int)] = localMap.toList.sortBy(one => {
        one._2
      })
      if(tuples.size>3){
        val returnList = new ListBuffer[(String, Int)]()
        for(i <- 0 to 2){
          returnList.append(tuples(i))
        }
        (site, returnList)
      }else{
        (site, tuples)
      }
    })
    result.foreach(println)

    //pv
//    lines.map(line=>{(line.split("\t")(5),1)}).reduceByKey((v1:Int,v2:Int)=>{
//      v1+v2
//    }).sortBy(tp=>{tp._2},false).foreach(println)

    //uv
//    lines.map(line=>{line.split("\t")(0)+"_"+line.split("\t")(5)})
//      .distinct()
//      .map(one=>{(one.split("_")(1),1)})
//      .reduceByKey(_+_)
//      .sortBy(_._2,false)
//      .foreach(println)


  }
}
