package spark.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext}

object SparkTransformationOperator {
  /*
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Spark Wrord Count").setMaster("local[2]")

    val sparkContext: SparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("error")
    val text: RDD[String] = sparkContext.textFile("./data/100Rows")
    //先map后flat。与map类似，每个输入项可以映射为0到多个输出项。 我理解成扁平化
    val words: RDD[String] = text.flatMap((line: String) => {
      line.split(" ")
    })
    //将一个RDD中的每个数据项，通过map中的函数映射变为一个新的元素。 特点：输入一条，输出一条数据。
    val pairWords: RDD[(String, Int)] = words.map((word: String) => {Tuple2(word, 1)})
    //过滤符合条件的记录数，true保留，false过滤掉。
    val trueResult: RDD[(String, Int)] = pairWords.filter(pari => {
      null != pari._1 && !pari._1.isEmpty
    })
    //将相同的Key根据相应的逻辑进行处理
    val result: RDD[(String, Int)] = trueResult.reduceByKey((v1: Int, v2: Int) => {
      v1 + v2
    })
    println(s"--------------分区个数: ${result.partitions.length}")
    //作用在K,V格式的RDD上，对key进行升序或者降序排序。
    //    val sortedResult: RDD[(String, Int)] =result.sortByKey()
    val sortedResult: RDD[(String, Int)] =result.sortBy((_: (String, Int))._1,false,1)
    // 随机抽样算子，根据穿进去的小数按比例进行 有返回或者无返回的抽样
    val partSortResult: RDD[(String, Int)] = sortedResult.sample(withReplacement = false,0.01)
    partSortResult.foreach(println)
  }
*/

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("getClick").setMaster("local[1]")
    val sparkContext: SparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("Error")
    //     集合转化RDD方式
    val userRDD: RDD[(Int, String)] = sparkContext.parallelize(List[(Int, String)]((1, "张三"), (2, "李四"), (3, "王五")))
    val user2RDD: RDD[(Int, String)] = sparkContext.parallelize(List[(Int, String)]((5, "赵六"), (6, "田七"), (3, "王五")))
    val userScoreRDD: RDD[(Int, Int)] = sparkContext.parallelize(List[(Int, Int)]((1, 290), (2, 298), (3, 295), (4, 300)))
    println("  join  -------------------------------------------------------")
    /*
    * Join类似于SQL的inner join操作，返回结果是前面和后面集合中配对成功的，过滤掉关联不上的。
    * leftOuterJoin类似于SQL中的左外关联left outer join，返回结果以前面的RDD为主，关联不上的记录为空。
    * rightOuterJoin类似于SQL中的有外关联right outer join，返回结果以参数也就是右边的RDD为主，关联不上的记录为空。
    *
    *
    * */
  /*  userRDD.join(userScoreRDD).foreach(println)
    println("  intersection  ---------------------------------------------------")
    //    取两个数据集的交集，返回新的RDD与父RDD分区多的一致
    userRDD.intersection(user2RDD).foreach(println)

    println("  subtract   -------------------------------------------------------")
    //取两个数据集的差集，结果RDD的分区数与subtract前面的RDD的分区数一致。
    userRDD.subtract(user2RDD).foreach(println)

    println("  union   -----------------------------------------------------")
    userRDD.union(user2RDD).foreach(println)
    println("  cogroup ----------------------------------------------------")
    //cogroup相当于SQL中的全外关联full outer join，返回左右RDD中的记录，关联不上的为空。
    userRDD.cogroup(userScoreRDD).foreach(println)


    userRDD.mapPartitions((ite: Iterator[(Int, String)]) => {
      ite.map((pair: (Int, String)) => {
        (pair._1, pair._2.concat("_P"))
      })
    }).foreach(println)

    println("  userRDD mapPartitionWithIndex ----------------------------------------------------")

    var rdd1 = sparkContext.makeRDD(1 to 5, 2)
    //rdd1有两个分区
    val rdd2: RDD[String] = rdd1.mapPartitionsWithIndex { (x: Int, iter: Iterator[Int]) => {
      var result: List[String] = List[String]()
      var i = 0
      while (iter.hasNext) {
        i = iter.next()
        result=(x + "|" + i)::result
      }
      result.iterator
    }
    }
    //rdd2将rdd1中每个分区的数字累加，并在每个分区的累加结果前面加了分区索引
    rdd2.collect().foreach(println)
    println("=================================================")
    val repartitionRdd1: RDD[Int] = rdd1.repartition(4)
    println(s" rdd1 增加分区 ${repartitionRdd1.partitions.length}")
    val coalesceRdd1: RDD[Int] = repartitionRdd1.coalesce(1,false)
    println(s" rdd1 减少分区 ${coalesceRdd1.partitions.length}")
    println("=================================================")
    val wordsRDD: RDD[(String, Int)] = sparkContext.parallelize(List[(String, Int)](("张", 1), ("赵", 1), ("赵", 1), ("李", 1)))
    val value: RDD[(String, Int)] = wordsRDD.groupByKey().map(one =>{(one._1,one._2.toList.sum)})
    value.foreach(println)
*/
//    ---------------------------------------------------------------------
    /*
    * zip 函数用于将两个RDD组合成Key/value 形式的RDD，这里默认两个RDD的partition数量以及元素数量相同，否则会抛出异常
    *
    * */
    val zipRdd1: RDD[Int] = sparkContext.makeRDD(1 to 6 ,2)
    val zipRdd2: RDD[String] = sparkContext.makeRDD(Seq("A", "B", "C", "D", "E","F"), 2)
    val zipRddIndex: RDD[String] = sparkContext.makeRDD(Seq("A", "B", "C", "D", "E","F","G","H","L","M"), 2)
    val zipRdd3: RDD[String] = sparkContext.makeRDD(Seq("A", "B", "C", "D", "E","F"), 3)
    println(s"    zipRdd1.zip(zipRdd2)      ---------------------------------------------------------------------")
    zipRdd1.zip(zipRdd2).foreach(println)
   println(s"    zipRdd1.zip(zipRdd3)   因为元素数不同所以会抛出异常   ---------------------------------------------------------------------")
//    zipRdd1.zip(zipRdd3).foreach(println)
   println(s"    zipRdd2.zip(zipRdd3)   因为分区数不同所以会抛出异常    ---------------------------------------------------------------------")
//    zipRdd2.zip(zipRdd3).foreach(println)
    /*
    *zipPartitions函数将多个RDD按照paritition 组合成为新的RDD。该函数需要组合的RDD具有相同的分区数，但对于每个分区内的元素数量没有要求
    * */

    /*
    * zipWithIndex 该函数将RDD中的元素和这个元素在RDD中的ID（索引号）组合成键/值对
    * */
    println(s"    zipWithIndex  ---------------------------------------------------------------------")
    zipRdd2.zipWithIndex().foreach(println)
    /*
    zipWithUniqueId 该函数将RDD中元素和一个唯一ID组合成键值对，该唯一ID称称算法如下:
      每个分区中第一个元素的的唯一ID为: 该分区索引号
      每个分区中第N个元素的唯一ID为（前一个元素的唯一ID值）+(该RDD总的分区数)
    //总分区数为2
    //第一个分区第一个元素ID为0，第二个分区第一个元素ID为1
    //第一个分区第二个元素ID为0+2(该RDD总的分区数是2)=2，第一个分区第三个元素ID为2+2(该RDD总的分区数是2)=4
    //第二个分区第二个元素ID为1+2(该RDD总的分区数是2)=3，第二个分区第三个元素ID为3+2(该RDD总的分区数是2)=5
    */
    println(s"    zipWithUniqueId  ---------------------------------------------------------------------")
    zipRdd2.zipWithUniqueId().mapPartitionsWithIndex((x ,ite)=>{
      val list: List[(String, Long)] = ite.toList
      list.foreach(
        (row: (String, Long)) =>{
          println(s"第 $x 个分区  , ${row.swap.toString()}    , index = ${list.indexOf((row._1,row._2))}  ${row.toString()} ")
        }
      )
      ite
    }).collect()


//    ---------------------------------------------------------------------
    sparkContext.stop()
  }
}
