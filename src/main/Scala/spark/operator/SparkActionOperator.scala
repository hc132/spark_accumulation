package spark.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SparkActionOperator {
    def main(args: Array[String]): Unit = {
      val conf: SparkConf = new SparkConf().setAppName("Spark Wrord Count").setMaster("local[2]")

      val sparkContext: SparkContext = new SparkContext(conf)
      sparkContext.setLogLevel("error")
      val text: RDD[String] = sparkContext.textFile("./data/100Rows")

      //返回数据集中的元素数。会在结果计算完成后回收到Driver端。
      val count: Long = text.count()
      println(s" count : $count")
      val array: Array[String] = text.collect()
      //返回一个包含数据集前n个元素的集合。
      val takeStr: Array[String] = array.take(10)
      //first=take(1),返回数据集中的第一个元素。
      val str: String = text.first()
      println(s" str : $str")
      //循环遍历数据集中的每个元素，运行相应的逻辑。
      takeStr.foreach(println)
      println("============================================")
      text.foreachPartition(ite =>{
        ite.toList.take(1).foreach(println)
      })
      // 将RDD的内容封装到一个数组中,这个方法应该只在预期得到的数组很小的情况下使用，因为所有的数据都加载到Driver内存中。
      text.collect()

      val wordsRDD: RDD[(String, Int)] = sparkContext.parallelize(List[(String, Int)](("张", 1), ("赵", 1), ("赵", 1), ("李", 1)))
      println("  countByValue action   +=======================================================================")
      wordsRDD.countByValue().foreach(println)
      println("  countByKey action   +=======================================================================")
      wordsRDD.countByKey().foreach(println)
      sparkContext.stop()

    }
  //**************************  Persistence operator   **************************
  /* 控制算子有三种，cache,persist,checkpoint，以上算子都可以将RDD持久化，持久化的单位是partition。
     cache和persist都是懒执行的。必须有一个action类算子触发执行。
     checkpoint算子不仅能将RDD持久化到磁盘，还能切断RDD之间的依赖关系。
     */
  /*
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Spark Wrord Count").setMaster("local[2]")

    val sparkContext: SparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("error")
    val text: RDD[String] = sparkContext.textFile("./data/100Rows")
    /*
    * cache 默认是将RDD的数据持久化到内存中，而且是懒执行
    * 注意:  def cache(): this.type = persist()
    *
    */
    text.cache()
    val unCacheStartTime: Long = System.currentTimeMillis()
    println(s"开始时间: $unCacheStartTime")
    println(s" uncache count numbers : ${text.count()}")
    val unCacheEndTime: Long = System.currentTimeMillis()
    println(s"结束时间: $unCacheEndTime")
    println(s" 未执行缓存消耗时间 ${unCacheEndTime - unCacheStartTime} 毫秒")
    val cachedStartTime: Long = System.currentTimeMillis()
    println(s" cached count numbers : ${text.count()}")
    val cachedEndTime: Long = System.currentTimeMillis()
    println(s" 执行缓存消耗时间 ${cachedEndTime - cachedStartTime} 毫秒")
    /*
     persist：
    可以指定持久化的级别。最常用的是MEMORY_ONLY和MEMORY_AND_DISK。”_2”表示有副本数。
    持久化级别如下：
                  1.MEMORY_ONLY(不常用)
                    使用未序列化的对象格式，将数据保存在内存中。如果内存不够存放所有的数据，则数据可能就不会进行持久化。那么下次对这个RDD执行算子操作时，那些没有被持久化的数据，需要从源头处重新计算一遍。
                    这是默认的持久化策略，使用cache()方法时，实际就是使用的这种持久化策略。

                  2.MEMORY_AND_DISK(不常用)
                    使用未序列化的对象格式，优先尝试将数据保存在内存中。如果内存不够存放所有的数据，会将数据写入磁盘文件中，下次对这个RDD执行算子时，持久化在磁盘文件中的数据会被读取出来使用。

                  3.MEMORY_ONLY_SER(常用)
                    基本含义同MEMORY_ONLY。唯一的区别是，会将RDD中的数据进行序列化，RDD的每个partition会被序列化成一个字节数组。这种方式更加节省内存，从而可以避免持久化的数据占用过多内存导致频繁GC。

                  4.MEMORY_AND_DISK_SER(常用)
                    基本含义同MEMORY_AND_DISK。唯一的区别是，会将RDD中的数据进行序列化，RDD的每个partition会被序列化成一个字节数组。这种方式更加节省内存，从而可以避免持久化的数据占用过多内存导致频繁GC。

                  5.DISK_ONLY(不常用)
                    使用未序列化的对象格式，将数据全部写入磁盘文件中。

                  6.	对于上述任意一种持久化策略，如果加上后缀_2，代表的是将每个持久化的数据，都复制一份副本，并将副本保存到其他节点上。这种基于副本的持久化机制主要用于进行容错。
                    假如某个节点挂掉，节点的内存或磁盘中的持久化数据丢失了，那么后续对RDD计算时还可以使用该数据在其他节点上的副本。如果没有副本的话，就只能将这些数据从源头处重新计算一遍了。
                    一般是不使用副本的
   	cache和persist的注意事项：
    1.	cache和persist都是懒执行，必须有一个action类算子触发执行。
    2.	cache和persist算子的返回值可以赋值给一个变量，在其他job中直接使用这个变量就是使用持久化的数据了。持久化的单位是partition。
    3.	cache和persist算子后不能立即紧跟action算子。
    4.	cache和persist算子持久化的数据当application执行完成之后会被清除。

    错误：rdd.cache().count() 返回的不是持久化的RDD，而是一个数值了。

    checkpoint
    checkpoint将RDD持久化到磁盘，还可以切断RDD之间的依赖关系。checkpoint目录数据当application执行完之后不会被清除。
   	checkpoint 的执行原理：
    1.	当RDD的job执行完毕后，会从finalRDD从后往前回溯。
    2.	当回溯到某一个RDD调用了checkpoint方法，会对当前的RDD做一个标记。
    3.	Spark框架会自动启动一个新的job，重新计算这个RDD的数据，将数据持久化到HDFS上。
   优化：对RDD执行checkpoint之前，最好对这个RDD先执行cache，这样新启动的job只需要将内存中的数据拷贝到HDFS上就可以，省去了重新计算这一步。
   */
    sparkContext.setCheckpointDir("./data/checkpoint")
    text.persist(StorageLevel.MEMORY_ONLY_SER)
    text.checkpoint()
    text.count()
    sparkContext.stop()
  }*/


}
