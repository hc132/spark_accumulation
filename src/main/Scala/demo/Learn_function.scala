package demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created on 2019/11/13 17:24
  *
  * @author : HC
  */
object Learn_function {

  def  fun (a:Int ,b:Int)(c:Int ,d:Int)={
    a+b+c+d
  }


  def main(args: Array[String]): Unit = {
    println(fun(1,2)(3,4))

    val abv = 1
    val list: Object = List[Int](1,2)
    println(list)

//    val tuple = new Tuple3[Int](1,2,3)
//
//    new Tuple3[Int](1,2,3)

  }
}
