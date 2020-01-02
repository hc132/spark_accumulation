package ScalaLearn

class BaseDemo {

}
object BaseDemo{
  def main(args: Array[String]): Unit = {


    for( i   <- 1 to 10 ){
      print(s" 第 $i 次循环  $i  \n")
    }
  }
}