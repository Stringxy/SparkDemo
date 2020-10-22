package xy.scala.simple

import org.apache.spark.SparkContext

/**
  * @author xy
  *         2020/10/22
  */
object RDDTest {

  def main(args: Array[String]): Unit = {
//    val sc =new SparkContext("local","xyRdd")
//    mapTest(sc)
//    arrTest（sc）
//    val addOne =add(1)_
//    println(addOne(2))

  }


  def arrTest(sc:SparkContext): Unit ={
    val rdd1=sc.parallelize(List(1,2,3,4))
    val rdd2=sc.parallelize(List(3,4,5,6))
    println("并集")
    rdd1.union(rdd2).distinct().foreach(print)
    println("交集")
    rdd1.intersection(rdd2).foreach(print)
    println("去重")
    rdd1.subtract(rdd2).union(rdd2.subtract(rdd1)).foreach(print)
    println("累加:"+rdd1.reduce(_+_))
  }
  def mapTest(sc:SparkContext): Unit ={
    val rdd = sc.parallelize(Array(("HuaWei",6),("Xiaomi",5),("OPPO",4),
      ("Iphone",1),("HuaWei",4),("Xiaomi",3),("OPPO",6)))
    rdd.groupByKey().foreach(obj=>
      println((obj._1,(obj._2.sum/obj._2.size)))
    )
  }
  def add(a:Int)(b:Int): Int ={//函数柯里化
    a+b
  }


}
