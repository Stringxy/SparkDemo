package xy.scala.simple

import org.apache.spark.SparkContext

/**
  * 单词技术demo
  * @author xy
  *         2020/10/20
  */
object WordCount {

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("xySpark").setMaster("local")
    val sc = new SparkContext("local","wc") //SparkConf的简写 master=local appName=wc
    val rdd = sc.textFile(args(0))//在Run Configurations-Program arguments自主配置文件地址参数
    val wordCount = rdd.flatMap(_.split("\t")).map((_,1)).reduceByKey(_+_)
    for(arg<-wordCount.collect())
      println(arg+" ")
    sc.stop()
  }
}
