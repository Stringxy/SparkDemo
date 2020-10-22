package xy.scala.simple

import org.apache.spark.SparkContext

/**
  * 根据各班成绩查询全校前n位的情况
  *
  * @author xy
  *         2020/10/22
  */
object ClassRank {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "rank")
    sc.setLogLevel("ERROR")
    //设置日志打印等级
    val lines = sc.textFile("./files/Class1.txt,./files/Class2.txt,./files/Class3.txt")//可从项目根目录获取相对地址
    var num = 0
    val result = lines.filter(line => (line.trim.length > 0) && (line.split(",").length == 4))//去空格并检查每行格式
      .map(line => {
        val fields = line.split(",")//按,切割为长度为4数组
        val userid = fields(1)
        val score = fields(2).toInt
        val classes = fields(3)
        (score, (classes, userid))//拼接数据返回单个对象
      })
    println("rank\tclass\t\tuserid\tscore\n");
    val result1 = result.sortByKey(false).take(10).foreach(x => {//sortByKey:true升序false降序
      num = num + 1
      println(num + "\t\t" + x._2._1 + "\t\t" + x._2._2 + "\t" + x._1)
    })
  }
}
