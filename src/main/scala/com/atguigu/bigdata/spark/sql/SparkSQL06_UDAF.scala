package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

/**
  * Created by jxy on 2020/10/7 0007 17:56
  */
object SparkSQL06_UDAF {
  def main(args: Array[String]): Unit = {
    import org.apache.spark
    //创建配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("sparkSql")
    //val session = new SparkSession(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    //创建聚合函数
    val udaf = new MyAgeAvgClassFunction
    //将聚合函数转换为查询列
    val avgCol = udaf.toColumn.name("avgAge")
    val userDS = spark.read.json("F:\\workspace\\SparkWordCount\\src\\main\\resources\\User.json")
    //应用函数
    userDS.select(avgCol).show()
    //释放资源
    spark.stop
  }
}
case class UserBean(name:String,age:BigInt)
case class AvgBuffer(var sum:BigInt,var count:Int)
//声明用户自定义聚合类型
//1继承Aggregator 设定泛型
//2实现方法
class MyAgeAvgClassFunction extends Aggregator[UserBean,AvgBuffer,Double]{
  override def zero: AvgBuffer = {
     AvgBuffer(0,0)
  }

  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
      b.sum = b.sum+a.age
      b.count = b.count+1
      b
  }

  //缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
      b1.sum = b1.sum+b2.sum
      b1.count = b1.count+b2.count
      b1
  }

  //完成计算
  override def finish(reduction: AvgBuffer): Double = {
     reduction.sum.toDouble/reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}