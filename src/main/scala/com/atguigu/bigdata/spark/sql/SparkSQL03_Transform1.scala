package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by jxy on 2020/10/7 11:13
  */
object SparkSQL03_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("sparkSql")
    //val session = new SparkSession(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //创建RDD
    val rdd = spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",30)))
    //转换为ds
    //todo 需要隐式转换
    //进行转换之前，需要引入隐式转换规则
    //这里的spark不是包的含义，是SparkSession对象的名字
    import spark.implicits._
    var userRdd = rdd.map{
      case(id,name,age) =>{
        User(id,name,age)
      }
    }
    val userDs = userRdd.toDS()
    val rdd1 = userDs.rdd
    println(userDs.show())
  }
}
