package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by jxy on 2020/10/7 10:21
  */
object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("sparkSql")
    //val session = new SparkSession(sparkConf)
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    //创建SparkSQL的换究竟
    val frame = session.read.json("F:\\workspace\\SparkWordCount\\src\\main\\resources\\User.json")
    //将DataFrame转换为一张表
    frame.createGlobalTempView("user")
    //展示数据
    session.sql("select * from global_temp.user").show
    //frame.show()
    //采用Sql的语法访问数
    //释放资源
    session.stop()
  }
}
