package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by jxy on 2020/10/7 10:51
  */
object SparkSQL02_SQL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("sparkSql")
    //val session = new SparkSession(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //创建RDD
    val rdd = spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",30)))
    //转换为df
    //todo 需要隐式转换
    //进行转换之前，需要引入隐式转换规则
    //这里的spark不是包的含义，是SparkSession对象的名字
    import spark.implicits._
    val df = rdd.toDF("id","name","age")
    //转换为ds
    val ds = df.as[User]
    //转换为df
    val df1 = ds.toDF()
    //转换为rdd
    val rdd1 = df1.rdd
    rdd1.foreach(x=>{
       //获取数据时候，可以通过索引访问数据
       println(x.getInt(0))
    })
  }
}
case class User(id:Int,name:String,age:Int)
