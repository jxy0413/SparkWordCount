package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author jxy
  * @Date: 2020/10/1 0:17
  */
object Spark02_Oper1 {
  def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
      val sc = new SparkContext(sparkConf)
      //map算子
      val listRdd = sc.makeRDD(1.to(10))
      val mapPartionsRdd = listRdd.mapPartitions(data=>{
         data.map(data=>data*2)
      })
      //mapPartitions可以对一个RDD中的所有分区进行遍历
      //mapPartitions的作用  效率优于map算子 减少了发送到执行器的交互次数 为了节省IO操作 但缺点是有可能内存放不下 内存溢出（OOM)
      mapPartionsRdd.collect().foreach(println)
      val mapRdd = listRdd.map(x =>x*2)
      mapRdd.collect().foreach(println)
  }
}
