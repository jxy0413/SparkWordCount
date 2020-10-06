package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/4  17:52
  */
object Spark19_Oper19 {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val listRdd = sc.makeRDD(List(("a",1),("b",1),("c",2),("a",2)))
    val sortRdd = listRdd.sortByKey()
    sortRdd.collect().foreach(println)
  }
}
