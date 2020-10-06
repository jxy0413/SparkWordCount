package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/1  13:28
  */
object Spark08_Oper7 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc = new SparkContext(sparkConf)
    //分组
    val listRdd = sc.makeRDD(List(1,2,3,4))
    val filtRdd = listRdd.filter(x=>x%2==0)
    filtRdd.collect().foreach(println)
  }
}
