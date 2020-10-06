package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/1  13:23
  */
object Spark07_Oper6 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc = new SparkContext(sparkConf)
     //分组
    val listRdd = sc.makeRDD(List(1,2,3,4))
    //分组后的数据形成了对偶元组(K-V)
    val value = listRdd.groupBy(i => i%2)
    value.collect().foreach(println)
  }
}
