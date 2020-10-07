package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/1  13:06
  */
object Spark06_Oper5 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc = new SparkContext(sparkConf)
    val listRdd = sc.makeRDD(1 to 16,4)
    //将一个分区的数据放到一个数组中
    val glomRdd = listRdd.glom()
    val max = glomRdd.collect().foreach(array => {
         val max = array.toList.max
         println(max)
    })

  }
}
