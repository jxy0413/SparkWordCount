package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/1  14:19
  */
object Spark10_Oper9 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc = new SparkContext(sparkConf)
    val listRdd = sc.makeRDD(List(1,2,1,31,4,12,2,4))
    //val distinctRdd = listRdd.distinct()
    val distinctRdd = listRdd.distinct(2)
    distinctRdd.saveAsTextFile("F:\\workspace\\SparkWordCount\\out3")
  }
}
