package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/1 14:51
  */
object Spark11_Oper10 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc = new SparkContext(sparkConf)
    val listRdd = sc.makeRDD(1.to(16),4)
    println("缩减分区前size "+listRdd.partitions.size)
    val backRdd = listRdd.coalesce(3)
    //缩减分区：可以简单的理解为合并分区
    println("缩减分区后size "+backRdd.partitions.size)

  }
}
