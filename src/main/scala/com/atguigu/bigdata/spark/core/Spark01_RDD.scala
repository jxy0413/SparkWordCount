package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //1.从内存中创建makeRDD
    val listRdd = sc.makeRDD(List(1,2,3,4))
    //2.从内存中创建parallelize
    val arrayRdd = sc.parallelize(List(1,2,3,4))
    //3.从外部存储中创建
    //默认情况下，可以读取项目路径，也可以读取其他路径 hdfs://
    //默认情况下，读取的数据都是字符串类型
    sc.textFile("hdfs://Master:9000/......")
    //将Rdd的数据保存到文件中
//    listRdd.saveAsTextFile("F:\\workspace\\SparkWordCount\\out1")
    //读取文件时候，传递的分区数是最小分区数，但不一定是这个分区，取决于hadoop读取文件的分片规则
    listRdd.collect().foreach(print)
  }
}
