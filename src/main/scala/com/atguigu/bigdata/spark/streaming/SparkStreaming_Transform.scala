package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jxy on 2020/10/8  22:20
  */
object SparkStreaming_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
    val streamingContext = new StreamingContext(sparkConf,Seconds(3))
    //转换
  }
}
