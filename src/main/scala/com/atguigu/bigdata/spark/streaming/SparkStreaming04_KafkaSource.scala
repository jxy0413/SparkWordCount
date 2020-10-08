package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jxy on 2020/10/8 16:17
  */
object SparkStreaming04_KafkaSource {
  def main(args: Array[String]): Unit = {
    //实时分析的对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
    val streamingContext = new StreamingContext(sparkConf,Seconds(5))
    val kafkaRdd = KafkaUtils.createStream(
      streamingContext,
      "Master:2181",
      "root",
      Map("atguigu" -> 1)
    )
    val wordDstream = kafkaRdd.flatMap(t=>t._2.split(" "))
    val wordMapDstream = wordDstream.map(t => {
      (t, 1)
    })
    //进行聚合处理
    val wordToSum = wordMapDstream.reduceByKey(_+_)
    //将结果打印出来
    wordToSum.print()
    //不能停止采集程序
    //启动采集器
    streamingContext.start()
    //Driver等待采集器的执行
    streamingContext.awaitTermination()
  }
}
