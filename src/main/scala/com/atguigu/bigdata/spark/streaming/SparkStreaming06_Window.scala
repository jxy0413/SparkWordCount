package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by jxy on 2020/10/8 0008 21:09
  */
object SparkStreaming06_Window {
  def main(args: Array[String]): Unit = {
     //scala中的
     val ints = List(1,2,3,4,5,6)
     //滑动窗口
     val intses = ints.sliding(3,3)
     for(list <- intses){
        println(list.mkString(","))
     }
     //spark中的窗口
     val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
    val streamingContext = new StreamingContext(sparkConf,Seconds(3))
    val kafkaRdd = KafkaUtils.createStream(
      streamingContext,
      "Master:2181",
      "root",
      Map("atguigu" -> 1)
    )
    //窗口大小应该为采集周期的整数倍
    val windowRdd = kafkaRdd.window(Seconds(9),Seconds(3))
    val wordDstream =windowRdd.flatMap(t=>t._2.split(" "))
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
