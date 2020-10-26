package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by jxy on 2020/10/8 19:30
  */
object SparkStreaming05_UpdateState {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
    val streamingContext = new StreamingContext(sparkConf,Seconds(5))
    //保存检查点的状态，需要设置检查点路径
    streamingContext.sparkContext.setCheckpointDir("F:\\workspace\\SparkWordCount\\src\\main\\resources\\")
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
    //将转换后的数据进行聚合处理
    val stateDStream :DStream[(String,Int)] = wordMapDstream.updateStateByKey {
      case (seq, buffer) => {
        val sum = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }
    //将结果打印出来
    stateDStream.print()
    //不能停止采集程序
    //启动采集器
    streamingContext.start()
    //Driver等待采集器的执行
    streamingContext.awaitTermination()
  }
}
