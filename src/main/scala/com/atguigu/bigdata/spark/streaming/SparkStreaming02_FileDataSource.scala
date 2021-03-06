package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jxy on 2020/10/7 21:50
  */
object SparkStreaming02_FileDataSource {
  def main(args: Array[String]): Unit = {
      //实时分析的对象
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming02_FileDataSource")
      val streamingContext = new StreamingContext(sparkConf,Seconds(5))
      //采集周期：以指定的时间为周期采集实时数据
      val socketLineDStream = streamingContext.textFileStream("F:\\workspace\\SparkWordCount\\in")
      //将采集的数据进行分解（扁平化)
      val wordDstream = socketLineDStream.flatMap(line=>line.split(" "))
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
