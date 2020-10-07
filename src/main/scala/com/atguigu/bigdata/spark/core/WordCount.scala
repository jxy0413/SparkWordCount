package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
       //使用开发工具完成Spark  WordCount的开发
       //local模式
       //创建SparkConf对象
       //设定Spark计算框架的运行(部署)环境
       val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
       //创建Spark上下文对象
       val sc = new SparkContext(config)
       //读取文件  一行一行的数据
       //如果需要从本地查找：file:///opt/module/spark/
       val lines = sc.textFile("F:\\workspace\\SparkWordCount\\in\\word.txt")
       println("---"+lines)
       //将一行行数据拆分成一个个单词
       val words = lines.flatMap(_.split(" "))
       println(words.collect())
       //为了统计方便，将单词结构进行了转换
       val wordToOne = words.map((_,1))
       val wordToSum = wordToOne.reduceByKey(_+_)
       //打印到控制台
       val result = wordToSum.collect()
       result.foreach(println)
  }
}
