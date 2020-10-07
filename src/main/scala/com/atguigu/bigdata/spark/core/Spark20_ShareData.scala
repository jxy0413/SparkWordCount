package com.atguigu.bigdata.spark.core

import java.util
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/6  11:57
  */
object Spark20_ShareData {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc = new SparkContext(sparkConf)
//    val listRdd = sc.makeRDD(List(1,2,3,4),2)
////    val i = listRdd.reduce(_+_)
////    println(i)
//    //使用累加器来共享变量，来累加数据
//    //创建累加器对象
//    val accumulator = sc.longAccumulator
//    listRdd.foreach{
//      case i =>{
//        //执行累加器的功能
//        accumulator.add(i)
//      }
//    }
//    //获取累加器的值
//    println("sun = "+accumulator.value)
    //创建累加器
    val dataRdd = sc.makeRDD(List("hadoop","hbase","zookeeper","flume","kafka","spark"))
    val wordAccumulator = new WordAccumulator
    //todo 注册累加器
    sc.register(wordAccumulator)
    dataRdd.foreach{
      case word =>{
        //执行累加器的累加功能
        wordAccumulator.add(word)
      }
    }
    println("SUM = "+wordAccumulator.value)

    sc.stop()
  }
}
//声明累加器
//继承AccumulatorV2
//实现抽样方法
//创建累加器
class WordAccumulator extends AccumulatorV2[String,util.ArrayList[String]]{
  val list = new util.ArrayList[String]()
  //当前的累加器是否为初始化状态
  override def isZero: Boolean = list.isEmpty

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  //重置累加器对象
  override def reset(): Unit = list.clear()

  //向累加器中增加数据
  override def add(v: String): Unit = {
      if(v.contains("h")){
          list.add(v)
      }
  }

  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  override def value: util.ArrayList[String] = list
}