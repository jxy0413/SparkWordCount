package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
  * Created by jxy on 2020/10/7 11:42
  */
object SparkSQL05_UDAF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("sparkSql")
    //val session = new SparkSession(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val udaf = new MyAgeAvgFunction
    spark.udf.register("avgAge",udaf)
    //使用聚合函数

  }
}
//声明用户自定义聚合函数
//1)继承UserDefinedAggregateFunction
//2)实现方法
class MyAgeAvgFunction extends UserDefinedAggregateFunction{
  //函数输入的数据结构
  override def inputSchema: StructType = {
     new StructType().add("age",LongType)
  }
  //计算时的数据结构
  override def bufferSchema: StructType = {
     new StructType().add("sum",LongType).add("count",LongType)
  }
  //返回的数据类型
  override def dataType: DataType = DoubleType

  //函数的稳定性
  override def deterministic: Boolean = true

  //函数缓冲器的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //更新查询结果更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
     buffer(0) = buffer.getLong(0)+input.getLong(0)
     buffer(1) = buffer.getLong(1)+1
  }

  //将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
     //sum
     buffer1(0) = buffer1.getLong(0)+buffer2.getLong(0)
     //count
     buffer1(1)= buffer1.getLong(1)+buffer2.getLong(1)
  }

  //计算
  override def evaluate(buffer: Row): Any = {
     buffer.getLong(0).toDouble/buffer.getLong(1).toDouble
  }
}
