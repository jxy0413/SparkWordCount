package com.atguigu.bigdata.spark.core

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/1  22:52
  * //13自定义分区器
  */
object Spark14_Oper13 {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //val listRdd = sc.makeRDD(Array(1,2,45,5,2,4))
    val listRdd = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val partRdd = listRdd.partitionBy(new MyPartioner(3))
    partRdd.glom.foreach(t=>{
      println(t.mkString(","))
    })
  }
}
class MyPartioner(partitions:Int) extends Partitioner{
  override def numPartitions: Int ={
      partitions
  }

  override def getPartition(key: Any): Int = {
      1
  }
}