package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/1  22:24
  */
object Spark12_Oper11 {
  def main(args: Array[String]): Unit = {
     //重分区
     val config = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val listRdd = sc.makeRDD(1 to 16,4)
    val listRddglom = listRdd.glom().foreach(t => {
        println(t.mkString(","))
    })
    val reList = listRdd.repartition(2)
    println("重新分区后：")
    reList.glom().foreach(t=>{
      println(t.mkString(","))
    })
  }
}
