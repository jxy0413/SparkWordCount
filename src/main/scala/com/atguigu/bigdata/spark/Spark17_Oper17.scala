package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/2  9:50
  */
object Spark17_Oper17 {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val listRdd = sc.makeRDD(List(("a",3),("a",2),("c",4),("c",2),("b",4),("b",5)),2)
    listRdd.glom().collect.foreach(t=>{
      println(t.mkString(","))
    })
    //找相同key的相加 保留每个分区的最大值
    val aggreateRdd = listRdd.aggregateByKey(0)(math.max(_,_),_+_)
    val flodBykey = listRdd.foldByKey(0)(_+_)
    flodBykey.collect().foreach(println)

    //
  }
}
