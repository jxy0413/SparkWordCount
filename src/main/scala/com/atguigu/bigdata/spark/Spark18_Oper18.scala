package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/4 16:46
  */
object Spark18_Oper18 {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val mkRdd = sc.makeRDD(Array(("a",88),("b",22),("a",33),("b",3)),2)
    val combineRdd = mkRdd.combineByKey((_, 1), (acc:(Int, Int), v) => (acc._1 + v, acc._2 + 1), (acc1:(Int, Int), acc2: (Int, Int))=> (acc1._1 + acc2._1, acc1._2 + acc2._2))
    combineRdd.collect().foreach(println)
    val combine = combineRdd.map(t=>(t._1,t._2._1.toDouble/t._2._2.toDouble))
    combine.collect().foreach(println)
  }
}
