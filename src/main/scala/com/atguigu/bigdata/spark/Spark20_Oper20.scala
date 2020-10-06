package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/4  18:42
  * 统计每个省份广告点击次数的top3
  * 1516609143867 6 7 64 16
  * 时间戳 省份 城市 用户 广告
  */
object Spark20_Oper20 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Rdd").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines =  sc.textFile("file://" + ClassLoader.getSystemResource("1.txt"))
    val proviceRdd = lines.map(line => {
      val splits = line.split(" ")
      ((splits(1), splits(4)), 1)
    })
    //将每个省份每个广告被点击的总次数
    val provinceADSum = proviceRdd.reduceByKey(_+_)
    //将省份作为key 广告加点击数为value
    val provinceAdSum = provinceADSum.map(x=>(x._1._1,(x._1._2,x._2)))
    //按照省份进行分组
    val proviceGroup = provinceAdSum.groupByKey()
    //对同一个省份的广告进行排序
    val sortKey = proviceGroup.mapValues {
      x => x.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    }
    //排序
    val value = sortKey.sortByKey(true)
    value.collect().foreach(println)
  }
}
