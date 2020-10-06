package com.atguigu.bigdata.spark

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/6  11:08
  */
object Spark19_Hbase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc = new SparkContext(sparkConf)
    //创建Hbase对象
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"student1")
    val hbaseRdd = sc.newAPIHadoopRDD(hbaseConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    hbaseRdd.foreach{
      case (rowkey,result) =>{
        val cells = result.rawCells()
        for(cell <- cells){
          println(Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    }
    val dataRDD = sc.makeRDD(List(("1002","zhangsan"),("1003","lisi"),("1004","wangwu")))
    val putRdd = dataRDD.map {
      case (rowkey, name) => {
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
        (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
      }
    }
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"student1")
    putRdd.saveAsHadoopDataset(jobConf)


  }
}
