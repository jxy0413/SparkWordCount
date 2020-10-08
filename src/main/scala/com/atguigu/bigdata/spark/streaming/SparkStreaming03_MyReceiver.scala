package com.atguigu.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * Created by jxy on 2020/10/7 22:31
  */
object SparkStreaming03_MyReceiver {
  def main(args: Array[String]): Unit = {

  }
}
//生命采集器
//1)继承Receiver
class MyReceiver(host:String,port:Int) extends Receiver(StorageLevel.MEMORY_ONLY){
  var socket :java.net.Socket=null
  def receivce() = {
      socket = new java.net.Socket(host,port)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,"UTF-8"))
      val line:String = null
      while ((line==reader.readLine())!=null){
        //将采集的数据存储到采集器
        if("END".equals(line)){

        }
      }
  }
  override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
             receivce()
        }
      }).start()
  }

  override def onStop(): Unit = ???
}