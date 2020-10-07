package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/5  22:59
  */
object Spark23_Mysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Rdd").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/clouddb02"
    val userName = "root"
    val password = "111111"
    //查询数据
    val sql = "select * from user where id >=? and id <= ?"
    //创建JDBCRDdd 方法数据库
//    val jdbcRdd = new JdbcRDD(
//      sc,
//      () => {
//        //获取数据库连接对象
//        Class.forName(driver)
//        java.sql.DriverManager.getConnection(url, username, password)
//      },
//      sql,
//      1,
//      3,
//      2,
//      (rs)=>{
//        println(rs.getString(2)+", "+rs.getInt(3))
//      }
//    )
//    jdbcRdd.collect()
    //保存数据
    val dataRdd = sc.makeRDD(List(("zhangsan",20),("lisi",30)))
//    dataRdd.foreach{
//      case(username,age) =>{
//        val sql = "insert into user(name,age) values(?,?)"
//        val statement = connection.prepareStatement(sql)
//        statement.setString(1,username)
//        statement.setInt(2,age)
//        statement.executeUpdate()
//        statement.close()
//      }
//    }
    dataRdd.foreachPartition(datas=>{
      Class.forName(driver)
      val connection = java.sql.DriverManager.getConnection(url, userName, password)
          datas.foreach {
            case (username, age) => {
              val sql = "insert into user(name,age) values(?,?)"
              val statement = connection.prepareStatement(sql)
              statement.setString(1, username)
              statement.setInt(2, age)
              statement.executeUpdate()
              statement.close()
            }
          }
      connection.close()
    })
  }
}
