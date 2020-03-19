package com.kaikeba.db

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//Data2MysqlForeachPartition 算子吧rdd数据存到mysql中
object Data2MysqlForeachPartition {
  def main(args: Array[String]): Unit = {
    // 1. 构建sparkConf对象
    val conf:SparkConf = new SparkConf().setAppName("Data2MysqlForeachPartition").setMaster("local[2]")

    //2sparkContext
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn");

    //读取
    val personRdd: RDD[String] = sc.textFile("E:\\data\\person.txt")

    //切分数据
    val mapRDD: RDD[(String, String, Int)] = personRdd.map(x => x.split(",")).map(x => (x(0), x(1), x(2).toInt))

    var connection: Connection = null
    //Data2MysqlForeachPartition
    mapRDD.foreachPartition(iter=>{
      try {
        //获取数据库连接
        var connection = DriverManager.getConnection("jdbc:mysql://node03:3306/spark", "root", "123456")

        //定义sql语句
        val sql = "insert into person(id,name,age) values(?,?,?)"

        //获取preParedStatement
        val ps: PreparedStatement = connection.prepareStatement(sql)

        //赋值
        iter.foreach(t=>{
          ps.setString(1, t._1);
          ps.setString(2, t._2);
          ps.setInt(3, t._3)

          //设置批量提交
          ps.addBatch()
        })

        ps.executeBatch()
      }catch {
        case e:Exception => println(e.getMessage)
      }finally {
        if(connection != null){
          connection.close()
        }
      }
    })


  }
}
