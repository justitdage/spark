package com.jzj.db

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//通过foreach 算子吧rdd数据存到mysql中
object Data2MysqlForeach {
  def main(args: Array[String]): Unit = {
    // 1. 构建sparkConf对象
    val conf:SparkConf = new SparkConf().setAppName("Data2MysqlForeach").setMaster("local[2]")

    //2sparkContext
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn");

    //读取
    val personRdd: RDD[String] = sc.textFile("E:\\data\\person.txt")

    //切分数据
    val mapRDD: RDD[(String, String, Int)] = personRdd.map(x => x.split(",")).map(x => (x(0), x(1), x(2).toInt))

    //在Driver端构建
    var connection: Connection = DriverManager.getConnection("jdbc:mysql://node03:3306/spark", "root", "123456")


    //使用foreach算子把rdd结果写到mysql中
    mapRDD.foreach(t=>{
      try {
        //获取数据库连接 （在executor端构建）
//        var connection = DriverManager.getConnection("jdbc:mysql://node03:3306/spark", "root", "123456")

        //定义sql语句
        val sql = "insert into person(id,name,age) values(?,?,?)"

        //获取preParedStatement
        val ps: PreparedStatement = connection.prepareStatement(sql)

        //赋值
        ps.setString(1, t._1);
        ps.setString(2, t._2);
        ps.setInt(3, t._3)

        ps.execute()
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
