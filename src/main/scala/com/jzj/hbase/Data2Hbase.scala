package com.kaikeba.hbase

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

//通过foreacheParation 写入到hbase
object Data2Hbase {
  def main(args: Array[String]): Unit = {
    // 1. 构建sparkConf对象
    val conf:SparkConf = new SparkConf().setAppName("Data2Hbase").setMaster("local[2]")

    //2sparkContext
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn");

    //读取
    val data: RDD[Array[String]] = sc.textFile("E:\\data\\users.dat").map(x => x.split("::"))

    var connection: Connection = null

    try{
    data.foreachPartition(iter=> {
      //获取数据库连接
      val configuration: Configuration = HBaseConfiguration.create();
      configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181")

      connection = ConnectionFactory.createConnection(configuration)

      //hbase表操作
      // create 'person','f1','f2'
      val table: Table = connection.getTable(TableName.valueOf("person"))


      iter.foreach(line => {


        val puts = new util.ArrayList[Put]()
        //构建put对象
        val put = new Put(line(0).getBytes)
        val put1: Put = put.addColumn("f1".getBytes, "gerder".getBytes, line(1).getBytes)
        val put2: Put = put.addColumn("f2".getBytes, "age".getBytes, line(2).getBytes)
        val put3: Put = put.addColumn("f1".getBytes, "position".getBytes, line(3).getBytes)
        val put4: Put = put.addColumn("f2".getBytes, "code".getBytes, line(4).getBytes)

        puts.add(put1)
        puts.add(put2)
        puts.add(put3)
        puts.add(put4)
        table.put(puts)

      })

    })
    }catch{
      case e:Exception => println(e)
    }finally {
      if (connection!=null){
      connection.close()
      }
    }

  }
}
