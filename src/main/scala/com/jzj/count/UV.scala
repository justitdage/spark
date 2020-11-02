package com.jzj.count

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

//日访客人数
object UV {

  def main(args: Array[String]): Unit = {

    // 1. 构建sparkConf对象
    val conf:SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")

    //2sparkContext
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn");

    //3读取
    val data:RDD[String] = sc.textFile("E:\\data\\access.log")

    //4 获取所有的ip地址
    val ipRDD:RDD[String] = data.map(x => x.split(" ")(0))
    val distinctRDD: RDD[String] = ipRDD.distinct()

    val uv: Long = distinctRDD.count()
    println(s"uv:$uv")

    sc.stop();
  }

}
