package com.jzj.count

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//日点击次数
object PV {

  def main(args: Array[String]): Unit = {
    // 1. 构建sparkConf对象
    val conf:SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")

    //2sparkContext
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn");

    //3读取
    val data:RDD[String] = sc.textFile("E:\\data\\access.log")

    //4.统计pv
    val pv = data.count()
    println(s"pv:$pv")

    sc.stop();

  }

}
