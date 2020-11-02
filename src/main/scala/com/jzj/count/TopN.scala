package com.jzj.count

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

//求访问次数最多的url 前5位
object TopN {

  def main(args: Array[String]): Unit = {
    // 1. 构建sparkConf对象
    val conf:SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")

    //2sparkContext
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn");

    //3读取
    val data:RDD[String] = sc.textFile("E:\\data\\access.log")

    //过滤1
    val filterRdd: RDD[String] = data.filter(x => x.split(" ").length > 10)

    //获取url
    val urlsRDD: RDD[String] = filterRdd.map(x => x.split(" ")(10))


    //把每个url记为1
    val urlAndOneRdd: RDD[(String, Int)] = urlsRDD.map(x => ((x, 1)))

    //累加
    val result: RDD[(String, Int)] = urlAndOneRdd.reduceByKey(_ + _)

    //排序
    val sortRDD: RDD[(String, Int)] = result.sortBy(_._2, false)

    //取出最多的前5
    val top5: Array[(String, Int)] = sortRDD.take(5)

    top5.foreach(println)
  }

}
