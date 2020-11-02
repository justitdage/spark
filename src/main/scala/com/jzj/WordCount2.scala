package com.jzj

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("wordCount").setMaster("local[2]")

    val sparkContext = new SparkContext(conf);

    val value: RDD[String] = sparkContext.textFile("e:\\file.txt")

    val words: RDD[String] = value.flatMap(x => x.split(" "))

    val wordsAndOne: RDD[(String,Int)] = words.map(x => (x, 1))

    val result: RDD[(String,Int)] = wordsAndOne.reduceByKey((x, y) => x + y)

    val sortedRDD: RDD[(String,Int)] = result.sortBy(x => x._2, false)

    val resultArray: Array[(String, Int)] = sortedRDD.collect()

    resultArray.foreach(println)

    sparkContext.stop()
  }
}
