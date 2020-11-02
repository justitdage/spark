package com.jzj.datasourceV2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @ClassName HbaseSourceAndSink
 * @Date 2019/01/02 20:40 下午
 * @Author JiangZJ
 */
object HbaseSourceAndSink {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("HbaseSourceAndSink")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()


//    val context: SparkContext = session.sparkContext

    val data: DataFrame = session.read.format("com.jzj.datasourceV2.source.HbaseSource")
      .option("hbase.table.name", "spark_hbase_sql")
      .option("hbase.schema", "`name` STRING,`score` STRING")
      .option("hbase.cf.cc", "cf:name,cf:score")
      .load()


    data.explain()
    data.printSchema()
    data.show()


    //写入 实现WriteSupport接口
//    data.write.format("")


  }

}
