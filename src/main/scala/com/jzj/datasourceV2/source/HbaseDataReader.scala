package com.jzj.datasourceV2.source


import java.util

import com.jzj.tools.HbaseTools
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReader

/**
 * @ClassName MysqlDataReader
 * @Date 2019/01/02 20:37 下午
 * @Author JiangZJ
 */
class HbaseDataReader(tableName:String,cfcc:String) extends DataReader[Row]{


  var hbaseConnect:Connection = null;


  //获取迭代器
  def getIterator: Iterator[Result] = {
    hbaseConnect = HbaseTools.getHbaseConn
    val table: Table = hbaseConnect.getTable(TableName.valueOf(tableName))

    val scan: Scan = new Scan()

    val cfccs: Array[String] = cfcc.split(",")
    for (eachcfcc <- cfccs){
      val cfAndcc = eachcfcc.split(":")
      scan.addColumn(cfAndcc(0).trim.getBytes,cfAndcc(1).trim.getBytes)
    }
    val scanner: ResultScanner = table.getScanner(scan)
    val value: util.Iterator[Result] = scanner.iterator()

    import scala.collection.JavaConverters._

    value.asScala
  }

  val data:Iterator[Result] = getIterator

  override def next(): Boolean = {
    data.hasNext
  }

  override def get(): Row = {
    val result: Result = data.next()
    val res: Array[String] = cfcc.split(",").map(each => {
      val strings: Array[String] = each.trim.split(",")
      Bytes.toString(result.getValue(strings(0).trim.getBytes(), strings(1).trim.getBytes()))
    })
    Row.fromSeq(res)
  }

  override def close(): Unit = {
    hbaseConnect.close()
  }
}
