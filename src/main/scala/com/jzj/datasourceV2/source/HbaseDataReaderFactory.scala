package com.jzj.datasourceV2.source

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}

/**
 * @ClassName MysqlDataReaderFactory
 * @Date 2019/01/02 20:34 下午
 * @Author JiangZJ
 */
class HbaseDataReaderFactory(tableName:String,cfcc:String) extends DataReaderFactory[Row]{
  override def createDataReader(): DataReader[Row] = {

    new HbaseDataReader(tableName,cfcc)
  }
}
