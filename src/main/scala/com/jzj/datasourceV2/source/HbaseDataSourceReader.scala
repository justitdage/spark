package com.jzj.datasourceV2.source

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.StructType

/**
 * @ClassName MysqlDataSourceReader
 * @Date 2019/01/02 20:36 下午
 * @Author JiangZJ
 */
class HbaseDataSourceReader(tableName:String,schema:String,cfcc:String) extends DataSourceReader{

  val structType: StructType = StructType.fromDDL(schema)

  override def readSchema(): StructType = {
    structType
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {

    import scala.collection.JavaConverters._
    Seq(
      new HbaseDataReaderFactory(tableName,cfcc).asInstanceOf[DataReaderFactory[Row]]
    ).asJava
  }
}
