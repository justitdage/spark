package com.jzj.datasourceV2.source

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

/**
 * @ClassName MysqlSource
 * @Date 2019/01/02 20:22 下午
 * @Author JiangZJ
 */
class HbaseSource extends DataSourceV2 with ReadSupport{
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val tableName: String = options.get("hbase.schema").get()
    val schema: String = options.get("hbase.table.name").get()
    val cfcc: String = options.get("hbase.cf.cc").get()
    new HbaseDataSourceReader(tableName,schema,cfcc)

  }
}
