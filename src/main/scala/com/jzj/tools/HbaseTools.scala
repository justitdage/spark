package com.jzj.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
 * @ClassName HbaseTools
 * @Date 2020/11/2 20:13 下午
 * @Author JiangZJ
 */
object HbaseTools {

  def getHbaseConn: Connection = {
    try{
      val config:Configuration = HBaseConfiguration.create()
      config.set("hbase.zookeeper.quorum" , "192.168.0.1")
      config.set("hbase.zookeeper.property.clientPort" , "2181")
      val connection = ConnectionFactory.createConnection(config)
      connection
    }catch{
      case exception: Exception =>
        error(exception.getMessage)
        error("HBase获取连接失败")
        null
    }
  }

}
