package com.kaikeba.ip

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


//spark 来实现ip
object ipLocation {

  //把ip地址转换成Long类型数字 192.168.200.100
  def ip2Long(ip: String): Long = {
    val ips: Array[String] = ip.split("\\.")
    var ipNum:Long = 0L;

    for (i <- ips){
      ipNum = i.toLong | ipNum <<8L
    }
    ipNum
  }

  //利用二分查询查数字在数组的下标
  def binarySearch(ipNHum: Long, city_ip_array: Array[(String, String, String, String)]): Int = {
    //定义数组开始下表
    var start = 0;

    var end = city_ip_array.length -1;

    while (start <=end){
      //获取中间下表
      val middle = (start+end)/2


      if(ipNHum >= city_ip_array(middle)._1.toLong && ipNHum <= city_ip_array(middle)._2.toLong){
          return middle
      }

      if(ipNHum < city_ip_array(middle)._1.toLong){
        end = middle -1
      }
      if(ipNHum > city_ip_array(middle)._2.toLong){
        start = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    //1
    val conf: SparkConf = new SparkConf().setAppName("ipLocation").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    //2
    sc.setLogLevel("warn")

    //3加载城市ip信息数据，获取ip的开始数字、进度纬度
    val city_ip_rdd: RDD[(String, String, String, String)] = sc.textFile("./data/ip.txt").map(x => x.split("\\|")).
      map(x => (x(2), x(3), x(x.length - 2), x(x.length - 1)))

    //使用spark的广播变量 把公共的数据广播到参与计算的worker节点的excutor中
    val cityIpBroudcast: Broadcast[Array[(String, String, String, String)]] = sc.broadcast(city_ip_rdd.collect())


    //4读取运营商日志数据
    val userIpRdd: RDD[String] = sc.textFile("./data/20090121000132.394251.http.format").map(x => x.split("\\|")(1))


    //5 遍历userIpRdd获取每个ip地址 然后转换成数值，去广播变量中进行匹配
    val resultRdd: RDD[((String, String), Int)] = userIpRdd.mapPartitions(iter => {
      //5.1获取广播变量
      val city_ip_array: Array[(String, String, String, String)] = cityIpBroudcast.value

      //5.2获取每个ip地址
      iter.map(ip => {
        //把ip转换成数值
        val ipNHum: Long = ip2Long(ip)

        //需要与广播变量的数值匹配 获取Long类型的数字在数组的下表
        val index: Int = binarySearch(ipNHum, city_ip_array)

        //获取对应下表的信息
        val result: (String, String, String, String) = city_ip_array(index)

        //封装结果信息
        ((result._3, result._4), 1)
      })
    })

    //6相同的经纬度累加
    val finalResult: RDD[((String, String), Int)] = resultRdd.reduceByKey(_+_)

    finalResult.foreach(println)


    sc.stop()
  }
}
