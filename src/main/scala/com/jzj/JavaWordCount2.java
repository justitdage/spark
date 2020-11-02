package com.jzj;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class JavaWordCount2 {

    public static void main(String[] args) {

        //sc.textFile("数据").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect

        //创建sparkCore
        SparkConf sparkConf = new SparkConf().setSparkHome("wordCount").setMaster("local[2]");

        //创建sparkContext
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //读取文件
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("E:\\file.txt");

        //切分每一行
        JavaRDD<String> wordRdd = stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] words = s.split(" ");
                return Arrays.asList(words).iterator();
            }
        });

        //每个单词记为1
        JavaPairRDD<String, Integer> wordAndOne = wordRdd.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        
        //累加
        JavaPairRDD<String, Integer> resultRdd = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        List<Tuple2<String, Integer>> collect = resultRdd.collect();

        for (Tuple2<String, Integer> t : collect) {
            System.out.println("单词：" + t._1 + "次数：" +t._2);
        }

        sparkContext.close();
    }

}
