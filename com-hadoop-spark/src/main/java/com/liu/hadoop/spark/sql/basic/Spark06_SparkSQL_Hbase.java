package com.liu.hadoop.spark.sql.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.hadoop.hbase.client.Result;

/**
 * @author LiuJunFeng
 * @date 2021/4/18 上午1:07
 * @description: spark 连接  hbase  读取 / 写入
 */
public class Spark06_SparkSQL_Hbase {

	public static void main(String[] args){

		// spark 的运行环境
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark on hbase");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		// hbase 的运行环境
		String tablename = "liu";
		Configuration hbaseConf = HBaseConfiguration.create();
		//设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
		hbaseConf.set("hbase.zookeeper.quorum","192.168.122.51,192.168.122.52,192.168.122.53");
		//设置zookeeper连接端口，默认2181
		hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseConf.set(TableInputFormat.INPUT_TABLE, tablename);

		//读取数据并转化成rdd
		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRdd = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

		System.out.println("hbaseRdd.count() = " + hbaseRdd.count());


	}

}
