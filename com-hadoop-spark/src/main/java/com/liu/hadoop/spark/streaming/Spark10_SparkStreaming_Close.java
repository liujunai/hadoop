package com.liu.hadoop.spark.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author LiuJunFeng
 * @date 2021/4/20 下午4:54
 * @description: SparkStreaming  关闭 Close
 * <p>
 * 流式任务需要 7*24 小时执行，但是有时涉及到升级代码需要主动停止程序，但是分
 * 布式程序，没办法做到一个个进程去杀死，所有配置优雅的关闭就显得至关重要了。
 * 使用外部文件系统来控制内部程序关闭
 */
public class Spark10_SparkStreaming_Close {

	public static void main(String[] args) {

		// 创建spark 的运行环境 SparkStreaming
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming");
		// 第一个参数表示环境配置
		// 第二个参数表示批量处理的周期（采集周期） minutes 分钟  seconds 秒  milliseconds 毫秒
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

		JavaReceiverInputDStream<String> localhost = jssc.socketTextStream("localhost", 8888);

		localhost.print();

		jssc.start();
		// 如果想要关闭采集器，那么需要创建新的线程
		// 而且需要在第三方程序中增加关闭状态
		new Thread(new Runnable() {
			@Override
			public void run() {
				// 优雅地关闭
				// 计算节点不在接收新的数据，而是将现有的数据处理完毕，然后关闭
				try {
					FileSystem fs = FileSystem.get(new URI("hdfs://liu1:9000"), new Configuration(), "liu");

					while (true){
						StreamingContextState state = jssc.getState();
						// 判断hdfs上的路径是否存在 用来当做关闭条件
						boolean exists = fs.exists(new Path("hdfs://liu1:9000/stopSpark"));
						if (exists){
							if (state == StreamingContextState.ACTIVE){
								jssc.stop(true,true);
								System.exit(0);
							}
						}
						Thread.sleep(5000);
					}
				} catch (InterruptedException | IOException | URISyntaxException e) {
					e.printStackTrace();
				}
			}
		}).start();


		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
