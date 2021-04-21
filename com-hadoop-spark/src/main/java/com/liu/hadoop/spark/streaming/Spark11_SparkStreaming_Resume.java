package com.liu.hadoop.spark.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/20 下午4:54
 * @description: SparkStreaming  关闭之后 从检查点 恢复数据
 * <p>
 * 流式任务需要 7*24 小时执行，但是有时涉及到升级代码需要主动停止程序，但是分
 * 布式程序，没办法做到一个个进程去杀死，所有配置优雅的关闭就显得至关重要了。
 * 使用外部文件系统来控制内部程序关闭
 */
public class Spark11_SparkStreaming_Resume {



	public static void main(String[] args) {

		// 从检查点恢复数据
		JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(
				"/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/stateful",
				new Function0<JavaStreamingContext>() {
					@Override
					public JavaStreamingContext call() throws Exception {
						SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming");
						JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));
						jssc.checkpoint("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-spark/stateful");

						// 新获取的值
						JavaReceiverInputDStream<String> messages = jssc.socketTextStream("localhost", 8888);

						JavaDStream<String> words = messages.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

						JavaPairDStream<String, Integer> wordOne = words.mapToPair(x -> new Tuple2<>(x, 1));

						// 和检查点的值合并
						JavaPairDStream<String, Integer> wordCount = wordOne.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
							@Override
							public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
								Integer newv2 = 0;
								if (v2.isPresent()) {
									newv2 = v2.get();
								}
								for (Integer integer : v1) {
									newv2 += integer;
								}
								return Optional.of(newv2);
							}
						});

						wordCount.print();
						return jssc;
					}
				}
		);

		jssc.start();

		// 其他线程运行关闭
		new Thread(new Runnable() {
			@Override
			public void run() {
				// SparkStreaming 运行10秒后关闭 测试从检查点恢复
				try {
					Thread.sleep(20000);
					if (jssc.getState() == StreamingContextState.ACTIVE) {
						jssc.stop(true, true);
						System.exit(0);
					}
				} catch (InterruptedException e) {
					System.out.println("e = " + e.toString()+ "睡眠中断");
				}
			}
		}).start();


		try {
			jssc.awaitTermination();
		} catch (
				InterruptedException e) {
			e.printStackTrace();
		}

	}


}
