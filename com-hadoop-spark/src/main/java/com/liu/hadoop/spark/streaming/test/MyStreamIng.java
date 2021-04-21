package com.liu.hadoop.spark.streaming.test;

import org.apache.spark.api.java.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * @author LiuJunFeng
 * @date 2021/4/21 下午6:22
 * @description:
 */
public class MyStreamIng {

	private static String appName = "streaming";
	private static String master = "local[2]";
	private static String host = "mini1";
	private static int port = 8888;

	public static void main(String[] args) throws InterruptedException {
		String checkpointDir = "C:\\workspace\\sparkTest\\src\\test\\java\\data\\";//checkPointPath
		JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDir, createContext(appName, checkpointDir));
		//开始作业
		ssc.start();
		ssc.awaitTermination();
	}

	public static Function0<JavaStreamingContext> createContext(final String appName, final String checkpointDir) {
		return new Function0<JavaStreamingContext>() {
			@Override
			public JavaStreamingContext call() throws Exception {
				SparkConf sparkConf = new SparkConf().setMaster(master).setAppName(appName);//初始化sparkConf
				JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));//获得JavaStreamingContext
				ssc.checkpoint(checkpointDir);

				JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, port);//从socket源获取数据
				JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {//拆分行成单词
					@Override
					public Iterator<String> call(String s) throws Exception {
						return Arrays.asList(s.split(" ")).iterator();
					}
				});
				JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {//转化成<K,V>
					public Tuple2<String, Integer> call(String s) throws Exception {
						return new Tuple2<String, Integer>(s, 1);
					}
				}).cache();

				//统计历史数据
				JavaPairDStream<String, Integer> dStream = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>(){
					//<当前批次数据,历史数据>
					@Override
					public Optional<Integer> call(List<Integer> integers, Optional<Integer> optional) throws Exception {
						Integer updatedValue = 0;//默认初始值
						if (optional.isPresent()) {//获取历史数据
							updatedValue = optional.get();
						}
						for (Integer value : integers) {//累加
							updatedValue += value;
						}
						return Optional.of(updatedValue);//返回结果
					}
				});
				dStream.print();//输出
				return ssc;
			}
		};
	}

}
