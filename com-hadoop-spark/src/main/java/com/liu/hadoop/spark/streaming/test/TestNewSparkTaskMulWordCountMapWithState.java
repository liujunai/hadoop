package com.liu.hadoop.spark.streaming.test;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * @author LiuJunFeng
 * @date 2021/4/21 下午8:13
 * @description: mapWithState
 *
 * 2、mapWithState(1.6-2.1版本至今仍为试验性接口，但是目前验证没发现问题)
 * 根据key 维护并更新state到内存中(源码中存储调用persist(StorageLevel.MEMORY_ONLY) -内存中存储)
 * 底层通过分区等策略，实现对部分数据的mapFunc(自定义的Function算子)运算，实现增量数据计算，官方称相比updateStateByKey 有10倍性能提升
 * 由于状态的存储都是在内存中，所以要借助spark的checkpoint特性，实现对spark计算上下文环境的备份，确保维护的state在服务器宕机或者服务升级、
 * 重启后，能够恢复之前的state，继续进行运算。
 */
public class TestNewSparkTaskMulWordCountMapWithState {

	private static final String SOCKET_SERVER_IP = "xxx";
	private static final int SOCKET_SERVER_PORT_BMP = 5555;
	private static final int SOCKET_SERVER_PORT_PREFIX = 9999;
	private static final String CHECK_POINT_DIR = "D:\\spark\\checkpoint\\mapwithstate\\wordcount";
	private static final int CHECK_POINT_DURATION_SECONDS = 10;

	public static void main(String[] args){
		testSpark();
	}

	private static void testSpark(){
		// 1、从checkpoint恢复，没有备份点则新建JavaStreamingContext
		JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(CHECK_POINT_DIR, new Function0<JavaStreamingContext>(){
			@Override
			public JavaStreamingContext call() throws Exception	{
				return getJavaStreamingContext();
			}
		});

		// 2、任务开启
		jssc.start();
		try{
			jssc.awaitTermination();
		}catch (InterruptedException e){
			e.printStackTrace();
		}
		jssc.close();
	}

	// 新建JavaStreamingContext
	private static JavaStreamingContext getJavaStreamingContext(){
		/**
		 * Spark中本地运行模式有3种，如下
		 *（1）local 模式：本地单线程运行；
		 *（2）local[k]模式：本地K个线程运行；
		 *（3）local[*]模式：用本地尽可能多的线程运行。
		 */
		SparkConf conf = new SparkConf().setAppName("SparkMapWithState").setMaster("local[*]");
		// 1、设置任务间隔
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(CHECK_POINT_DURATION_SECONDS));

		// 2、设置checkpoint目录
		jssc.checkpoint(CHECK_POINT_DIR);

		// 3、启动两个socket连接, 模拟两个输入，远程Linux可使用nc -lk [port] 启动server
		JavaReceiverInputDStream<String> word1 = jssc.socketTextStream(SOCKET_SERVER_IP, SOCKET_SERVER_PORT_BMP);
		JavaReceiverInputDStream<String> word2 = jssc.socketTextStream(SOCKET_SERVER_IP, SOCKET_SERVER_PORT_PREFIX);

		// 4、处理接收过来的数据
		FlatMapFunction<String,String> flatMapFunction = new FlatMapFunction<String,String>() {
			@Override
			public Iterator<String> call(String t) throws Exception	{
				return Arrays.asList(t.split(" ")).iterator();
			}
		};

		JavaDStream<String> word1Stream = word1.flatMap(flatMapFunction);
		JavaDStream<String> word2Stream = word2.flatMap(flatMapFunction);

		// 5、合并两个stream数据
		JavaDStream<String> unionData = word1Stream.union(word2Stream);

		// 6、转换为JavaPairDStream
		JavaPairDStream<String, Integer> pairs = unionData.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String word) throws Exception	{
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		// 7、 统计全局的word count，而不是单一的某一批次
		Function3<String, Optional<Integer>, State<Integer>, String> mappingFunction = new Function3<String, Optional<Integer>, State<Integer>, String>(){
			// curState为当前key对应的state
			@Override
			public String call(String key, Optional<Integer> value,   State<Integer> curState) throws Exception	{
				if (value.isPresent()){
					Integer curValue = value.get();
					System.out.println("value ------------->" + curValue);
					if(curState.exists()){
						curState.update(curState.get() + curValue);
					}else{
						curState.update(curValue);
					}
				}
				System.out.println("key ------------->" + key);
				System.out.println("curState ------------->" + curState);
				return key;
			}
		};
		JavaMapWithStateDStream<String, Integer, Integer, String> wordcounts = pairs.mapWithState(StateSpec.function(mappingFunction));

		// 8、输出
		wordcounts.print();
		return jssc;
	}

}
