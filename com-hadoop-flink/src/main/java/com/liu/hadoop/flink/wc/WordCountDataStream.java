package com.liu.hadoop.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author LiuJunFeng
 * @date 2021/4/23 下午7:31
 * @description:  wordCount  DataStream : 处理无界流数据 （流处理）
 *
 *
 * Table : 是以表为中心的声明式编程，其中表可能会动态变化（在表达流数据时）
 *
 * DataStream : 有界或无界流数据
 * 			DataSet : 处理有界流数据 （批处理）
 * 			DataStream : 处理无界流数据 （流处理）
 *
 * Process Function : 最底层级的抽象仅仅提供了有状态流
 *
 *
 * getExecutionEnvironment 会根据查询运行的方式决定返回什么样的运行环境，是最常用的一种创建执行环境的方式。
 *
 */
public class WordCountDataStream {

	public static void main(String[] args) {

		// 创建 DataStream 执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 从socket文本流读取数据
		DataStream<String> ds = env.socketTextStream("localhost",8888);

		// 基于数据流进行转换计算，按空格分词展开，转换成(word, 1)二元组进行统计
		DataStream<Tuple2<String, Integer>> wordOne = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
				String[] s1 = s.split(" ");
				for (String str : s1) {
					collector.collect(new Tuple2<>(str, 1));
				}
			}
		});

		// 按照key 划分
		KeyedStream<Tuple2<String, Integer>, Tuple> words = wordOne.keyBy(0);

		// 按照value 数据求和
		DataStream<Tuple2<String, Integer>> wordCount = words.sum(1);

		wordCount.print("WordCount");

		try {
			env.execute("test");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}


}
