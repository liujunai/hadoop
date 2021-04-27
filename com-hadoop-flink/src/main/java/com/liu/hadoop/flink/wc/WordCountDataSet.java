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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author LiuJunFeng
 * @date 2021/4/23 下午7:31
 * @description:  wordCount  DataSet : 处理有界流数据 （批处理）
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
 */
public class WordCountDataSet {

	public static void main(String[] args) {

		// 创建 DataSet 执行环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// 从文件中读取数据
		DataSet<String> ds = env.readTextFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-flink/data/demo");

		// 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
		FlatMapOperator<String, Tuple2<String, Integer>> wordOne = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
				String[] s1 = s.split(" ");
				for (String str : s1) {
					collector.collect(new Tuple2<>(str, 1));
				}
			}
		});

		// 按照第一个位置的word分组
		UnsortedGrouping<Tuple2<String, Integer>> words = wordOne.groupBy(0);

		// 将第二个位置上的数据求和
		AggregateOperator<Tuple2<String, Integer>> wordCount = words.sum(1);

		try {
			wordCount.print();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}


}
