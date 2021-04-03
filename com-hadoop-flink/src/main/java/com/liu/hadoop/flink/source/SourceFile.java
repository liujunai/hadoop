package com.liu.hadoop.flink.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author LiuJunFeng
 * @date 2021/1/6 下午5:03
 * @description: 从文件中读取数据
 */
public class SourceFile {

	public static void main(String[] args) {

		//创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		//从文件中读取数据
		DataStream<String> dataStream = env.readTextFile("./src/main/resources/sensor.txt");

		//输出数据
		dataStream.print("文件输出");

		//执行
		try {
			env.execute("demoTask2");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


}
