package com.liu.hadoop.flink.source;

import com.liu.hadoop.flink.beans.Sensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;

/**
 * @author LiuJunFeng
 * @date 2021/1/6 下午5:43
 * @description: 自定义数据来源
 */
public class Flink04_Source_Custom {


	public static void main(String[] args) {

		//创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//自定义数据来源
		DataStream<Sensor> dataStream = env.addSource(new MySource());

		//输出数据
		dataStream.print("自定义输出");

		//执行
		try {
			env.execute("demoTask4");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//实现自定义的SourceFunction
	public static class MySource implements SourceFunction<Sensor>{

		//定义标识位,控制数据产生
		private boolean running = true;

		public void run(SourceContext<Sensor> sourceContext) throws Exception {
			//定义一个随机数发生器
			Random random = new Random();

			//设置5个传感器
			HashMap<String, Double> sensor = new HashMap<String, Double>();
			for (int x=0;x<5;x++){
				sensor.put("sensor_"+(x+1), 60+ random.nextGaussian() *10);
			}

			while (running){
				for (String sensorId: sensor.keySet()){
					Double newTime = sensor.get(sensorId) + random.nextGaussian();
					sensor.put(sensorId,newTime);
					sourceContext.collect(new Sensor(sensorId,System.currentTimeMillis(),newTime));
				}
				//5秒输出一次
				Thread.sleep(5000L);
			}
		}

		public void cancel() {
			running = false;
		}
	}

}
