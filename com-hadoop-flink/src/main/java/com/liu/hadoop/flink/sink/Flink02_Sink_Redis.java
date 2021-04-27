package com.liu.hadoop.flink.sink;

import com.liu.hadoop.flink.beans.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description:  将结果发送 Redis
 *
 * Flink 没有类似于 spark 中 foreach 方法，让用户进行迭代的操作。虽有对外的输出操作都要利用 Sink 完成
 *
 *
 */
public class Flink02_Sink_Redis {

	public static void main(String[] args) {

		//创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//全局并行度
		env.setParallelism(1);

		//从文件中读取数据
		DataStream<String> dataStream = env.readTextFile("/home/liu/workspace/intellij_work/intellij_20201211/com-hadoop-flink/data/sensor.txt");


		DataStream<Sensor> map = dataStream.map(new MapFunction<String, Sensor>() {
			@Override
			public Sensor map(String s) throws Exception {
				String[] split = s.split(",");
				return new Sensor(split[0], new Long(split[1]), new Double(split[2]));
			}
		});

		// 定义redis连接配置
		FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
				.setHost("localhost")
				.setPort(6379)
				.build();


		map.addSink(new RedisSink<>(config, new RedisMapper<Sensor>() {
			// 定义保存数据到redis的命令，存成Hash表
			@Override
			public RedisCommandDescription getCommandDescription() {
				// 表名
				return new RedisCommandDescription(RedisCommand.HSET,"sensor_tmp");
			}

			@Override
			public String getKeyFromData(Sensor sensor) {
				// 设置 key
				return sensor.getId();
			}

			@Override
			public String getValueFromData(Sensor sensor) {
				// 设置 value
				return sensor.getTemperature().toString();
			}
		}));


		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
