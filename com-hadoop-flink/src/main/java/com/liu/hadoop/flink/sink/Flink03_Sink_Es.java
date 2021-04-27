package com.liu.hadoop.flink.sink;

import com.liu.hadoop.flink.beans.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description:  将结果发送 ES
 *
 * Flink 没有类似于 spark 中 foreach 方法，让用户进行迭代的操作。虽有对外的输出操作都要利用 Sink 完成
 *
 *
 */
public class Flink03_Sink_Es {

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

		// 定义es的连接配置
		ArrayList<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost("localhost",9200));

		map.addSink( new ElasticsearchSink.Builder<Sensor>(httpHosts, new ElasticsearchSinkFunction<Sensor>() {
			@Override
			public void process(Sensor sensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
				// 定义写入的数据source
				HashMap<String, String> sensorMap = new HashMap<>();
				sensorMap.put("id",sensor.getId());
				sensorMap.put("time",sensor.getTimestamp().toString());
				sensorMap.put("temp",sensor.getTemperature().toString());

				// 创建请求，作为向es发起的写入命令
				IndexRequest source = Requests.indexRequest().index("demo").type("test").source(sensorMap);

				// 用requestIndexer发送请求
				requestIndexer.add(source);

			}
		}).build());




		//执行
		try {
			env.execute("demoTask1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
