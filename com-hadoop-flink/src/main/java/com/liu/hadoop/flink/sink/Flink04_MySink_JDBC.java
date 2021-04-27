package com.liu.hadoop.flink.sink;

import com.liu.hadoop.flink.beans.Sensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


/**
 * @author liu
 * @date 2021/1/5 下午6:28
 * @description: 将结果发送 JDBC
 * <p>
 * Flink 没有类似于 spark 中 foreach 方法，让用户进行迭代的操作。虽有对外的输出操作都要利用 Sink 完成
 */
public class Flink04_MySink_JDBC extends RichSinkFunction<Sensor> {

	// 声明连接和预编译语句
	Connection connection = null;
	PreparedStatement insertStmt = null;
	PreparedStatement updateStmt = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/liu", "liu", "a");
		insertStmt = connection.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)");
		updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
	}

	// 每来一条数据，调用连接，执行sql
	@Override
	public void invoke(Sensor value, Context context) throws Exception {
		// 直接执行更新语句，如果没有更新那么就插入
		updateStmt.setDouble(1, value.getTemperature());
		updateStmt.setString(2, value.getId());
		updateStmt.execute();
		if (updateStmt.getUpdateCount() == 0) {
			insertStmt.setString(1, value.getId());
			insertStmt.setDouble(2, value.getTemperature());
			insertStmt.execute();
		}

	}

	@Override
	public void close() throws Exception {
		insertStmt.close();
		updateStmt.close();
		connection.close();
	}
}

