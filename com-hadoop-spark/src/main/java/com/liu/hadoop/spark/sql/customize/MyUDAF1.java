package com.liu.hadoop.spark.sql.customize;

import com.liu.hadoop.spark.sql.basic.Person;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


/**
 * @author LiuJunFeng
 * @date 2021/4/18 下午3:55
 * @description: 自定义 UDAF 函数
 * <p>
 * <p>
 * 从 Spark3.0 版本后，UserDefinedAggregateFunction 已经不推荐使用了。可以统一采用  Aggregator  强类型聚合函数
 * <p>
 * 自定义聚合函数类：计算年龄的平均值
 * 1. 继承 org.apache.spark.sql.expressions.Aggregator, 定义泛型
 * K : 输入的数据类型 Long
 * V : 缓冲区的数据类型 Buff
 * C : 输出的数据类型 Double
 * 2. 重写方法(6)
 */
public class MyUDAF1 extends Aggregator<Double, Buff, Double> {

	// 1、缓冲区的初始化
	@Override
	public Buff zero() {
		return new Buff(0D, 0D);
	}

	// 2、根据输入的数据更新缓冲区的数据
	@Override
	public Buff reduce(Buff buff, Double in) {
		buff.setAgeTotal(buff.getAgeTotal() + in);
		buff.setCount(buff.getCount() + 1);
		return buff;
	}

	// 3、合并缓冲区
	@Override
	public Buff merge(Buff b1, Buff b2) {
		b1.setAgeTotal(b1.getAgeTotal() + b2.getAgeTotal());
		b1.setCount(b1.getCount() + b2.getCount());
		return b1;
	}

	// 4、计算结果
	@Override
	public Double finish(Buff buff) {
		return buff.getAgeTotal() / buff.getCount();
	}

	// 5、缓冲区的编码操作
	@Override
	public Encoder<Buff> bufferEncoder() {
		return Encoders.bean(Buff.class);
	}

	// 6、输出的编码操作
	@Override
	public Encoder<Double> outputEncoder() {
		return Encoders.DOUBLE();
	}
}
