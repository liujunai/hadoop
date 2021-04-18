package com.liu.hadoop.spark.sql.customize;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
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
 *
 * 通过继承 UserDefinedAggregateFunction 来实现用户自定义弱类型聚合函数
 *
 * 自定义聚合函数类：计算年龄的平均值
 *      1. 继承UserDefinedAggregateFunction
 *      2. 重写方法(8)
 */
public class MyUDAF extends UserDefinedAggregateFunction {


	//1、函数的输入参数的数据类型
	@Override
	public StructType inputSchema() {
		List<StructField> inputFields = new ArrayList<>();
		inputFields.add(DataTypes.createStructField("age", DataTypes.DoubleType, true));

		return DataTypes.createStructType(inputFields);
	}

	//2、缓冲区中的数据类型.（有序性）
	@Override
	public StructType bufferSchema() {
		List<StructField> bufferFields = new ArrayList<>();
		bufferFields.add(DataTypes.createStructField("age", DataTypes.DoubleType, true));
		bufferFields.add(DataTypes.createStructField("count", DataTypes.DoubleType, true));

		return DataTypes.createStructType(bufferFields);
	}

	//3、返回值的数据类型
	@Override
	public DataType dataType() {
		return DataTypes.DoubleType;
	}

	//4、函数是否总是在相同的输入上返回相同的输出,一般为true
	@Override
	public boolean deterministic() {
		return true;
	}

	//5、初始化给定的聚合缓冲区,在索引值为0的sum=0;索引值为1的count=1;
	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, 0D);
		buffer.update(1, 0D);

	}

	// 6、根据输入的值更新缓冲区数据
	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		buffer.update(0, buffer.getDouble(0) + input.getDouble(0));
		buffer.update(1, buffer.getDouble(1) + 1);
	}

	//7、合并两个聚合缓冲区,并将更新后的缓冲区值存储回“buffer1”
	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0));
		buffer1.update(1, buffer1.getDouble(1) + buffer2.getDouble(1));
	}

	//8、计算出最终结果
	@Override
	public Object evaluate(Row buffer) {
		return buffer.getDouble(0) / buffer.getDouble(1);
	}
}
