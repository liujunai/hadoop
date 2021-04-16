package com.liu.hadoop.spark.core.rdd.partitions;

import org.apache.spark.Partitioner;

/**
 * @author LiuJunFeng
 * @date 2021/4/16 上午4:03
 * @description: 自定义分区器
 */
public class MyPartitions extends Partitioner {

	/**
	 * @return int
	 * @author LiuJunFeng
	 * @date 2021/4/16 上午4:20
	 * @Description: 指定分区数量
	 */
	@Override
	public int numPartitions() {
		return 3;
	}

	/**
	 * @return int
	 * @author LiuJunFeng
	 * @date 2021/4/16 上午4:20
	 * @Description: 指定分区规则
	 *
	 * 根据数据的key值返回数据所在的分区索引（从0开始）
	 */
	@Override
	public int getPartition(Object key) {

		if ("人族".equals(key)) {
			return 0;
		} else if ("魔族".equals(key)) {
			return 1;
		} else {
			return 2;
		}
	}
}
