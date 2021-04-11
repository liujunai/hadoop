package com.liu.hadoop.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * @author LiuJunFeng
 * @date 2021/4/11 上午10:10
 * @description:  HbaseAPI  旧
 */
public class HbaseDemo {

	/**

	public static boolean isTableExist(String tableName) throws IOException {

		//1.获取配置文件信息
		HBaseConfiguration conf = new HBaseConfiguration();
		conf.set("hbase.zookeeper.quorum","192.168.122.51,192.168.122.52,192.168.122.53");

		//2.获取管理员对象
		HBaseAdmin admin = new HBaseAdmin(conf);

		//3.判断表是否存在
		boolean exist = admin.tableExists(tableName);

		//4.关闭连接
		admin.close();

		return exist;
	}


	public static void main(String[] args) throws IOException {
		System.out.println(isTableExist("aaa"));
	}

	 */

}

