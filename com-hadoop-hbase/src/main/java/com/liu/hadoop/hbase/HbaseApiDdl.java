package com.liu.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/11 上午10:24
 * @description: HbaseAPI DDL:
 *
 * 1.判断表是否存在
 * 2.创建命名空间
 * 3.创建表
 * 4.删除表
 * 5.列出数据库中所有的表
 *
 */
public class HbaseApiDdl {

	private static Connection conn = null;
	private static Admin admin = null;

	static {
		try {
			//1.获取配置文件信息
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "192.168.122.51,192.168.122.52,192.168.122.53");

			//2.创建连接对象
			conn = ConnectionFactory.createConnection(conf);

			//3.创建Admin对象
			admin = conn.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {

		//1.判断表是否存在
//		System.out.println(isTableExist("liu1"));

		//2.创建命名空间
//		createNameSpace("1989");

		//3.创建表
//		createTable("liu1", "info1", "info2");

		//4.删除表
//		deleteTable("liu1");

		//5.列出数据库中所有的表
		listTables();

//		System.out.println(isTableExist("liu1"));

		close();
	}

	/**
	 * @return boolean
	 * @author LiuJunFeng
	 * @date 2021/4/11 上午10:46
	 * @Description: 1.判断表是否存在
	 */
	public static boolean isTableExist(String tableName) throws IOException {

		return admin.tableExists(TableName.valueOf(tableName));
	}

	/**
	 * @author LiuJunFeng
	 * @date 2021/4/11 下午12:22
	 * @Description:  2.创建命名空间
	 * @return void
	*/
	public static void createNameSpace(String ns){

		//1.创建命名空间描述器
		NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();

		//2.创建命名空间
		try {
			admin.createNamespace(namespaceDescriptor);
		}catch (NamespaceExistException e){
			System.out.println(ns + "  命名空间已存在");
		}catch (IOException e) {
			e.printStackTrace();
		}


	}

	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/11 上午10:58
	 * @Description: 3.创建表
	 */
	public static void createTable(String tableName, String... cfs) throws IOException {

		//1.判断列族
		if (cfs.length <= 0) {
			System.out.println("cfs = " + "列族信息不能为空");
			return;
		}

		//2.判断表名
		if (isTableExist(tableName)) {
			System.out.println("tableName = " + "表名信息不能为空");
			return;
		}

		//3.创建表描述器
		TableDescriptorBuilder tableDesBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

		//4.循环添加列族信息
		List<ColumnFamilyDescriptor> colFamilyList = new ArrayList<>();
		for (String cf : cfs) {
			ColumnFamilyDescriptor colFamilyDes = ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build();
			colFamilyList.add(colFamilyDes);
		}

		//5.添加具体列族信息
		TableDescriptor tableDes = tableDesBuilder.setColumnFamilies(colFamilyList).build();

		admin.createTable(tableDes);

	}

	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/11 下午12:11
	 * @Description: 4.删除表
	 */
	public static void deleteTable(String tableName) throws IOException {

		//1.判断表是否存在
		if (!isTableExist(tableName)) {
			System.out.println("tableName = " + "表不存在");
		}

		//2.使表下线
		admin.disableTable(TableName.valueOf(tableName));

		//3.删除表
		admin.deleteTable(TableName.valueOf(tableName));

	}

	/**
	 * @author LiuJunFeng
	 * @date 2021/4/11 下午12:35
	 * @Description:  5.列出数据库中所有的表
	 * @return void
	*/
	public static void listTables() throws IOException{
		for(TableName table:admin.listTableNames()) {
			System.out.println(table);
		}
	}



	public static void close() {

		if (admin != null) {
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if (conn != null) {
			try {
				conn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
}
