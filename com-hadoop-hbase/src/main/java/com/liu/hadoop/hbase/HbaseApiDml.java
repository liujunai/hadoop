package com.liu.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/11 上午10:24
 * @description: HbaseAPI  DML:
 * <p>
 * 1.插入数据
 * 2.查询数据（get）
 * 3.查询数据（scan）
 * 4.删除数据
 */
public class HbaseApiDml {

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

		//1.插入数据
//		addData("liu","10001","info1","sex","123");

		//2.查询数据（get）
//		getData("liu", "10001", "info1", "");

		//3.查询数据（scan）
//		scanData("liu");

		//4.删除数据
		deleteData("liu","10001","","");

		close();
	}

	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/11 下午3:37
	 * @Description: 1.插入数据
	 */
	public static void addData(String tableName, String rowKey, String cf, String column, String value) throws IOException {

		//1.获取表对象
		Table table = conn.getTable(TableName.valueOf(tableName));

		//2.创建put对象
		Put put = new Put(Bytes.toBytes(rowKey));

		//3.向put对象中组装数据
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

		//4.插入数据
		table.put(put);

		//5.关闭连接
		table.close();
	}

	/**
	 * @return void
	 * @author LiuJunFeng
	 * @date 2021/4/11 下午3:41
	 * @Description:  2.查询数据（get）
	 */
	public static void getData(String tableName, String rowKey, String cf, String column) throws IOException {

		//1.获取表对象
		Table table = conn.getTable(TableName.valueOf(tableName));

		//2.创建get对象
		Get get = new Get(Bytes.toBytes(rowKey));

		if ((cf != null && cf.length() > 0) && (column != null && column.length() > 0)) {
			//指定列族和列查询
			get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column));
		} else if (cf != null && cf.length() > 0) {
			//指定列族查询
			get.addFamily(Bytes.toBytes(cf));
		}

		//3.获取数据
		Result result = table.get(get);

		//4.解析result
		for (Cell cell : result.rawCells()) {
			System.out.println("RowKey =" + Bytes.toString(CellUtil.cloneRow(cell)) +
					",CF =" + Bytes.toString(CellUtil.cloneFamily(cell)) +
					",Column =" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
					",value =" + Bytes.toString(CellUtil.cloneValue(cell)));
		}

		//5.关闭表连接
		table.close();

	}

	/**
	 * @author LiuJunFeng
	 * @date 2021/4/11 下午4:25
	 * @Description:  3.查询数据（scan）
	 * @return void
	*/
	public static void scanData(String tableName) throws IOException {

		//1.获取表对象
		Table table = conn.getTable(TableName.valueOf(tableName));

		//2.创建scan对象
		Scan scan = new Scan();

		//3.扫描表
		ResultScanner resultScanner = table.getScanner(scan);

		//4.解析resultScanner
		for (Result result : resultScanner) {

			//5.解析result
			for (Cell cell : result.rawCells()) {
				System.out.println("RowKey =" + Bytes.toString(CellUtil.cloneRow(cell)) +
						",CF =" + Bytes.toString(CellUtil.cloneFamily(cell)) +
						",Column =" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
						",value =" + Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		//6.关闭表连接
		table.close();
	}

	/**
	 * @author LiuJunFeng
	 * @date 2021/4/11 下午5:04
	 * @Description:  4.删除数据
	 * @return void
	*/
	public static void deleteData(String tableName, String rowKey, String cf, String column) throws IOException {

		//1.获取表对象
		Table table = conn.getTable(TableName.valueOf(tableName));

		//2.获取删除对象
		Delete delete = new Delete(Bytes.toBytes(rowKey));

		//删除指定列族
//		delete.addFamily(Bytes.toBytes(cf));

		//删除指定列族和列名
//		delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column));

		//3.执行删除操作
		table.delete(delete);

		//4.关闭表连接
		table.close();
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
