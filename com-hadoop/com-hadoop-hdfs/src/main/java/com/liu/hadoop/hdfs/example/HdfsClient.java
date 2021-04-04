package com.liu.hadoop.hdfs.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author LiuJunFeng
 * @date 2021/4/4 下午5:16
 * @description:
 */
public class HdfsClient {

	FileSystem fs;

	/**
	 * @author LiuJunFeng
	 * @date 2021/4/4 下午5:40
	 * @Description:  创建连接
	 * @return void
	*/
	@Before
	public void before() throws IOException, URISyntaxException, InterruptedException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		fs = FileSystem.get(new URI("hdfs://liu1:8020"),configuration,"liu");

	}



	@Test
	public void testMkdirs() throws IOException {
		fs.mkdirs(new Path("/test1/"));
	}

	/**
	 * @author LiuJunFeng
	 * @date 2021/4/4 下午5:39
	 * @Description: 关闭资源
	 * @return void
	*/
	@After
	public void after() throws IOException {
		fs.close();
	}

}