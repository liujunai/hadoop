package com.liu.hive.example;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class HiveJDBCDemo {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://192.168.122.51:10000/liu";
    private static String user = "liu";
    private static String password = "a";

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;


    // 加载驱动、创建连接
    @Before
    public void init() throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url,user,password);
        stmt = conn.createStatement();
    }

    // 创建数据库
    @Test
    public void createDatabase() throws Exception {
        String sql = "create database hive_jdbc_test";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 查询所有数据库
    @Test
    public void showDatabases() throws Exception {
        String sql = "show databases";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    // 删除数据库
    @Test
    public void dropDatabase() throws Exception {
        String sql = "drop database if exists hive_jdbc_test";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 创建表
    @Test
    public void createTable() throws Exception {
        String sql = "create table liutable(yy string," +
                "c_region string," +
                "c_dept string," +
                "c_count string" +
                ")" +
                "row format delimited fields terminated by ','";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 查询所有表
    @Test
    public void showTables() throws Exception {
        String sql = "show tables";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    // 查看表结构
    @Test
    public void descTable() throws Exception {
        String sql = "desc liutable";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1) + "=" + rs.getString(2));
        }
    }


    // 加载数据
    @Test
    public void loadData() throws Exception {
        //服务器上的路径，不是本地路径
        String filePath = "/home/liu/mm_c_dept_data.csv";
        String sql = "load data local inpath '" + filePath + "' overwrite into table liutable";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 查询数据
    @Test
    public void selectData() throws Exception {
        String sql = "select * from liutable";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        System.out.println("日期" + "--" + "区域" + "--" + "部门" + "--" + "办件量");
        while (rs.next()) {
            System.out.println(rs.getString("yy") + "--" +
                    rs.getString("c_region") + "--" +
                    rs.getString("c_dept") + "--" +
                    rs.getString("c_count"));
        }
    }

    // 统计查询（会运行mapreduce作业）
    @Test
    public void countData() throws Exception {
        String sql = "select count(1) from liutable";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getInt(1) );
        }
    }

    // 删除数据库表
    @Test
    public void deopTable() throws Exception {
        String sql = "drop table if exists liutable";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }



    // 释放资源
    @After
    public void destory() throws Exception {
        if ( rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

}
