package com.liu.hadoop.hdfs.example;

import org.apache.hadoop.fs.*;
import org.junit.Test;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;

public class HadoopHdfsDemo
{
    /**
     * 创建文件夹 mkdir
     */
    @Test
    public void mkdir() throws URISyntaxException, InterruptedException {
        Path path = new Path("/liuDemo");

        try
        {
            FileSystem fs = HadoopHdfsRes.Before();
            if (fs.exists(path)){
                System.out.println("path = " + "目录已存在");
            }else {
                Boolean res = fs.mkdirs(path);
                System.out.println(res);
            }
            HadoopHdfsRes.After(fs);
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }
    /**
     * 新建文件
     */
    @Test
    public void write() throws URISyntaxException, InterruptedException {
        Path path = new Path("/liutest.txt");
        try
        {
            FileSystem fs = HadoopHdfsRes.Before();
            if (fs.exists(path)){
                System.out.println("path = " + "文件已存在");
            }else {
                FSDataOutputStream fsdos = fs.create(path);
                fsdos.writeBytes("Hello Hadoop");
                fsdos.close();
                System.out.println("文件创建成功");
            }
            HadoopHdfsRes.After(fs);
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }
    /**
     * 查看HDFS上的文件内容
     */
    @Test
    public void read() throws URISyntaxException, InterruptedException {
        Path path = new Path("/liutest.txt");
        try
        {
            FileSystem fs = HadoopHdfsRes.Before();
            if (fs.exists(path)){
                FSDataInputStream fsdis = fs.open(path);
                InputStreamReader isr =  new InputStreamReader(fsdis);//创建输入流
                BufferedReader br = new BufferedReader(isr);//按行读取
                String str = br.readLine();
                while(str!=null)
                {
                    System.out.println(str);
                    str = br.readLine();
                }
                br.close();
                isr.close();
                fsdis.close();
            }else {
                System.out.println("文件不存在");
            }
            HadoopHdfsRes.After(fs);
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }

    /**
     * 上传文件到HDFS
     */
    @Test
    public void upload() throws URISyntaxException, InterruptedException {
        try
        {
            FileSystem fs = HadoopHdfsRes.Before();
            fs.copyFromLocalFile(new Path("/home/titanic/liu/upload.txt"),new Path("/liutest"));
            HadoopHdfsRes.After(fs);
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }
    /**
     * 文件重命名
     */
    @Test
    public void rename() throws URISyntaxException, InterruptedException {
        Path path1 = new Path("/liutest/upload.txt");
        Path path2 = new Path("/liutest/rename.txt");
        try
        {
            FileSystem fs = HadoopHdfsRes.Before();
            if (!fs.exists(path1)){
                System.out.println("path1 = " + "文件不存在！");
            }
            if (fs.exists(path2)){
                System.out.println("path2 = " + "文件已存在！");
            }
            fs.rename(path1,path2);
            HadoopHdfsRes.After(fs);
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }
    /**
     * 从HDFS下载文件
     */
    @Test
    public void download() throws URISyntaxException, InterruptedException {
        Path path = new Path("/liutest/rename.txt");
        try
        {
            FileSystem fs = HadoopHdfsRes.Before();
            if (!fs.exists(path)){
                System.out.println("文件不存在！");
            }else {
                fs.copyToLocalFile(path,new Path("/home/titanic/liu"));
            }
            HadoopHdfsRes.After(fs);
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }
    /**
     * 查询某目录下所有文件 ls
     */
    @Test
    public void query() throws URISyntaxException, InterruptedException {
        Path path = new Path("/");
        try
        {
            FileSystem fs = HadoopHdfsRes.Before();
            if (!fs.exists(path)){
                System.out.println("目录不存在！");
            }
            FileStatus[] fslist = fs.listStatus(path);
            for (FileStatus fileStatus : fslist)
            {
                System.out.println(fileStatus.getPath().toString());
            }
            HadoopHdfsRes.After(fs);
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }
    /**
     * 删除文件
     */
    @Test
    public void delete() throws URISyntaxException, InterruptedException {
        try
        {
            FileSystem fs = HadoopHdfsRes.Before();
            boolean res = fs.delete(new Path("/xxxx"),true);
            System.out.println("res = " + res);
            HadoopHdfsRes.After(fs);
        } catch (IOException e)
        {
            e.printStackTrace();
        }

    }
}
