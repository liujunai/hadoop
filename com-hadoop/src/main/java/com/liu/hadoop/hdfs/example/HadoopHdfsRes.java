package com.liu.hadoop.hdfs.example;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class HadoopHdfsRes
{

    public static FileSystem Before() throws IOException
    {
        Configuration conf = new Configuration();
        conf.addResource("src/main/resources/core-site.xml");
        conf.addResource("src/main/resources/hdfs-site.xml");
        conf.addResource("src/main/resources/mapred-site.xml");
        conf.addResource("src/main/resources/yarn-site.xml");

        FileSystem fs =  fs = FileSystem.get(conf);
        return fs;
    }

    public static void After(FileSystem fs) throws IOException
    {

        fs.close();
    }


}
