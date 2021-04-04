package com.liu.hadoop.hdfs.example;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HadoopHdfsRes
{

    public static FileSystem Before() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://liu1:8020"),conf,"liu");
        return fs;
    }

    public static void After(FileSystem fs) throws IOException {
        fs.close();
    }


}
