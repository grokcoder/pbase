package org.apache.hadoop.hbase.client.api;


import org.apache.hadoop.conf.Configuration;

/**
 * Created by wangxiaoyi on 15/9/14.
 */
public class CommonTest {

    public static void main(String []args){

        Configuration conf1 = new Configuration();

        long start = System.currentTimeMillis();

        //Configuration configuration = new Configuration(conf1);
        //configuration.set("key", "value");
        conf1.set("key", "value"); // set operation is time consuming

        long end = System.currentTimeMillis();



        System.out.println((end - start)/1000.0 + " sec");//测试一下configuration 创建的时间

        System.out.println("k-v in conf1 : " + conf1.get("key"));
        //System.out.println("k-v in configuration : " + configuration.get("key"));

    }

}
