package org.apache.hadoop.hbase.mytest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by wangxiaoyi on 15/5/6.
 */
public class TestPut {


    public static void main(String []args){

        try {
            TableName tn = TableName.valueOf("people1");
            Configuration conf = HBaseConfiguration.create();
            try (Connection connection = ConnectionFactory.createConnection(conf)) {
                try (Table table = connection.getTable(tn)) {
                    for(int i = 200; i<= 299; i++) {
                        Put put = new Put(String.format("%07d", i).getBytes());
                        put.addColumn("cf".getBytes(), "name".getBytes(), ("wangxiaoyi"+i).getBytes());
                        put.addColumn("cf".getBytes(), "age".getBytes(), (""+i).getBytes());
                        put.addColumn("cf".getBytes(), "country".getBytes(), ("China"+i).getBytes());
                        put.setAttribute("type", "parquet".getBytes());
                        table.put(put);
                    }
                    System.out.println("done!");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            //LOG.error(e.getMessage());
        }

    }
}
