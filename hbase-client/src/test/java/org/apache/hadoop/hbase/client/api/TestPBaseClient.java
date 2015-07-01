package org.apache.hadoop.hbase.client.api;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by wangxiaoyi on 15/7/1.
 */
public class TestPBaseClient {

    public static final Configuration conf = HBaseConfiguration.create();
    public static final PBaseClient client = new PBaseClient(conf);
    public static final TableName tableName = TableName.valueOf("people2");

    public static void main(String []args){
        testPut();
        //testScan();
    }


    public static void testPut(){

        List<Put> puts = new LinkedList<>();
        for(int i = 10001; i < 1000000; i++) {
            Put put = new Put(String.format("%07d", i).getBytes());
            put.addColumn("cf".getBytes(), "name".getBytes(), ("wangxiaoyi" + i).getBytes());
            put.addColumn("cf".getBytes(), "age".getBytes(), ("" + i).getBytes());
            put.addColumn("cf".getBytes(), "job".getBytes(), ("student" + i).getBytes());
            puts.add(put);
        }
        client.batchPut(puts, tableName);
        System.out.println("done");
    }


    public static void testScan(){
        Matcher matcher = new Matcher(tableName.getNameAsString(), null)
                .setCachingRows(100)
                .setStartRow(String.format("%07d", 89).getBytes());
               // .setStopRow(String.format("%07d", 100).getBytes());


        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            try (Table table = connection.getTable(matcher.getTableName())) {
                ResultScanner rs = table.getScanner(matcher.getScan());

                Iterator<Result> it = rs.iterator();
                while (it.hasNext()){
                    Result result = it.next();
                    while (result.advance()){
                        Cell cell = result.current();
                        System.out.print(Bytes.toString(cell.getRow()) + "\t");
                        System.out.print(Bytes.toString(cell.getQualifier()) + "\t");
                        System.out.print(Bytes.toString(cell.getValue()) + "\t");

                    }
                    System.out.println();
                }

            }
        } catch (IOException e) {
            //LOG.error(e.getMessage());
        }

    }
}
