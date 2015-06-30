package org.apache.hadoop.hbase.mytest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by wangxiaoyi on 15/5/18.
 */
public class TestScan {

    public static void main(String []args){

        try {
            TableName tn = TableName.valueOf("people1");
            Configuration conf = HBaseConfiguration.create();
            try (Connection connection = ConnectionFactory.createConnection(conf)) {
                try (Table table = connection.getTable(tn)) {

                    Scan scan = new Scan();
                    scan.setStartRow(String.format("%07d",200).getBytes());
                    scan.setStopRow(String.format("%07d", 299).getBytes());
                    scan.setCaching(100);
                    //scan.setStopRow(Bytes.toBytes(125));

                    String schema =  " message test { " +
                            "          required binary rowkey; " +
                            "          repeated binary cf1:c1;  " +
                            "          repeated binary cf1:c2;   " +
                            " }";

                    scan.setAttribute(HConstants.SCHEMA, schema.getBytes());


                    ResultScanner rs = table.getScanner(scan);

                    Iterator<Result> it = rs.iterator();
                    while (it.hasNext()){
                        Result result = it.next();
                        while (result.advance()){
                            Cell cell = result.current();
                            System.out.print(Bytes.toString(cell.getRow()) + "\t");
                            System.out.print(Bytes.toString(cell.getFamily()) + "\t");
                            System.out.print(Bytes.toString(cell.getQualifier()) + "\t");
                            System.out.print(Bytes.toString(cell.getValue()) + "\n");

                        }
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            //LOG.error(e.getMessage());
        }

    }
}
