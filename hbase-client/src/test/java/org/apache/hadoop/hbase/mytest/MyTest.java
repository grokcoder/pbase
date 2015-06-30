package org.apache.hadoop.hbase.mytest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Created by xiaoyi on 15-3-13.
 */
public class MyTest {

    static final Log LOG = LogFactory.getLog(MyTest.class);

    private static HBaseAdmin hBaseAdmin = null;

    private static Configuration conf = null;

    private static ClusterConnection clusterConnection = null;


    public static boolean createTable(Admin admin, HTableDescriptor table, byte[][] splits)
            throws IOException {
        try {
            admin.createTable(table, splits);
            return true;
        } catch (TableExistsException e) {
            LOG.info("table " + table.getNameAsString() + " already exists");
            // the table already exists...
            return false;
        }
    }


    public static void createTable(){
        try {
            TableName tn = TableName.valueOf("test010");
            conf = HBaseConfiguration.create();
            try (Connection connection = ConnectionFactory.createConnection(conf)) {
                try (Table table = connection.getTable(tn)) {
                    Put put = new Put("ROW1".getBytes());
                    put.addColumn("CF1".getBytes(),"column1".getBytes(),"value1".getBytes());
                    put.addColumn("CF2".getBytes(),"column1".getBytes(),"value1".getBytes());
                    table.put(put);
                    System.out.println("done!");

                }
            }





        } catch (IOException e) {
            LOG.error(e.getMessage());
        }


        // System.out.println("Connected");

    }

    public static void main(String[] args) throws InterruptedException{

        new MyTest().createTable();

    }

}
