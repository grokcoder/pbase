package org.apache.hadoop.hbase.client.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by wangxiaoyi on 15/6/30.
 *
 * admin for table management
 */
public class PBaseAdmin {

    private Configuration conf;
    private TableSchema schema;



    public PBaseAdmin(Configuration conf){
        this.conf = conf;
    }

    /**
     * create table with particular schema
     * @param schema
     */
    public void createTable(TableSchema schema){
        this.schema = schema;
        try (Connection connection = ConnectionFactory.createConnection(conf)){

            Admin admin = connection.getAdmin();
            TableName table = TableName.valueOf(schema.getName());
            HTableDescriptor htdp = new HTableDescriptor(table);

            HColumnDescriptor hcdp = new HColumnDescriptor(schema.getCF());
            htdp.addFamily(hcdp);
            htdp.setValue(HConstants.SCHEMA,schema.getTableSchema());

            admin.createTable(htdp);
        }catch (IOException io){
            io.printStackTrace();
        }
    }





}
