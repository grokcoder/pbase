package org.apache.hadoop.hbase.client.api;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Created by wangxiaoyi on 15/6/30.
 */
public class Test {

    public static void main(String []args){

        TableSchema schema = new TableSchema("people");
        schema.addColumnDescripor("name", FIELD_RULE.repeated, FIELD_TYPE.binary);
        schema.addColumnDescripor("age", FIELD_RULE.repeated, FIELD_TYPE.binary);
        schema.addColumnDescripor("job", FIELD_RULE.repeated, FIELD_TYPE.binary);

        Configuration conf = HBaseConfiguration.create();
        PBaseAdmin admin = new PBaseAdmin(conf);
        admin.createTable(schema);

    }
}
