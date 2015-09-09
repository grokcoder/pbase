package org.apache.hadoop.hbase.client.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;


/**
 * Created by wangxiaoyi on 15/7/1.
 */
public class TestPBaseAdmin {


    public static void main(String []args) {
        TableSchema schema = new TableSchema("people2");
        schema.addColumnDescriptor("name", FIELD_RULE.repeated, FIELD_TYPE.binary);
        schema.addColumnDescriptor("age", FIELD_RULE.repeated, FIELD_TYPE.binary);
        schema.addColumnDescriptor("job", FIELD_RULE.repeated, FIELD_TYPE.binary);

        Configuration conf = HBaseConfiguration.create();
        PBaseAdmin admin = new PBaseAdmin(conf);
        admin.createTable(schema);
    }

}
