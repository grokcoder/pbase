package org.apache.hadoop.hbase.client.api;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;


/**
 * Created by wangxiaoyi on 15/6/30.
 */
public class Matcher {


    private Scan scan = null;
    private TableName tableName = null;

    public Matcher(String tableName, TableSchema schema){
        this.tableName = TableName.valueOf(tableName);
        scan = new Scan();
        if(schema == null){
            scan.setAttribute(HConstants.SCHEMA, "empty".getBytes());
        }else {
            scan.setAttribute(HConstants.SCHEMA, schema.getTableSchema().getBytes());
        }
    }



    public Matcher setStartRow(byte[] startRow){
        scan.setStartRow(startRow);
        return this;
    }

    public Matcher setStopRow(byte[] stopRow){
        scan.setStopRow(stopRow);
        return this;
    }

    /**
     * set the num of records to fetch form server
     * @param num
     * @return
     */
    public Matcher setCachingRows(int num){
        scan.setCaching(num);
        return this;
    }


    public Scan getScan(){
        return scan;
    }

    public TableName getTableName(){
        return tableName;
    }

}
