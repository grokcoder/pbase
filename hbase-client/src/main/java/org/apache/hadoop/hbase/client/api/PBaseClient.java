package org.apache.hadoop.hbase.client.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

/**
 * Created by wangxiaoyi on 15/6/30.
 */
public class PBaseClient {

    private static final Log LOG = LogFactory.getLog(PBaseClient.class);
    private static final String PUT_KEY = "type";//for server to identify the parquet put
    private static final byte[] PUT_VALUE = "parquet".getBytes();


    private Configuration conf;

    public PBaseClient(Configuration conf) {
        this.conf = conf;
    }

    /**
     * put a record
     * @param put
     * @param tableName
     */
    public void put(Put put, TableName tableName) {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            try (Table table = connection.getTable(tableName)) {
                    put.setAttribute(PUT_KEY, PUT_VALUE);
                    table.put(put);
            }
        }catch (IOException ioe){
            LOG.error(ioe.getMessage());
        }
    }

    /**
     * bath put for better performance
     * @param puts
     * @param tableName
     */
    public void batchPut(List<Put> puts, TableName tableName){
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            try (Table table = connection.getTable(tableName)) {
                for(Put put : puts) {
                    put.setAttribute(PUT_KEY, PUT_VALUE);
                    table.put(put);
                }
            }
        }catch (IOException ioe){
            LOG.error(ioe.getMessage());
        }
    }


    /**
     * scan the database
     * @param matcher
     * @return
     */
    public ResultScanner scan(Matcher matcher) {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            try (Table table = connection.getTable(matcher.getTableName())) {
                return table.getScanner(matcher.getScan());
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return null;
    }
}
