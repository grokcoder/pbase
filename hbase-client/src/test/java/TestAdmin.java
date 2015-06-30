import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.omg.IOP.TAG_ALTERNATE_IIOP_ADDRESS;

import java.io.IOException;

/**
 * Created by wangxiaoyi on 15/4/27.
 */
public class TestAdmin {


    public static void main(String []args){
        String schema =  " message people { " +
                "          required binary rowkey; " +
                "          repeated binary cf:name;  " +
                "          repeated binary cf:age;   " +
                "          repeated binary cf:country; "+
                "          required int64 timestamp;"+
                " }";


        Configuration conf = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(conf)){

            Admin admin = connection.getAdmin();
            TableName table = TableName.valueOf("people1");
            HTableDescriptor htdp = new HTableDescriptor(table);

            HColumnDescriptor hcdp = new HColumnDescriptor("cf");
            htdp.addFamily(hcdp);
            htdp.setValue(HConstants.SCHEMA,schema);

            admin.createTable(htdp);
        }catch (IOException io){
            io.printStackTrace();
        }

    }
}
