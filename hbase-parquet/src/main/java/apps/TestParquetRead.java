package apps;

/**
 * Created by wangxiaoyi on 15/5/25.
 */

import com.google.common.primitives.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetFileInfo;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.schema.GroupType;
import parquet.schema.MessageTypeParser;
import parquet.schema.Type;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;


/**
 * Created by wangxiaoyi on 15/4/20.
 * <p/>
 * <p/>
 * read all the data from parquet file by default
 * we can add the read schema into the read conf for read specific columns
 */


public class TestParquetRead {


    public static void main(String[] args) throws IOException {


        Path root = new Path("hdfs://localhost:9000/parquet/");//文件夹路径

        Configuration configuration = new Configuration();

        String schema = "  message test { " +
                "          required binary name; " +
                "          required int32 age;   " +
                " }";


        //自定义查询的列,read_schema可以自定义,查询schema可以由客户端创建，发送到服务器端执行。
        //configuration.set(ReadSupport.PARQUET_READ_SCHEMA,"" + schema);

        Path file = new Path(root, "people1.parquet");


        try {

            ParquetReader<Group> reader = ParquetReader.
                    builder(new GroupReadSupport(), file)
                    .withConf(configuration)
                    .build();


            ParquetFileInfo fileInfo = reader.getFileInfo();


            Group group = null;
            int rowCount = 0;
            while ((group = reader.read()) != null) {

                GroupType type  = group.getType();
                for(Type t : type.getFields()){
                    //byte [] v = group.get
                    byte [] value = group.getBinary(t.getName(), 0).getBytes();
                    int a = 1;


                }


                rowCount++;
                String name = new String(group.getBinary("name", 0).getBytes(), "UTF-8");
                int age = group.getInteger("age", 0);
                System.out.println("name : " + name + " age : " + age);
            }

            System.out.println("row count " + rowCount);

            reader.close();

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }
}
