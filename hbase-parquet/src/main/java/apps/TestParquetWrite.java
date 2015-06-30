package apps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.column.ColumnDescriptor;
import parquet.column.ParquetProperties;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.Type;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by wangxiaoyi on 15/4/20.
 */
public class TestParquetWrite {


    public static void main(String[] args) throws IOException {


        Path root = new Path("hdfs://10.214.208.11:9000/parquet/");//文件夹路径

        Configuration configuration = new Configuration();

        MessageType schema = MessageTypeParser.parseMessageType( //parquet文件模式

                " message people { " +

                        "required binary rowkey;" +
                        "required binary cf:name;" +
                        "required binary cf:age;" +
                        "required int64 timestamp;"+
                 " }");

        GroupWriteSupport.setSchema(schema, configuration);

        SimpleGroupFactory sfg = new SimpleGroupFactory(schema);
        Path file = new Path(root, "people002.parquet");

        Map<String, String> meta = new HashMap<String, String>();
        meta.put("startkey", "1");
        meta.put("endkey", "2");


        ParquetWriter<Group> writer = new ParquetWriter<Group>(
                file,
                new GroupWriteSupport(meta),
                CompressionCodecName.UNCOMPRESSED,
                1024,
                1024,
                512,
                true,
                false,
                ParquetProperties.WriterVersion.PARQUET_1_0,
                configuration);

        Group group = sfg.newGroup().append("rowkey", "1")
                      .append("cf:name", "wangxiaoyi")
                      .append("cf:age", "24")
                      .append("timestamp", System.currentTimeMillis());


        for (int i = 0; i < 10000; ++i) {
            writer.write(
                    sfg.newGroup()
                            .append("name", "wangxiaoyi" + i)
                            .append("age", i));
        }
        writer.close();


    }
}
