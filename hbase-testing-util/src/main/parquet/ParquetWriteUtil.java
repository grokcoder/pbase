package main.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.column.ParquetProperties;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.Map;

/**
 * Created by wangxiaoyi on 15/6/10.
 *
 * help parquet write in test
 *
 */
public class ParquetWriteUtil {


    public static Path root = new Path("hdfs://10.214.208.11:9000/parquet/");

    public static Configuration conf = new Configuration();

    public static MessageType schema = MessageTypeParser.parseMessageType(
            " message people { " +

                    "required binary rowkey;" +
                    "required binary cf:name;" +
                    "required binary cf:age;" +
                    "required int64 timestamp;" +

                    " }"
    );

    public static SimpleGroupFactory sfg = new SimpleGroupFactory(schema);


    public static Path initFile(String fileName){
        return new Path(root, fileName);
    }


    public static Map<String, String> initMeta(Map<String, String> metas){
        return metas;
    }


    public static ParquetWriter<Group> initWriter(String fileName, Map<String, String> metas)
    throws IOException{


        GroupWriteSupport.setSchema(schema, conf);


        ParquetWriter<Group> writer = new ParquetWriter<Group>(
                initFile(fileName),
                new GroupWriteSupport(metas),
                CompressionCodecName.UNCOMPRESSED,
                1024,
                1024,
                512,
                true,
                false,
                ParquetProperties.WriterVersion.PARQUET_1_0,
                conf);

        return writer;
    }





    public static void main(String[] args) throws IOException {


        int fileNum = 10;
        int fileRecordNum = 1000;
        int rowKey = 0;
        for(int i = 0; i < fileNum; ++ i ) {//构造10个文件

            ParquetWriter<Group> writer = initWriter("hbase/table" + i, null);

            for (int j = 0;  j < fileRecordNum; ++j) {//不同文件记录
                rowKey ++;
                Group group = sfg.newGroup().append("rowkey", "r" + rowKey)
                        .append("cf:name", "wangxiaoyi" + rowKey)
                        .append("cf:age", "24")
                        .append("timestamp", System.currentTimeMillis());
                writer.write(group);
            }

            writer.close();
        }



    }
}
