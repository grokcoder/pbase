package org.apache.hadoop.hbase.regionserver.pbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import parquet.column.ParquetProperties;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wangxiaoyi on 15/7/2.
 *
 * generate some parquet file
 * for testing the scanners
 *
 * 1.generate 10 separate files
 * 2.each file contains 50 record
 * 3.each file record sorted by row key
 * 4.row key ranges from 1 to 500
 *
 */
public class GenerateParquetFile {


    public static Path root = new Path("hdfs://localhost:9000/parquet/");

    public static Configuration conf = new Configuration();

    public static MessageType schema = MessageTypeParser.parseMessageType(
            " message people { " +
                    "required binary rowkey;" +
                    "required binary cf:name;" +
                    "required binary cf:age;" +
                    "required binary cf:job;" +
                    "required int64 timestamp;" +
                    " }"
    );

    public static SimpleGroupFactory sfg = new SimpleGroupFactory(schema);


    public static Path initFile(String fileName){
        return new Path(root, fileName);
    }



    public static ParquetWriter<Group> initWriter(String fileName, Map<String, String> metas)
            throws IOException{


        GroupWriteSupport.setSchema(schema, conf);


        ParquetWriter<Group> writer = new ParquetWriter<Group>(
                initFile(fileName),
                new GroupWriteSupport(metas),
                CompressionCodecName.SNAPPY,
                1024,
                1024,
                512,
                true,
                false,
                ParquetProperties.WriterVersion.PARQUET_1_0,
                conf);

        return writer;
    }


    public static String genRowKey(String format, int i){
        return String.format(format, i);
    }

    public static void main(String []args) throws IOException{


        int fileNum = 10;   //num of files constructed
        int fileRecordNum = 50; //record num of each file
        int rowKey = 0;
        for(int i = 0; i < fileNum; ++ i ) {

            Map<String, String> metas = new HashMap<>();
            metas.put(HConstants.START_KEY, genRowKey("%10d", rowKey + 1));
            metas.put(HConstants.END_KEY, genRowKey("%10d", rowKey + fileRecordNum));

            ParquetWriter<Group> writer = initWriter("pfile/scanner_test_file" + i, metas);

            for (int j = 0;  j < fileRecordNum; ++j) {
                rowKey ++;
                Group group = sfg.newGroup().append("rowkey", genRowKey("%10d", rowKey))
                        .append("cf:name", "wangxiaoyi" + rowKey)
                        .append("cf:age", String.format("%10d", rowKey))
                        .append("cf:job", "student")
                        .append("timestamp", System.currentTimeMillis());
                writer.write(group);
            }

            writer.close();
        }
    }


}
