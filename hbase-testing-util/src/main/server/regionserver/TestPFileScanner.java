package main.server.regionserver;

import main.parquet.ParquetReadUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.RecordScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * Created by wangxiaoyi on 15/6/26.
 */
public class TestPFileScanner {

    public static void main(String [] args){

        Path rootPath = new Path("hdfs://localhost:9000/hbase/data/default/test7/0f8c8c42a9552e443b14030a35e1d902/");

        List<Path> paths = ParquetReadUtil.loadParquetFiles(new Path(rootPath, "cf1"));
        List<RecordScanner> scanners = ParquetReadUtil.getParquetFileScanner(paths, null);

        RecordScanner scanner = scanners.get(0);
        byte [] row = Bytes.toBytes(1);
        scanner.seek(row);


    }

}
