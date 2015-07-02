package org.apache.hadoop.hbase.regionserver.pbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.pfile.PFileReader;
import org.apache.hadoop.hbase.regionserver.RecordScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by wangxiaoyi on 15/6/11.
 *
 * help parquet file read test
 */
public class ParquetReadUtil {

    public static Path rootPath = new Path("hdfs://localhost:9000/parquet");


    /**
     * load file path
     * @param path of the dir
     * @return
     */
    public static List<Path> loadParquetFiles(Path path){
        FileSystem fs = HDFSUtil.loadFileSystem(path);
        try {
            FileStatus[] statuses = fs.listStatus(path);
            List<Path> paths = new LinkedList<>();
            for(FileStatus status : statuses){
                paths.add(status.getPath());
            }
            return paths;
        }catch (IOException ioe){
            System.out.println(ioe.getMessage());
        }
        return new LinkedList<>();
    }

    /**
     * get parquet file scanner
     * @param paths parquet files
     * @param schema for parquet reader
     * @return
     */
    public static List<RecordScanner> getParquetFileScanner(List<Path> paths, MessageType schema){

        List<RecordScanner> scanners = new LinkedList<>();
        for(Path file : paths){
            try {
                PFileReader reader = new PFileReader(file, new Configuration(), schema);
                RecordScanner scanner = reader.getScanner();
                scanners.add(scanner);
            }catch (IOException ioe){

            }
        }

        return scanners;
    }


    @Test
    public void testReadWriteValue(){
        List<Path> paths = loadParquetFiles(new Path(rootPath, "pfile"));
        List<RecordScanner> scanners = getParquetFileScanner(paths, null);

        List<String> rowkeys = new LinkedList<>();

        for(RecordScanner scanner : scanners){
            while (scanner.hasNext()){
                List<Cell> cells = scanner.next();
                if(!cells.isEmpty()){
                    rowkeys.add(Bytes.toString(cells.get(0).getRow()));
                }
            }
        }

        for(int i = 0; i < 500; ++i){
            String row = String.format("%10d", i + 1);
            org.junit.Assert.assertEquals("read row key is not equals to the wrote row key", row, rowkeys.get(i));
        }
    }


}
