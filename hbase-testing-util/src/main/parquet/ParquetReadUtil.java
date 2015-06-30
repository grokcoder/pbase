package main.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.pfile.PFileReader;
import org.apache.hadoop.hbase.regionserver.RecordScanner;
import org.apache.hadoop.hbase.util.Bytes;
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

    //public static Path rootPath = new Path("hdfs://10.214.208.11:9000/parquet/");



    public static Path rootPath = new Path("hdfs://localhost:9000/hbase/data/default/test7/0f8c8c42a9552e443b14030a35e1d902/");


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
            PFileReader reader = new PFileReader(file, new Configuration(), schema);

            System.out.println("Start Key : " + Bytes.toInt(reader.getStartKey()));
            System.out.println("End KEY : " + Bytes.toInt(reader.getEndKey()));

            RecordScanner scanner = reader.getScanner();
            scanners.add(scanner);
        }

        return scanners;
    }

    public static void main(String []args){
        List<Path> paths = loadParquetFiles(new Path(rootPath, "cf1"));
        List<RecordScanner> scanners = getParquetFileScanner(paths, null);

        for(RecordScanner scanner : scanners){
            while (scanner.hasNext()){
                List<Cell> cells = scanner.next();
                for(Cell cell : cells){
                    System.out.println(Bytes.toInt(cell.getRow()) + " " + Bytes.toString(cell.getFamily())
                            + " " + Bytes.toString(cell.getQualifier()) + " " + Bytes.toString(cell.getValue())
                            + " " + cell.getTimestamp());
                }
            }
        }
    }



}
