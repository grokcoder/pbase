package org.apache.hadoop.hbase.regionserver.pbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.PStoreScanner;
import org.apache.hadoop.hbase.regionserver.RecordScanner;
import org.apache.hadoop.hbase.regionserver.memstore.PMemStore;
import org.apache.hadoop.hbase.regionserver.memstore.PMemStoreImpl;
import org.apache.hadoop.hbase.regionserver.pbase.util.ParquetReadUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by wangxiaoyi on 15/7/2.
 */
public class TestPStoreScanner {

    private static final Log LOG = LogFactory.getLog(TestPStoreScanner.class);

    public static Path rootPath = new Path("hdfs://localhost:9000/parquet");

    public static List<Path> paths = ParquetReadUtil.loadParquetFiles(new Path(rootPath, "pfile"));




    /**
     * init the memstore scanner
     * @return
     */
    public static RecordScanner getMemStoreScanner(){
        RecordScanner scanner = null;
        PMemStore memStore = new PMemStoreImpl(null);

        final int ROWS_LEN = 100;
        for(int i = 501 ; i <= 500 + ROWS_LEN; ++i){
            Put put = new Put(String.format("%10d", i).getBytes());
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), ("wangxiaoyi" + i).getBytes());
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), ("age" + i).getBytes());
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("job"), ("student" + i).getBytes());
            try {
                memStore.add(put);
            }catch (IOException ioe){
                ioe.printStackTrace();
            }
        }
        scanner = memStore.getScanner();
        return scanner;
    }



    @Test
    public void testIterate(){
        List<RecordScanner> scanners = new LinkedList<>();
        scanners.add(getMemStoreScanner());

        List<RecordScanner> pscanners = ParquetReadUtil.getParquetFileScanner(paths, null);
        scanners.addAll(pscanners);

        int start = 1, end = 600;
        Scan scan = new Scan(String.format("%10d", start).getBytes());
        scan.setStopRow(String.format("%10d", end + 1).getBytes());

        PStoreScanner scanner = new PStoreScanner(null, scan, 0l, scanners);

        for(; start <= end; ++ start){
            Assert.assertEquals(true, scanner.hasNext());

            List<Cell> peek = scanner.peek();
            Assert.assertEquals(false, peek.isEmpty());
            String peekRow = Bytes.toString(peek.get(0).getRow());
            Assert.assertEquals(String.format("%10d", start), peekRow);

            List<Cell> result = scanner.next();
            Assert.assertEquals(false, result.isEmpty());
            String resultRow = Bytes.toString(result.get(0).getRow());
            Assert.assertEquals(String.format("%10d", start), resultRow);

        }
    }

    @Test
    public void testClose(){
        List<RecordScanner> scanners = new LinkedList<>();
        scanners.add(getMemStoreScanner());

        List<RecordScanner> pscanners = ParquetReadUtil.getParquetFileScanner(paths, null);
        scanners.addAll(pscanners);

        int start = 1, end = 600;
        Scan scan = new Scan(String.format("%10d", start).getBytes());
        scan.setStopRow(String.format("%10d", end + 1).getBytes());

        PStoreScanner scanner = new PStoreScanner(null, scan, 0l, scanners);

        for(; start <= end; ++ start){
            if(start == 16) {
                Assert.assertEquals(false, scanner.hasNext());
                break;
            }
            Assert.assertEquals(true, scanner.hasNext());

            List<Cell> peek = scanner.peek();
            Assert.assertEquals(false, peek.isEmpty());
            String peekRow = Bytes.toString(peek.get(0).getRow());
            Assert.assertEquals(String.format("%10d", start), peekRow);

            List<Cell> result = scanner.next();
            Assert.assertEquals(false, result.isEmpty());
            String resultRow = Bytes.toString(result.get(0).getRow());
            Assert.assertEquals(String.format("%10d", start), resultRow);

            if(start == 15){
                scanner.close();
            }

        }
    }

    @Test
    public void testIterateWithStartKeyAndStopKey(){
        List<RecordScanner> scanners = new LinkedList<>();
        scanners.add(getMemStoreScanner());

        List<RecordScanner> pscanners = ParquetReadUtil.getParquetFileScanner(paths, null);
        scanners.addAll(pscanners);

        int start = 1, end = 600;
        Scan scan = new Scan();
        //scan.setStartRow(String.format("%10d", start).getBytes());
        scan.setStopRow(String.format("%10d", end + 1).getBytes());

        PStoreScanner scanner = new PStoreScanner(null, scan, 0l, scanners);

        for(; start <= end; ++ start){
            Assert.assertEquals(true, scanner.hasNext());

            List<Cell> peek = scanner.peek();
            Assert.assertEquals(false, peek.isEmpty());
            String peekRow = Bytes.toString(peek.get(0).getRow());
            Assert.assertEquals(String.format("%10d", start), peekRow);

            List<Cell> result = scanner.next();
            Assert.assertEquals(false, result.isEmpty());
            String resultRow = Bytes.toString(result.get(0).getRow());
            Assert.assertEquals(String.format("%10d", start), resultRow);

        }
    }





}
