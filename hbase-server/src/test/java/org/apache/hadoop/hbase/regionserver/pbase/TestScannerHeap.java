package org.apache.hadoop.hbase.regionserver.pbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.RecordScanner;
import org.apache.hadoop.hbase.regionserver.ScannerHeap;
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
public class TestScannerHeap {

    private static final Log LOG = LogFactory.getLog(TestScannerHeap.class);

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
        scanner = memStore.getScanner(null);
        return scanner;
    }


    /**
     * construct the scanner heap
     * @return
     */
    public static ScannerHeap getScannerHeap(){
        ScannerHeap scannerHeap = null;
        List<RecordScanner> scanners = new LinkedList<>();
        scanners.add(getMemStoreScanner());

        List<RecordScanner> pscanners = ParquetReadUtil.getParquetFileScanner(paths, null);
        scanners.addAll(pscanners);

        try {
            scannerHeap = new ScannerHeap(scanners);
        }catch (IOException ioe){
            LOG.error(ioe.getMessage());
        }
        return scannerHeap;
    }



    @Test
    public void testIterate(){
        ScannerHeap heap = getScannerHeap();

        final int RECORD_NUM = 600;
        for(int i = 1; i <= RECORD_NUM; ++ i){
            Assert.assertEquals(true, heap.hasNext());

            List<Cell> peek = heap.peek();
            Assert.assertEquals(true, !peek.isEmpty());
            String peekRow = Bytes.toString(peek.get(0).getRow());
            Assert.assertEquals(String.format("%10d", i), peekRow);
            List<Cell> cells = heap.next();
            Assert.assertEquals(true, ! cells.isEmpty());
            String expectedRow = Bytes.toString(cells.get(0).getRow());
            Assert.assertEquals(String.format("%10d", i), expectedRow);
        }
        Assert.assertEquals(false, heap.hasNext());
    }


}
