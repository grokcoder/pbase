package org.apache.hadoop.hbase.regionserver.pbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.RowScanner;
import org.apache.hadoop.hbase.regionserver.memstore.PMemStore;
import org.apache.hadoop.hbase.regionserver.memstore.PMemStoreImpl;
import org.apache.hadoop.hbase.regionserver.memstore.PMemStoreSnapshot;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import javax.swing.plaf.PanelUI;
import java.io.IOException;
import java.util.List;

/**
 * Created by wangxiaoyi on 15/7/2.
 */
public class TestPMemStoreImpl {

    public static final Log LOG = LogFactory.getLog(TestPMemStoreImpl.class);

    public static PMemStore memStore = new PMemStoreImpl(HBaseConfiguration.create());

    /**
     * init the Pmemstore
     * 1. row length = 100
     * 2. row key ranges form 1 to 100
     * 3. table schema {name, age, job}
     */

    public void initMemStore(){

        if(memStore.getRecordCount() != 0){
            LOG.error("memstore size should be zero at init stage! ");
            return;
        }

        final int ROWS_LEN = 100;
        for(int i = 1 ; i <= ROWS_LEN; ++i){
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
    }

    public void clearMemStore(){
        memStore = null;    //todo: really clear the memestore
    }

    @Test
    public void testSnapshot(){
        initMemStore();
        PMemStoreSnapshot snapshot = memStore.snapshot();

        long snapshotId = snapshot.getId();


        Assert.assertEquals("snapshot size should be equal to ", 100, snapshot.getMutationCount());
        Assert.assertArrayEquals("start key should be equal to ", String.format("%10d", 1).getBytes(), snapshot.getStartKey());
        Assert.assertArrayEquals("end key should be equal to ", String.format("%10d", 100).getBytes(), snapshot.getEndKey());

        try {
            memStore.clearSnapshot(snapshotId);
            Assert.assertEquals("curr snapshot id  ", -1l, memStore.getCurrSnapshotId());
        }catch (IOException ioe){
            LOG.error(ioe.getMessage());
        }

        memStore = new PMemStoreImpl(HBaseConfiguration.create());
    }

    @Test
    public void testStartKeyAndEndKey(){
        initMemStore();
        Assert.assertArrayEquals("start key should be equal to ", String.format("%10d", 1).getBytes(), memStore.getStartKey());
        Assert.assertArrayEquals("end key should be equal to ", String.format("%10d", 100).getBytes(), memStore.getEndKey());
        memStore = new PMemStoreImpl(HBaseConfiguration.create());
    }




    @Test
    public void testGetScanner(){
        initMemStore();

        RowScanner scanner = memStore.getScanner();

        for(int i = 1; i<= 100; ++i){
            Assert.assertEquals("should has next", true, scanner.hasNext());
            List<Cell> peek = scanner.peek();
            Assert.assertEquals("peek should not be empty ", true, !peek.isEmpty());
            String peekRow = Bytes.toString(peek.get(0).getRow());
            Assert.assertEquals("peek row should be equal to", String.format("%10d", i),  peekRow);
            //test peek info
            List<Cell>result = scanner.next();
            Assert.assertEquals("result should not be empty ", true, ! result.isEmpty());
            String resultRow = Bytes.toString(result.get(0).getRow());
            Assert.assertEquals("row should be equal ", String.format("%10d", i), resultRow);
            //test result
        }


        memStore = new PMemStoreImpl(HBaseConfiguration.create());
    }

    @Test
    public void testScannerSeek(){
        initMemStore();

        RowScanner scanner = memStore.getScanner();
        scanner.seek(String.format("%10d", 0).getBytes());

        List<Cell> peek = scanner.peek();
        Assert.assertEquals("peek should not be empty ", true, !peek.isEmpty());
        String peekRow = Bytes.toString(peek.get(0).getRow());
        Assert.assertEquals("peek row should be equal to", String.format("%10d", 1), peekRow);


        for(int i = 1; i<= 100; i += 2){

            Assert.assertEquals("should has next", true, scanner.hasNext());
            scanner.seek(String.format("%10d", i).getBytes());
            List<Cell> peek1 = scanner.peek();
            Assert.assertEquals("peek should not be empty ", true,  !peek1.isEmpty());
            String peekRow1 = Bytes.toString(peek1.get(0).getRow());
            Assert.assertEquals("peek row should be equal to", String.format("%10d", i), peekRow1);
        }

        scanner.seek(String.format("%10d", 200).getBytes());

        List<Cell> peek3 = scanner.peek();
        Assert.assertEquals("peek should  be empty ", true, peek3.isEmpty());

        memStore = new PMemStoreImpl(HBaseConfiguration.create());
    }



}
