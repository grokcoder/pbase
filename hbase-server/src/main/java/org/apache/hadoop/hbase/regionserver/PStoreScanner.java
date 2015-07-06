package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by wangxiaoyi on 15/6/16.
 */

public class PStoreScanner implements RecordScanner{

    private static final Log LOG = LogFactory.getLog(PStoreScanner.class);
    protected Store store;

    protected ScannerHeap heap;

    protected final Scan scan;

    // A flag whether use pread for scan
    private boolean scanUsePread = false;

    protected ReentrantLock lock = new ReentrantLock();

    private final long readPt;



    public PStoreScanner(Store store, final Scan scan, final long readPt, List<? extends RecordScanner> scanners){
        this.store = store;
        this.scan = scan;
        this.readPt = readPt;
        try {
            this.heap = new ScannerHeap(scanners);
        }catch (IOException ioe){
            LOG.error(ioe.getMessage());
        }
    }


    /**
     * don't iterate just
     *
     * @return first element of the scanner
     */

    public List<Cell> peek() {
        List<Cell> cells = new LinkedList<>();

        lock.lock();
        try{
            if(this.heap == null) return cells;
            else cells =  this.heap.peek();
        }finally {
            lock.unlock();
        }
        return cells;
    }


    /**
     * @return weather there has more record
     */

    public boolean hasNext() {

        if(this.heap == null || ! this.heap.hasNext()){
            close();
            return false;
        }
        lock.lock();
        try {

            if(! Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
                List<Cell> peek = this.heap.peek();
                byte[] peekRow = peek.get(0).getRow();
                if(Bytes.compareTo(peekRow, scan.getStopRow()) >= 0){
                    this.close();
                    return false;
                }
            }else {
                return this.heap.hasNext();
            }

        }finally {
            lock.unlock();
        }
        return  true;
    }


    boolean checkReseek(){
        return false;
    }

    /**
     * return record
     */
    public List<Cell> next() {
        List<Cell> result = new LinkedList<>();

        lock.lock();
        try{
            if (checkReseek()) {
                return result;
            }
            result = heap.next();
        }finally {
            lock.unlock();
        }
        return result;
    }


    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     */
    public void close(){

        lock.lock();
        try {
            // under test, we dont have a this.store
            if (this.store != null)
                //todo : this.store.deleteChangedReaderObserver(this);
            try {
                if (this.heap != null)
                    this.heap.close();
            }catch (IOException ioe){
                LOG.error(ioe.getMessage());
            }
            this.heap = null;
            //this.lastTop = null; // If both are null, we are closed.
        }finally {
            lock.unlock();
        }
    }

    /**
     * TODO: seek the record by rowkey
     *
     * @param rowkey
     */
    @Override
    public void seek(byte[] rowkey) {

    }
}
