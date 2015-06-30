package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
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

    // if heap == null and lastTop != null, you need to reseek given the key below
    protected List<Cell> lastTop = null;


    // A flag whether use pread for scan
    private boolean scanUsePread = false;
    protected ReentrantLock lock = new ReentrantLock();

    private final long readPt;
    protected boolean closing = false;

    protected volatile boolean hasNext = true;




    public PStoreScanner(Store store, final Scan scan, final long readPt, List<? extends RecordScanner> scanners){
        this.store = store;
        this.scan = scan;
        this.readPt = readPt;
        try {
            this.heap = new ScannerHeap(scanners);
        }catch (IOException ioe){

        }
    }


    /**
     * don't iterate just
     *
     * @return first element of the scanner
     */

    public List<Cell> peek() {
        List<Cell> cells = null;

        lock.lock();
        try{
            if(this.heap == null) cells = this.lastTop;
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
        return  this.hasNext;
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

            // if the heap was left null, then the scanners had previously run out anyways, close and
            // return.
            if (this.heap == null) {
                close();
                return result;
            }

            result = heap.next();
            byte [] row = result.get(0).getRow();

            if(scan.getStopRow() != null && Bytes.compareTo(row, scan.getStopRow()) >= 0){// judge whether the curr row is bigger than stop row of scan
                this.hasNext = false;
                this.close();
                return null;
            }else {
                this.hasNext = heap.hasNext();
                if(hasNext == false){
                    this.close();
                }
            }

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
            if (this.closing) return;
            this.closing = true;
            // under test, we dont have a this.store
            if (this.store != null)
                //todo : this.store.deleteChangedReaderObserver(this);
            try {
                if (this.heap != null)
                    this.heap.close();
            }catch (IOException ioe){
                LOG.error(ioe.getMessage());
            }
            this.heap = null; // CLOSED!
            //this.lastTop = null; // If both are null, we are closed.
        }finally {
            lock.unlock();
        }
    }

    /**
     * seek the record by rowkey
     *
     * @param rowkey
     */
    @Override
    public void seek(byte[] rowkey) {

    }
}
