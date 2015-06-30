package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by wangxiaoyi on 15/6/8.
 */

public class ScannerHeap {

    private static final Log LOG = LogFactory.getLog(RecordScanner.class);


    protected PriorityQueue<RecordScanner> heap = null;

    //current scanner
    protected RecordScanner curr = null;

    protected RecordScannerComparator comparator;

    public ScannerHeap(List<? extends RecordScanner> scanners, RecordScannerComparator comparator) throws IOException{

        this.comparator = comparator;
        if( !scanners.isEmpty()){
            this.heap = new PriorityQueue<>(scanners.size(), comparator);
            for(RecordScanner scanner : scanners){
                if(scanner.peek() != null){
                    heap.add(scanner);
                }else {
                    scanner.close();
                }
            }
        }
        this.curr = getCurrentScanner();
    }

    public ScannerHeap(List<? extends RecordScanner> scanners) throws IOException{

        this.comparator = new RecordScannerComparator();

        if( !scanners.isEmpty()){
            this.heap = new PriorityQueue<>(scanners.size(), comparator);
            for(RecordScanner scanner : scanners){
                if(scanner.peek() != null){
                    heap.add(scanner);
                }else {
                    scanner.close();
                }
            }
        }
        this.curr = getCurrentScanner();
    }


    public RecordScanner getCurrentScanner(){
        if (curr != null)
            return curr;
        else {
            if(heap != null)
                return heap.poll();
            else
                return null;
        }
    }



    /**
     * don't iterate just
     *
     * @return first element of the scanner
     */
    public List<Cell> peek() {
        if(curr == null)
            return null;
        else
            return curr.peek();
    }


    /**
     * @return weather there has more record
     */
    public boolean hasNext() {
        if(curr == null)
            return false;
        else {
            return curr.hasNext();
        }
    }

    /**
     * return record
     */
    public List<Cell> next() {
        List<Cell> result = new LinkedList<>();
        if(curr != null){
            result = curr.next();
            if(! curr.hasNext()){
                try {
                    curr.close();
                }catch (IOException ioe){
                    LOG.error(ioe.getMessage());
                }
                curr = heap.poll();
            }else {
                RecordScanner topScanner = heap.peek();
                List<Cell> nextResult = curr.peek();//下一组数据同 堆顶元素进行比较

                if(topScanner != null && Bytes.compareTo(nextResult.get(0).getRow(), topScanner.peek().get(0).getRow()) >= 0){
                    heap.add(curr);
                    curr = heap.poll();
                }
            }
        }
        return result;
    }


    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * @throws IOException if an I/O error occurs
     */
    public void close() throws IOException {
        if(curr != null){
            curr.close();
        }
        if(heap != null){
            RecordScanner scanner ;
            while ((scanner = heap.poll()) != null){
                scanner.close();
            }
        }
    }

    public boolean seek(byte [] row){
        return false;
    }

}
