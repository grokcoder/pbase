package org.apache.hadoop.hbase.regionserver.memstore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.InternalRecordScanner;
import org.apache.hadoop.hbase.regionserver.RecordScanner;
import org.apache.hadoop.hbase.regionserver.RowScanner;
import org.apache.hadoop.hbase.regionserver.UnexpectedStateException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * Created by wangxiaoyi on 15/5/6.
 * implement memstore for mutation
 *
 */
public class PMemStoreImpl implements PMemStore{

    private static final Log LOG = LogFactory.getLog(PMemStore.class);

    public final static long FIXED_OVERHEAD = ClassSize.align(
            ClassSize.OBJECT + (4 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_LONG));

    public final static long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD +
            ClassSize.ATOMIC_LONG + (0 * ClassSize.TIMERANGE_TRACKER) +
            (0 * ClassSize.CELL_SKIPLIST_SET) + (2 * ClassSize.CONCURRENT_SKIPLISTMAP));



    private Configuration conf;

    private volatile Map<byte[], Mutation> rowInMem;
    private volatile Map<byte[], Mutation> snapshotRowInMem;

    private volatile byte[] startkey = null;
    private volatile byte[] endkey = null;

    // Used to track own heapSize
    private AtomicLong memstoreSize;
    private volatile long snapshotSize;

    // Used to track when to flush
    volatile long timeOfOldestEdit = Long.MAX_VALUE;

    volatile long snapshotId;

    public PMemStoreImpl(Configuration conf){
        this.conf = conf;
        rowInMem = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
        snapshotRowInMem = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
        memstoreSize = new AtomicLong(DEEP_OVERHEAD);
        snapshotSize = 0;

    }

    /**
     * insert mutation into memstore
     * @param m
     * @return
     */

    @Override
    public long add(Mutation m) throws IOException{
        Mutation mutation = rowInMem.get(m.getRow());
        if(mutation != null){
            if(m instanceof Put)
                ((Put)mutation).mergePut((Put) m);
            else {
                //TODO: other mutation different merge function
            }
        }else {

            //TODO : make a efficient implementation
            if(startkey == null){
                startkey = m.getRow();
            }else {
                if(Bytes.compareTo(m.getRow(), startkey) < 0){
                    startkey = m.getRow();
                }
            }

            if(endkey == null){
                endkey = m.getRow();
            }else {
                if(Bytes.compareTo(endkey, m.getRow()) < 0){
                    endkey = m.getRow();
                }
            }

            rowInMem.put(m.getRow(), m);
        }
        memstoreSize.getAndAdd(m.heapSize());
        setOldestEditTimeToNow();
        return m.heapSize();
    }

    /**
     * get row from the memstore
     *
     * @param row
     */
    @Override
    public Mutation get(byte[] row) {
        Mutation m = rowInMem.get(row);
        return m;
    }

    /**
     * Write a delete
     *
     * @param m
     * @return approximate size of the passed Mutation
     */
    @Override
    public long delete(Mutation m) {
        if(m == null || rowInMem.get(m.getRow()) == null)
            return 0;
        else {
            rowInMem.remove(m.getRow());
        }
        memstoreSize.getAndSet(memstoreSize.get() - m.heapSize());
        setOldestEditTimeToNow();
        return m.heapSize();
    }

    /**
     * Creates a snapshot of the current memstore. Snapshot must be cleared by call to
     * {@link #clearSnapshot(long)}.
     *
     * @return {@link PMemStoreSnapshot}
     */
    @Override
    public PMemStoreSnapshot snapshot() {
        if (!this.snapshotRowInMem.isEmpty()) {
            //snapshotRowInMem.clear();
            LOG.warn("Snapshot called again without clearing previous. " +
                    "Doing nothing. Another ongoing flush or did we fail last attempt?");
            return null;
        }else {
            snapshotId = EnvironmentEdgeManager.currentTime();
            this.snapshotSize = dataSize();
            if(! rowInMem.isEmpty()){
                this.snapshotRowInMem = this.rowInMem;
                this.rowInMem = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
                this.memstoreSize.set(DEEP_OVERHEAD);
                timeOfOldestEdit = Long.MAX_VALUE;

            }

            PMemStoreSnapshot snapshot = new PMemStoreSnapshot(snapshotId,
                    snapshotRowInMem.size(),
                    snapshotSize,
                    getScanner(this.snapshotRowInMem),
                    startkey, endkey);

            this.startkey = null;
            this.endkey = null;
            return snapshot;
        }
    }

    /**
     * Clears the current snapshot of the Memstore.
     *
     * @param id
     * @throws UnexpectedStateException
     * @see #snapshot()
     */
    @Override
    public void clearSnapshot(long id) throws UnexpectedStateException {
        //MemStoreLAB tmpAllocator = null;
        if (this.snapshotId != id) {
            throw new UnexpectedStateException("Current snapshot id is " + this.snapshotId + ",passed "
                    + id);
        }
        // OK. Passed in snapshot is same as current snapshot. If not-empty,
        // create a new snapshot and let the old one go.
        if (!this.snapshotRowInMem.isEmpty()) {
            this.snapshotRowInMem = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
        }
        this.snapshotSize = 0;
        this.snapshotId = -1;
    }


    /**
     * @return Approximate 'exclusive deep size' of implementing object.  Includes
     * count of payload and hosting object sizings.
     */
    @Override
    public long heapSize() {
        return this.memstoreSize.get();
    }

    @Override
    public long getFlushableSize() {
        return this.snapshotSize > 0 ? snapshotSize : size();
    }


    /**
     * @return Oldest timestamp of all the Mutations in the MemStore
     */
    @Override
    public long timeOfOldestEdit() {
        return this.timeOfOldestEdit;
    }

    @Override
    public byte[] getStartKey() {
        return this.startkey;
    }

    @Override
    public byte[] getEndKey() {
        return this.endkey;
    }

    /**
     * @return Total memory occupied by this MemStore.
     */
    @Override
    public long size() {
        return heapSize();
    }

    public long dataSize(){
        return size() - DEEP_OVERHEAD;
    }

    void setOldestEditTimeToNow() {
        if (timeOfOldestEdit == Long.MAX_VALUE) {
            timeOfOldestEdit = EnvironmentEdgeManager.currentTime();
        }
    }

    /**
     * create scanner for {@link PMemStore}
     *
     * @return {@link PMemStoreScanner}
     */
    @Override
    public RowScanner getScanner() {
        return new PMemStoreScanner(this.rowInMem);
    }

    public RowScanner getScanner(Map<byte[], Mutation> rowInMem) {
        return new PMemStoreScanner(rowInMem);
    }



    void dump() {
       for(Map.Entry<byte[], Mutation> en : rowInMem.entrySet()){
           try {
               LOG.info(((Mutation) en.getValue()).toJSON());
           }catch (IOException ioe){
               LOG.error(ioe);
           }
       }
    }




    /**
     * row scanner for {@link PMemStore}
     */
    class PMemStoreScanner implements RowScanner, InternalRecordScanner{

        private byte[] curr = null;
        private byte[] next = null;
        private Iterator<byte []> it =null;
        private int countLeft = 0;

        private Map<byte[], Mutation> rowInMem;
        //private volatile Map<byte[], Mutation> snapshotRowInMem;

        private Mutation currRow;


        public PMemStoreScanner(Map<byte[], Mutation> rowInMem){
            this.rowInMem = rowInMem;
            countLeft = rowInMem.size();
            seek();
        }

        /**
         * seek the query row
         * @param row
         */
        public void seek(byte[] row){
            if(row == null) return;

            Set<byte []> rows = rowInMem.keySet();
            it = rows.iterator();
            boolean seekEd = false;
            while (it.hasNext()){
                curr = it.next();
                if(Bytes.compareTo(curr, row) >= 0){
                    seekEd = true;
                    break;
                }
            }
            if(it.hasNext()){
                next = it.next();
            }
            if(!seekEd){//查询数据不再该范围
                curr = null;
                next = null;
            }

        }

        /**
         * init use
         */
        public void seek(){
            Set<byte []> rows = rowInMem.keySet();
            it = rows.iterator();
            int count = 1;
            while (it.hasNext()){
                if(count == 1) {
                    curr = it.next();
                }
                if (count == 2){
                    next = it.next();
                    break;
                }
                count ++;
            }
        }

        /**
         * has next row
         * @return
         */
        public boolean hasNext(){
            return next == null ? false : true;
        }

        /**
         * return the next row
         * @return
         */

        public Mutation nextRow(){
            Mutation m = rowInMem.get(next);
            curr = next;
            next = it.hasNext() ? it.next() : null;
            countLeft --;
            return m;
        }


        /**
         * @return max result count left of this scanner
         */
        @Override
        public long getMaxResultsCount() {
            return countLeft;
        }

        /**
         * @return total records' count of this scanner
         */
        @Override
        public long getRecordCount() {
            return rowInMem.size();
        }


        /**
         * @return start key of this scanner
         */
        @Override
        public byte[] getStartKey() {//todo
            return new byte[0];
        }

        /**
         * return record
         */
        @Override
        public List<Cell> next() {
            List<Cell> cells = new LinkedList<>();
            Mutation m = nextRow();
            try {
                if(m != null){
                    CellScanner scanner = m.cellScanner();
                    while (scanner.advance()){
                        Cell cell = scanner.current();
                        cells.add(cell);
                    }
                }
            }catch (IOException ioe){
                LOG.error(ioe);
            }

            return cells;
        }

        /**
         * @return end key of this scanner
         */
        @Override
        public byte[] getEndKey() {
            return new byte[0];//todo
        }


        /**
         * don't iterate just
         *
         * @return first element of the scanner
         */
        @Override
        public List<Cell> peek() {
            if(next != null) {
                Mutation m = rowInMem.get(next);
                List<Cell> cells = new LinkedList<>();
                try {
                    if (m != null) {
                        CellScanner scanner = m.cellScanner();
                        while (scanner.advance()) {
                            Cell cell = scanner.current();
                            cells.add(cell);
                        }
                    }
                } catch (IOException ioe) {
                    LOG.error(ioe);
                }
                return cells;
            }

           return null;
        }

        /**
         * Closes this stream and releases any system resources associated
         * with it. If the stream is already closed then invoking this
         * method has no effect.
         *
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void close() throws IOException {

        }
    }


    public static void main(String []args)throws IOException{


        PMemStore memStore = new PMemStoreImpl(HBaseConfiguration.create());
        final int ROWS_LEN = 10;

        for(int i = 0 ; i < ROWS_LEN; ++i){
            Put put = new Put(Bytes.toBytes(i));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("value1"));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c2"), Bytes.toBytes("value2"));
            try {
                memStore.add(put);
            }catch (IOException ioe){
                ioe.printStackTrace();
            }
        }

        PMemStoreSnapshot snapshot = memStore.snapshot();
        System.out.println("snapshot id : " + snapshot.getId());
        System.out.println("snapshot size : " + snapshot.getSize());
        System.out.println("snapshot mutation size : " + snapshot.getMutationCount());
        System.out.println("startkey : " + Bytes.toInt(snapshot.getStartKey()));
        System.out.println("endkey : " + Bytes.toInt(snapshot.getEndKey()));



        RowScanner rowScanner = snapshot.getScanner();
        rowScanner.seek(Bytes.toBytes(10));

        //rowScanner.seek(Bytes.toBytes(0));
        while (rowScanner.hasNext()){

            List<Cell> cells = rowScanner.next();
            List<Cell> peek = rowScanner.peek();


            System.out.print("curr" + Bytes.toInt(cells.get(0).getRow()));
            System.out.println("\tpeek" + Bytes.toInt(peek.get(0).getRow()));



/*            Mutation m = rowScanner.nextRow();
            CellScanner scanner = m.cellScanner();
            //System.out.println("rowkey columnfamily column value");

            while (scanner.advance()){
                Cell cell = scanner.current();
                System.out.println(
                        Bytes.toInt(m.getRow()) + "\t\t" +
                                Bytes.toString(cell.getFamily()) + "\t\t" +
                                Bytes.toString(cell.getQualifier()) + "\t\t" +
                                Bytes.toString(cell.getValue()));
            }*/
        }




    }

}
