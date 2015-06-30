package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.regionserver.memstore.PMemStoreSnapshot;
import org.apache.hadoop.hbase.util.Bytes;

import javax.print.DocFlavor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wangxiaoyi on 15/5/6.
 *
 * execute the real flush task
 */
public class ParquetStoreFlusher extends StoreFlusher {

    private static final Log LOG = LogFactory.getLog(DefaultStoreFlusher.class);
    private final Object flushLock = new Object();


    public ParquetStoreFlusher(Configuration conf, Store store) {
        super(conf, store);
    }


    /**
     * Turns a snapshot of memstore into a set of store files.
     *
     * @param snapshot         {@link PMemStoreSnapshot} snapshot.
     * @param cacheFlushSeqNum Log cache flush sequence number.
     * @param status           Task that represents the flush operation and may be updated with status.
     * @return List of files written. Can be empty; must not be null.
     */
    @Override
    public List<Path> flushSnapshot(PMemStoreSnapshot snapshot, long cacheFlushSeqNum, MonitoredTask status)
            throws IOException {
        if(snapshot.getMutationCount() == 0) return new ArrayList<>();
        ArrayList<Path> result = new ArrayList<Path>();

        Map<String, String> meta = new HashMap<>();
        meta.put(HConstants.START_KEY, Bytes.toString(snapshot.getStartKey()));
        meta.put(HConstants.END_KEY, Bytes.toString(snapshot.getEndKey()));

        PStoreFile.Writer writer = ((HStore)(store)).createParquetWriter(meta);
        if(writer == null) return result;

        RowScanner scanner = snapshot.getScanner();
        while (scanner.hasNext()){
            Mutation m = scanner.nextRow();
            writer.append(m);
        }
        writer.close();
        result.add(writer.getFilePath());
        return result;
    }


    /**************************************************************************************************************
     * for old memstore
     */

    /**
     * Turns a snapshot of memstore into a set of store files.
     *
     * @param snapshot         Memstore snapshot.
     * @param cacheFlushSeqNum Log cache flush sequence number.
     * @param status           Task that represents the flush operation and may be updated with status.
     * @return List of files written. Can be empty; must not be null.
     */
    @Override
    public List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushSeqNum, MonitoredTask status) throws IOException {
        return null;
    }

    /**
     * Performs memstore flush, writing data from scanner into sink.
     *
     * @param scanner           Scanner to get data from.
     * @param sink              Sink to write data to. Could be StoreFile.Writer.
     * @param smallestReadPoint Smallest read point used for the flush.
     */
    @Override
    protected void performFlush(InternalScanner scanner, Compactor.CellSink sink, long smallestReadPoint) throws IOException {
        super.performFlush(scanner, sink, smallestReadPoint);
    }

    /**end for old store*********************************************************************************************************/
}
