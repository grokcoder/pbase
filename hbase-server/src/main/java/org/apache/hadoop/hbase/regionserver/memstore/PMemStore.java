package org.apache.hadoop.hbase.regionserver.memstore;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.regionserver.MemStoreSnapshot;
import org.apache.hadoop.hbase.regionserver.RecordScanner;
import org.apache.hadoop.hbase.regionserver.RowScanner;
import org.apache.hadoop.hbase.regionserver.UnexpectedStateException;

import java.io.IOException;
import java.util.Map;


/**
 * Created by wangxiaoyi on 15/5/6.
 */
public  interface PMemStore extends HeapSize{


    long getFlushableSize();


    /**
     * Creates a snapshot of the current memstore. Snapshot must be cleared by call to
     * {@link #clearSnapshot(long)}.
     * @return {@link MemStoreSnapshot}
     */
    PMemStoreSnapshot snapshot();

    /**
     * create scanner for {@link PMemStore}
     * @return {@link RowScanner}
     */
    RowScanner getScanner();

    /**
     * create scanner for {@link PMemStore}
     * @return {@link RowScanner}
     */
    RowScanner getScanner(Map<byte[], Mutation> mutations);

    /**
     * create scanner for {@link PMemStore} snapshot
     * @return {@link RecordScanner}
     */
    RecordScanner getSnapshotScanner();

    /**
     * Clears the current snapshot of the Memstore.
     * @param id
     * @throws UnexpectedStateException
     * @see #snapshot()
     */
    void clearSnapshot(long id) throws UnexpectedStateException;


    /**
     * @return Oldest timestamp of all the Mutations in the MemStore
     */
    long timeOfOldestEdit();


    /**
     * @return Total memory occupied by this MemStore.
     */
    long size();



    /**
     * Write a delete
     * @param m
     * @return approximate size of the passed Mutation
     */
    long delete(final Mutation m);


    long add(final Mutation m) throws IOException;

    /**
     * get row from the memstore
     * @param row
     */
    Mutation get(byte[] row);


    byte[] getStartKey();

    byte[] getEndKey();

    int getRecordCount();

    long getCurrSnapshotId();

}
