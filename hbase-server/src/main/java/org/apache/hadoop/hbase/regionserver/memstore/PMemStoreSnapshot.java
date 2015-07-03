package org.apache.hadoop.hbase.regionserver.memstore;

import org.apache.hadoop.hbase.regionserver.RowScanner;

/**
 * Created by wangxiaoyi on 15/5/6.
 * snapshot of PMemStore
 */

public class PMemStoreSnapshot {

    private final long id;
    private final int mutationCount;
    private final long size;

    private final byte[] startKey;
    private final byte[] endKey;

    private final RowScanner scanner;


    public PMemStoreSnapshot(final long id, final int mutationCount,
                             final long size, RowScanner scanner, byte[] startKey, byte[] endKey){

        this.id = id;
        this.mutationCount = mutationCount;
        this.size = size;
        this.scanner = scanner;
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public long getId() {
        return id;
    }

    public int getMutationCount() {
        return mutationCount;
    }

    public boolean isEmpty(){
        return scanner.hasNext();
    }

    public long getSize() {
        return size;
    }

    public RowScanner getScanner(){
        return this.scanner;
    }

    public byte[] getStartKey(){
        return startKey;
    }

    public byte[] getEndKey(){
        return endKey;
    }
}
