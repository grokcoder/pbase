package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;

import java.io.IOException;
import java.util.List;

/**
 * Created by wangxiaoyi on 15/6/1.
 *
 *  RegionScanner describes iterators over rows in an HRegion with parquet files
 *
 */


public interface PRegionScanner{


    /**
     * @return The RegionInfo for this scanner.
     */
    HRegionInfo getRegionInfo();


    /**
     * Do a reseek to the required row. Should not be used to seek to a key which
     * may come before the current position. Always seeks to the beginning of a
     * row boundary.
     *
     * @throws IOException
     * @throws IllegalArgumentException
     *           if row is null
     *
     */
    boolean seek(byte[] row) throws IOException;


    /**
     * @return The preferred max buffersize. See
     * {@link org.apache.hadoop.hbase.client.Scan#setMaxResultSize(long)}
     */
    long getMaxResultSize();



    /**
     * @return next row
     */
    List<Cell> nextRaw();

    /**
     * judge whether has more record to iterate
     * @return
     */
    boolean hasNext();


    /**
     * close the region scanner
     * @throws IOException
     */
    void close() throws IOException;


    /**
     * return the curr top row of the region
     * @return
     */
    List<Cell> peek();


    /**
     * whether the curr row is the stop row
     * @param row
     * @return
     */
    boolean isStopRow(byte[] row);



}
