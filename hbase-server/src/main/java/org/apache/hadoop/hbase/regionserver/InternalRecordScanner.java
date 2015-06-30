package org.apache.hadoop.hbase.regionserver;

/**
 * Created by wangxiaoyi on 15/6/23.
 */
public interface InternalRecordScanner extends RecordScanner{

    /**
     *
     * @return start key of this scanner
     */
    byte [] getStartKey();

    /**
     *
     * @return end key of this scanner
     */
    byte [] getEndKey();


    /**
     *
     * @return total records' count of this scanner
     */
    long getRecordCount();

    /**
     *
     * @return max result count left of this scanner
     */
    long getMaxResultsCount();

}
