package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;

import java.io.Closeable;
import java.util.List;



/**
 * Created by wangxiaoyi on 15/6/4.
 * scanner for each record
 */

public interface RecordScanner extends Closeable{

    /**
     * don't iterate just
     * @return first element of the scanner
     */
    List<Cell> peek();

    /**
     *
     * @return weather there has more record
     */
    boolean hasNext();


    /**
     *
     * return record
     */
    List<Cell> next();


    /**
     * seek the record by rowkey
     * @param rowkey
     */
    void seek(byte [] rowkey);


}
