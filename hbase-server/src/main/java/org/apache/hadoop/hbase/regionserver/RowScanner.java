package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.client.Mutation;
import parquet.example.data.Group;

/**
 * Created by wangxiaoyi on 15/5/12.
 *
 * scan row by rowkey
 */

public interface RowScanner extends RecordScanner{

    /**
     * @return the next {@link Mutation}
     */
    Mutation nextRow();


    boolean hasNext();

}
