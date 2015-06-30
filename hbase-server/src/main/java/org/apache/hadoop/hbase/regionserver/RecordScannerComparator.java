package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Comparator;
import java.util.List;

/**
 * Created by wangxiaoyi on 15/6/19.
 */
public class RecordScannerComparator implements Comparator<RecordScanner> {


    @Override
    public int compare(RecordScanner o1, RecordScanner o2) {

        if(o2 == null && o1 != null){
            return 1;
        }

        if(o2 == null && o1 == null){
            return 0;
        }

        if(o2 != null && o1 == null){
            return  -1;
        }

        List<Cell> cells1 = o1.peek();
        List<Cell> cells2 = o2.peek();


        byte[] r1 = null;
        byte[] r2 = null;

        if(cells1 != null){
            r1 = cells1.get(0).getRow();
        }

        if(cells2 != null){
            r2 = cells2.get(0).getRow();
        }

        return Bytes.compareTo(r1, r2);
    }
}
