package org.apache.hadoop.hbase.regionserver.pbase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.pfile.PFileReader;
import org.apache.hadoop.hbase.regionserver.RecordScanner;
import org.apache.hadoop.hbase.regionserver.pbase.util.ParquetReadUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Created by wangxiaoyi on 15/7/2.
 *
 */

public class TestPFileReaderAndScanner {

    public static Path rootPath = new Path("hdfs://localhost:9000/parquet");

    public static List<Path> paths = ParquetReadUtil.loadParquetFiles(new Path(rootPath, "pfile"));
    public static List<RecordScanner> scanners = ParquetReadUtil.getParquetFileScanner(paths, null);



    @Test
    public void testGetStartKeyAndEndKey(){
        int i = 0;
        for(int j = 0; j < 10; ++ j){
            PFileReader.PFileScanner scanner = (PFileReader.PFileScanner)scanners.get(j);
            Assert.assertEquals("start key not equal ", Bytes.toString(scanner.getStartKey()), String.format("%10d", i + 1));
            Assert.assertEquals("end key not equal ", Bytes.toString(scanner.getEndKey()), String.format("%10d", i + 50));
            i += 50;
        }
    }


    @Test
    public  void testSeek(){// test on scanner0
        PFileReader.PFileScanner scanner = (PFileReader.PFileScanner) scanners.get(0);
        int start = 2, end = 49;
        for(int i = start; i < end; ++ i){
            String expectedRowKey = String.format("%10d", i);
            scanner.seek(expectedRowKey.getBytes());
            List<Cell> result = scanner.peek();
            Assert.assertEquals("should have peek", true, !result.isEmpty());
            String actualRowkey = Bytes.toString(result.get(0).getRow());
            Assert.assertEquals("seek error !", expectedRowKey, actualRowkey);

        }

        //bigger than startkey
        scanner.seek(String.format("%10d", 0).getBytes());
        List<Cell> result = scanner.peek();
        Assert.assertEquals("seek error! ", false, result.isEmpty());
        if(!result.isEmpty()){
            String rowkey = Bytes.toString(result.get(0).getRow());
            Assert.assertEquals("seek error !", String.format("%10d", 48), rowkey);
        }

        //bigger than endKey
        scanner.seek(String.format("%10d", 200).getBytes());
        List<Cell> result2 = scanner.peek();
        Assert.assertEquals("seek error! ", true, result2.isEmpty());
    }


    @Test
    public void testPeekAndNext(){//scanner2
        PFileReader.PFileScanner scanner = (PFileReader.PFileScanner) scanners.get(2);
        int total = 150;
        for (int i = 101; i <= total; ++i){
            List<Cell> result = scanner.peek();

            Assert.assertEquals(false, result.isEmpty());
            Assert.assertEquals(String.format("%10d", i), Bytes.toString(result.get(0).getRow()));

            Assert.assertEquals("should has next", true, scanner.hasNext());

            List<Cell> result1 = scanner.next();
            Assert.assertEquals("should not be false", false, result1.isEmpty());

            Assert.assertEquals(String.format("%10d", i), Bytes.toString(result.get(0).getRow()));

        }

    }



    @Test
    public void testGetRecordCount(){//scanner1

        PFileReader.PFileScanner scanner = (PFileReader.PFileScanner)scanners.get(1);
        for(int i = 50; i >= 1; --i){
            //System.out.println("record left count : "+ scanner.getMaxResultsCount());
            Assert.assertEquals("lasting count should be equal to" + i, i, scanner.getMaxResultsCount());
            scanner.next();
        }

    }

}
