package main.server.regionserver;

import main.parquet.ParquetReadUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.RecordScanner;
import org.apache.hadoop.hbase.regionserver.ScannerHeap;
import org.apache.hadoop.hbase.regionserver.memstore.PMemStore;
import org.apache.hadoop.hbase.regionserver.memstore.PMemStoreImpl;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;


/**
 * Created by wangxiaoyi on 15/6/11.
 */
public class TestScannerHeap {

    public static Path rootPath = new Path("hdfs://10.214.208.11:9000/parquet/");

    /**
     * load scanners form parquet
     * @return
     */
    public static List<RecordScanner> getScannerFromParquet(){

        List<Path> paths = ParquetReadUtil.loadParquetFiles(new Path(rootPath, "hbase"));

        List<RecordScanner> scanners = ParquetReadUtil.getParquetFileScanner(paths, null);

        return scanners;
    }


    /**
     * load scanners from memstore
     * @return
     */
    public static RecordScanner getScannerFromMem(){
        PMemStore memStore = new PMemStoreImpl(HBaseConfiguration.create());
        final int ROWS_LEN = 150;

        for(int i = 0 ; i < 150; ){
            i += 2;
            Put put = new Put(Bytes.toBytes("r" + i));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("wangxiaoyi_in_mem" + i));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes("24_in_mem"));
            try {
                memStore.add(put);
            }catch (IOException ioe){
                ioe.printStackTrace();
            }
        }
        return memStore.getScanner();

    }

    public static void main(String []args) throws IOException{

        List<RecordScanner> scanners = new LinkedList<>();
        scanners.addAll(getScannerFromParquet());

        scanners.add(getScannerFromMem());

        ScannerHeap heap = new ScannerHeap(scanners);

        while (heap.hasNext()){
            List<Cell> cells  = heap.next();
            for(Cell cell : cells){
                System.out.println(Bytes.toString(cell.getRow()) + " " + Bytes.toString(cell.getFamily())
                        + " " + Bytes.toString(cell.getQualifier()) + " " + Bytes.toString(cell.getValue())
                        + " " + cell.getTimestamp());
            }
        }

        heap.close();



    }

}
