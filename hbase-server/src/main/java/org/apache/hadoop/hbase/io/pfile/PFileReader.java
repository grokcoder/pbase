package org.apache.hadoop.hbase.io.pfile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.InternalRecordScanner;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RecordScanner;
import org.apache.hadoop.hbase.regionserver.RowScanner;
import org.apache.hadoop.hbase.util.Bytes;
import parquet.column.ColumnDescriptor;
import parquet.column.ParquetProperties;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetFileInfo;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.FileMetaData;
import parquet.io.api.Binary;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.Type;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by wangxiaoyi on 15/4/24.
 *
 * parquet file reader
 *
 */
public class PFileReader implements PFile.Reader{

    private static final Log LOG = LogFactory.getLog(PFileReader.class);


    private static final String ROW_KEY = "rowkey";



    private Path path = null;
    private Configuration conf = null;
    private MessageType schema = null;
    private ParquetReader<Group> reader = null;


    /**
     * TODO: Check whether the params are valid
     * @param fileToRead
     * @param conf
     * @param schema
     */
    public PFileReader(Path fileToRead, Configuration conf, MessageType schema)throws IOException{
        this.path = fileToRead;
        this.conf = conf;
        this.schema = schema;
        if(schema != null)
            conf.set(ReadSupport.PARQUET_READ_SCHEMA, schema.toString());
        initReader();
    }

    public void initReader()throws IOException{
        reader = ParquetReader
                    .builder(new GroupReadSupport(),path)
                    .withConf(conf)
                    .build();
    }

    /**
     * read a row
     */
    @Override
    public Group readGroup() {
        Group group = null;
        try {
            group = reader.read();
        }catch (IOException ioe){
         LOG.error(ioe);
        }
        return group;
    }

    /**
     * read value from parquet as cell
     *
     * @return
     */
    @Override
    public List<Cell> readCells() {
        List<Cell> cells = new LinkedList<>();
        Group group = readGroup();
        if(group == null)
            return null;
        else {
            List<ColumnDescriptor> columns = schema.getColumns();
            for (ColumnDescriptor column : columns){

            }
            return cells;
        }
    }

    /**
     * close the reader
     */
    @Override
    public void close() {
        try {
            if (reader != null) {
                reader.close();
            }
        }catch (IOException ioe){
            LOG.error(ioe);
        }
    }

    public ParquetFileInfo getFileInfo(){

        return reader.getFileInfo();
    }


    /**
     * get parquet scanner
     * @return {@link org.apache.hadoop.hbase.io.pfile.PFileReader.PFileScanner}
     */
    public PFileScanner getScanner(){
        return new PFileScanner(this);
    }

    /**
     * @return start key of the parquet file
     */
    public byte[] getStartKey(){
        return getFileInfo().getMetaData(HConstants.START_KEY).getBytes();
    }

    /**
     * @return end key of the parquet file
     */
    public byte[] getEndKey(){
        return getFileInfo().getMetaData(HConstants.END_KEY).getBytes();
    }

    /**
     *
     * @return max result left to read
     */
    public long getMaxResultLeft(){
        if(reader != null)
            return  reader.getTotalCountLeft();
        else
            return -1l;
    }


    /**
     *
     * @return total record of this file
     */
    public long getRecordCount(){
        return reader.getTotal();
    }


    /**
     * small tests
     * @param args
     */
    public static void main(String []args) throws IOException{

        Path root = new Path("hdfs://10.214.208.11:9000/parquet/");//文件夹路径

        Configuration configuration = new Configuration();

        MessageType schema = MessageTypeParser.parseMessageType( //parquet文件模式

                " message people { " +

                        "required binary rowkey;" +
                        "required binary cf:name;" +
                        "required binary cf:age;" +
                        "required int64 timestamp;"+
                        " }");

        GroupWriteSupport.setSchema(schema, configuration);

        SimpleGroupFactory sfg = new SimpleGroupFactory(schema);
        Path file = new Path(root, "hbase_people3.snappy.parquet");

        Map<String, String> meta = new HashMap<String, String>();
        meta.put("startkey", "1");
        meta.put("endkey", "2");


/*
        ParquetWriter<Group> writer = new ParquetWriter<Group>(
                file,
                new GroupWriteSupport(meta),
                CompressionCodecName.UNCOMPRESSED,
                1024,
                1024,
                512,
                true,
                false,
                ParquetProperties.WriterVersion.PARQUET_1_0,
                configuration);

        for(int i = 0 ; i< 1000000; ++i){
            Group group = sfg.newGroup().append("rowkey", Binary.fromString("r" + i))
                    .append("cf:name", "wangxiaoyi")
                    .append("cf:age", "24")
                    .append("timestamp", System.currentTimeMillis());
            writer.write(group);
        }

        writer.close();
*/




        PFileReader reader = new PFileReader(
                file,
                new Configuration(),
                schema);

        ParquetFileInfo fileInfo = reader.getFileInfo();
        FileMetaData metaData = fileInfo.getFileMetaData();
        System.out.println(metaData.toString());

        InternalRecordScanner scanner = reader.getScanner();
        System.out.print("record count : " + scanner.getRecordCount());
        while (scanner.hasNext()){
            List<Cell> cells = scanner.next();
            for(Cell cell : cells){
                System.out.print(Bytes.toString(cell.getRow()) + ": " + Bytes.toString(cell.getFamily())
                + " : " + Bytes.toString(cell.getQualifier()) + " " + Bytes.toString(cell.getValue())
                + " " + cell.getTimestamp());
                System.out.println();
            }
            System.out.println("rest result count : " + scanner.getMaxResultsCount());

        }
        scanner.close();




    }


    /**
     * transform data in group into cells(List<cell> - > {@link org.apache.hadoop.hbase.client.Result}</>)
     * @param group
     * @return
     */
    public static List<Cell> groupToCells(Group group){

        List<Cell> cells = null;
        if(group != null){
            cells = new LinkedList<>();
            GroupType groupType = group.getType();
            List<Type> types = groupType.getFields();
            byte [] rowKey = group.getBinary(HConstants.ROW_KEY, 0).getBytes();

            long timestamp = group.getLong(HConstants.TIME_STAMP, 0);

            for(Type t : types){
                if(! t.getName().equals(HConstants.ROW_KEY) && ! t.getName().equals(HConstants.TIME_STAMP)){
                    String name = t.getName();
                    String [] names = name.split(":");
                    if(names.length == 2) {
                        byte[] value = group.getBinary(name, 0).getBytes();
                        Cell cell = new KeyValue(rowKey, names[0].getBytes(), names[1].getBytes(), timestamp, value);
                        cells.add(cell);
                    }
                }
            }
        }
        return cells;
    }



    /**
     * scanner for a parquet file
     */
    public class PFileScanner implements InternalRecordScanner{

        private Group curr = null;
        private Group next = null;

        PFileReader reader = null;

        public PFileScanner(PFileReader reader){
            this.reader = reader;
            seek(null);
        }

        /**
         * seek the query row
         *
         * @param rowkey
         */
        public void seek(byte[] rowkey) {

            if(rowkey != null) {
                Group group = curr == null ? reader.readGroup() : curr;
                while (group != null) {
                    byte[] key = group.getBinary(ROW_KEY, 0).getBytes();
                    if (Bytes.compareTo(key, rowkey) < 0) {
                        curr = group;
                        next = reader.readGroup();
                        group = next;
                    }else {
                        break;
                    }
                }
            }else {
                curr = reader.readGroup();
                if(curr != null){
                    next = readGroup();
                }
            }
        }

        public byte[] getStartKey(){
            return reader.getStartKey();
        }

        public byte[] getEndKey(){
            return reader.getEndKey();
        }

        /**
         * has next row
         *
         * @return
         */
        public boolean hasNext() {
            return curr == null ? false : true;
        }

        /**
         * @return the next {@link Group}
         */
        public Group nextRow() {
            Group rs = curr;
            curr = next;
            next = reader.readGroup();
            return rs;
        }



        /**
         * return the record
         */
        @Override
        public List<Cell> next() {
            List<Cell> record = new LinkedList<>();
            Group group = nextRow();
            if(group != null){
                record.addAll(groupToCells(group));
            }
            return record;
        }

        /**
         * @return total records' count of this scanner
         */
        @Override
        public long getRecordCount() {
            return reader.getRecordCount();
        }

        /**
         * @return max result count left of this scanner
         */
        @Override
        public long getMaxResultsCount() {
            return reader.getMaxResultLeft();
        }

        /**
         * don't iterate just
         *
         * @return first element of the scanner
         */
        @Override
        public List<Cell> peek() {
            Group group = curr;
            return groupToCells(group);
        }

        /**
         * Closes this stream and releases any system resources associated
         * with it. If the stream is already closed then invoking this
         * method has no effect.
         *
         * @throws IOException if an I/O error occurs
         */
        @Override
        public void close() throws IOException {
            reader.close();
        }
    }






}
