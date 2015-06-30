package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.pfile.PFile;
import org.apache.hadoop.hbase.io.pfile.PFileReader;
import org.apache.hadoop.hbase.io.pfile.PFileWriter;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import parquet.example.data.Group;
import parquet.example.data.GroupFactory;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetFileInfo;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wangxiaoyi on 15/5/26.
 *
 * provide the writer and reader for a parquet file
 *
 */
public class PStoreFile {

    private static final Log LOG = LogFactory.getLog(PStoreFile.class);
    private static final String START_KEY = "startkey";
    private static final String END_KEY = "endkey";

    private ParquetFileInfo fileInfo;
    private final FileSystem fs;

    private Path filePath;
    private Configuration conf;

    private volatile PFileReader reader;



    public PStoreFile(FileSystem fs, Path filePath, Configuration conf){
        this.fs = fs;
        this.filePath = filePath;
        this.conf = conf;
    }

    /**
     * used for the first time
     * PStoreFile Load into memory
     * @return whether store file init success
     */
    protected boolean initStoreFile() throws IOException{
        if(! fs.exists(filePath)){
            LOG.error(filePath + "not exists !");
            return false;
        }
        reader = createReader();
        if(reader == null){
            LOG.error("init Store File error!");
            return false;
        }
        fileInfo = reader.getFileInfo();
        reader.close();
        return true;
    }

    public MessageType getSchema(){
        return fileInfo.getFileSchema();
    }

    /**
     * create a reader for a parquet file
     * @param schema
     * @return
     */
    public PFileReader createReader(String schema){
        return createReader(MessageTypeParser.parseMessageType(schema));
    }

    public PFileReader createReader(MessageType schema){
        try {
            return new PFileReader(filePath, conf, schema);
        }catch (IOException ioe){
            LOG.error(ioe.getMessage());
            return null;
        }
    }

    public PFileReader createReader(){
        try {
            return new PFileReader(filePath, conf, null);
        }catch (IOException ioe){
            LOG.error(ioe.getMessage());
            return null;
        }
    }

    /**
     * @return start key of parquet
     */
    public String getStartKey(){
        return fileInfo.getMetaData(START_KEY);
    }

    /**
     * @return end key of parquet
     */

    public String getEndKey(){
        return fileInfo.getMetaData(END_KEY);
    }


    public Path getPath(){
        if(filePath == null)
            filePath = fileInfo.getFilePath();
        return filePath ;
    }



    /**
     * reader for parquet
     */

   public  class Reader implements Closeable{

        private PFileReader reader = null;


        public Reader(String schema){
            reader = createReader(schema);
        }


        public ParquetFileInfo getFileInfo(){
            fileInfo = reader.getFileInfo();
            return fileInfo;
        }

        public PFileReader getReader(){
            return reader;
        }

        public PFileReader.PFileScanner getScanner(){
            return reader.getScanner();
        }


        /**
         * @return file record length
         */
        public int getLength(){
            return 0;
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



    /**
     * to build writer
     */
    public static class WriterBuilder {
        private final Configuration conf;
        //private final CacheConfig cacheConf;
        private final FileSystem fs;


        private MessageType schema;
        private Path dir;
        private Path filePath;
        private Map<String, String> meta;
        private InetSocketAddress[] favoredNodes;




        public WriterBuilder(Configuration conf,
                               FileSystem fs, MessageType schema) {
            this.conf = conf;
            //this.cacheConf = cacheConf;
            this.fs = fs;
            this.schema = schema;
        }

        public WriterBuilder(Configuration conf,
                             FileSystem fs, MessageType schema, Path path) {
            this.conf = conf;
            //this.cacheConf = cacheConf;
            this.fs = fs;
            this.schema = schema;
            this.filePath = path;
        }

        public WriterBuilder addSchema(MessageType schema){
            this.schema = schema;
            return this;
        }



        /**
         * @param favoredNodes an array of favored nodes or possibly null
         * @return this (for chained invocation)
         */
        public WriterBuilder addFavoredNodes(InetSocketAddress[] favoredNodes) {
            this.favoredNodes = favoredNodes;
            return this;
        }

        public WriterBuilder addMetaData(Map<String, String> meta){
            this.meta = meta;
            return this;
        }


        /**
         * Create a store file writer. Client is responsible for closing file when
         * done. If metadata, add BEFORE closing using
         * {@link org.apache.hadoop.hbase.regionserver.PStoreFile.Writer}.
         */
        public Writer build() throws IOException {
            if ((dir == null ? 0 : 1) + (filePath == null ? 0 : 1) != 1) {
                throw new IllegalArgumentException("Either specify parent directory " +
                        "or file path");
            }

            if (dir == null) {
                dir = filePath.getParent();
            }

            if (!fs.exists(dir)) {
                fs.mkdirs(dir);
            }

            return new Writer(conf, filePath, schema, meta);
        }


    }

    /**
     * Writer in Store for Parquet File
     */

    public static class Writer implements Compactor.CellSink {


        protected PFile.Writer writer;

        private MessageType schema ;

        private GroupFactory gf;

        private Path filePath;



        private Writer(Configuration conf, Path file, MessageType schema, Map<String, String> meta){
            this.filePath = file;
            this.schema = schema;
            gf = new SimpleGroupFactory(schema);
            GroupWriteSupport.setSchema(schema, conf);
            writer = new PFileWriter(conf, schema, new GroupWriteSupport(meta))
                    .addPath(file)
                    .build();
        }


        public void append(Mutation m){

            writer.append(m.asGroup(gf));
        }

        public void append(Group group){
            writer.append(group);
        }



        @Override
        public void append(Cell cell) throws IOException {
            //do nothing
        }

        public void close() throws IOException{
            this.writer.close();
        }


        public Path getFilePath(){
            return  this.filePath;
        }
    }


    public static void main(String [] args)throws IOException{
        //PStoreFile storeFile = new PStoreFile(new HFileSystem(new Configuration(,)));


        Configuration configuration = new Configuration();

        Path path = new Path("hdfs://10.214.208.11:9000/parquet/wangxiaoyi3.parquet");
        FileSystem fs = FileSystem.get(path.toUri(), configuration);


        PStoreFile storeFile = new PStoreFile(fs, path, configuration);
        //storeFile.initStoreFile();

        //System.out.print("startkey " + storeFile.getStartKey());
        //System.out.print("endykey " + storeFile.getEndKey());



        String schema_str = "  message people { " +
                "          required binary name; " +
                "           required int32 age; " +
                " }";

        Map<String, String> meta = new HashMap<>();

        meta.put(START_KEY, "wangxiaoyi1");
        meta.put(END_KEY, "wangxiaoyi99999");

        MessageType schema = MessageTypeParser.parseMessageType(schema_str);



     /*   //write data

        Writer writer = new WriterBuilder(
                configuration,
                fs,
                schema,
                path)
                .addMetaData(meta)
                .build();

        SimpleGroupFactory sgf = new SimpleGroupFactory(schema);

        for(int i = 1; i < 10000000; ++ i){
            //2 10 万
            //3 100 万
            //4 1000 万

            Group group = sgf.newGroup()
                             .append("name", "wangxiaoyi" + i)
                             .append("age", i);
            writer.append(group);
        }
        writer.close();
*/


        long start = System.currentTimeMillis();

        //read with schema
        PFileReader reader = storeFile.createReader(schema_str);

        PFileReader.PFileScanner scanner = reader.getScanner();
        while (scanner.hasNext()){
            Group group = scanner.nextRow();
            //System.out.print(new String(group.getBinary("name", 0).getBytes()) + "\t");
            //System.out.println(group.getInteger("age", 0));
        }

        long end = System.currentTimeMillis();
        System.out.println("total time : "+ (end - start));
        //fs.delete(path, true);


    }

}
