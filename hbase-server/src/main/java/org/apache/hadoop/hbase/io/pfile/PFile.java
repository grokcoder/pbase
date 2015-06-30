package org.apache.hadoop.hbase.io.pfile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import parquet.example.data.Group;

import java.io.IOException;
import java.util.List;

/**
 * Created by wangxiaoyi on 15/4/24.
 *
 * file in disk to store table data
 */
public class PFile {

    private static final Log LOG = LogFactory.getLog(PFile.class);


    public Writer createWriter(){
        Writer writer = null;

        return writer;
    }


    public Reader createReader(){
        Reader reader = null;
        return reader;
    }


    /**
     * store parquet file info
     */
    public class FileInfo{

    }

    /**
     * parquet file writer api
     */
    public interface Writer extends Cloneable{

        /** @return the path to this {@link PFile} */
        Path getPath();

        public void append(Group group);

        /**
         * close the writer
         */
        public void close() throws IOException;
    }

    /**
     * parquet file reader api
     */
    public interface Reader extends Cloneable{

        /**
         * close the reader
         */
        public void close();

        /**
         * read a row
         */
        public Group readGroup();


        /**
         * read value from parquet as cell
         * @return
         */
        public List<Cell> readCells();

    }




}
