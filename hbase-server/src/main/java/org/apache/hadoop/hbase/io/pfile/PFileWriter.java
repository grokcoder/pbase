package org.apache.hadoop.hbase.io.pfile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.column.ParquetProperties;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by wangxiaoyi on 15/4/24.
 *
 * parquet file writer
 */
public class PFileWriter implements PFile.Writer{

    private static final Log LOG = LogFactory.getLog(PFileWriter.class);

    private Configuration conf = null;
    private MessageType schema = null;


    private ParquetWriter<Group> parquetWriter = null;
    private GroupWriteSupport gws = null;
    private SimpleGroupFactory sfg = null;
    private Path file = null;


    public PFileWriter(Configuration conf, MessageType schema, GroupWriteSupport gws){
        this.conf = conf;
        this.schema = schema;
        this.gws = gws;
    }


    public PFileWriter addConfiguration(Configuration conf){
        this.conf = conf;
        return this;
    }



    public PFileWriter addMessageType(MessageType schema){
        this.schema = schema;
        return this;
    }

    public PFileWriter addGWS(GroupWriteSupport gws){
        this.gws = gws;
        return this;
    }

    public PFileWriter addMetaData(Map<String, String> metas){
        if(this.gws == null){
            this.gws = new GroupWriteSupport(metas);
        }else {
            this.gws.addMeta(metas);
        }
        return this;
    }

    public PFileWriter addPath(Path file){
        this.file = file;
        return this;
    }

    public PFileWriter build(){
        try {
            this.parquetWriter = new ParquetWriter<Group>(
                    file,
                    gws,
                    CompressionCodecName.SNAPPY,
                    1024,
                    1024,
                    512,
                    true,
                    false,
                    ParquetProperties.WriterVersion.PARQUET_1_0,
                    conf);
        }catch (IOException ioe){
            LOG.error(ioe.toString());
        }
        return this;
    }

    /**
     * close the writer
     */
    @Override
    public void close() throws IOException {
        this.parquetWriter.close();
    }

    public void append(Group group){
        try {
            parquetWriter.write(group);
        }catch (IOException ioe){
            LOG.error(ioe.toString());
        }
    }


    public void append(List<Group> groups){
        try {
            for (Group group : groups) {
                parquetWriter.write(group);
            }
        }catch (IOException ioe){
            LOG.error(ioe.toString());
        }
    }


    /**
     * @return the path to this {@link PFile}
     */
    @Override
    public Path getPath() {
        return this.file;
    }
}
