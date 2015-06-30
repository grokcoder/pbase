package main.parquet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by wangxiaoyi on 15/6/10.
 *
 * manage the files on hdfs
 */
public class HDFSUtil {


    public static final Log LOG = LogFactory.getLog(HDFSUtil.class);

    public static Configuration configuration = new Configuration();

    public static Path rootPath = new Path("hdfs://10.214.208.11:9000/parquet/");

    public static FileSystem fs = null;


    /**
     * load File System
     */
    public static FileSystem loadFileSystem(Path path){
        try {
            fs = FileSystem.get(path.toUri(), configuration);
            return fs;
        }catch (IOException ioe){
            LOG.error(ioe.getMessage());
        }
        return null;
    }






    public static void main(String []args)throws IOException{
        loadFileSystem(rootPath);

        fs.mkdirs(new Path(rootPath, "hbase"));

        //fs.delete(new Path(rootPath, "hbase"), true);
        //deleteFile(new Path(rootPath, ""));
        System.out.print("done\n");
    }


}
