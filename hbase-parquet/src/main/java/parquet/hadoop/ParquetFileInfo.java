package parquet.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.schema.MessageType;

import javax.print.DocFlavor;
import java.util.List;

/**
 * Created by wangxiaoyi on 15/5/25.
 *
 * contains the basic file info
 */
public class ParquetFileInfo {
    private Path filePath;
    private List<Footer> footers;
    private List<FileStatus> fileStatuses;
    private List<BlockMetaData> blockMetaDatas;
    private MessageType fileSchema;
    private List<BlockMetaData> filteredBlocks;


    public ParquetFileInfo(Path filePath){
        this.filePath = filePath;
    }


    public void setFooters(List<Footer> footers){
        this.footers = footers;
    }

    public void setFileStatuses(List<FileStatus> statuses){
        this.fileStatuses = statuses;
    }

    public void setBlockMetaDatas(List<BlockMetaData> blockMetaDatas){
        this.blockMetaDatas = blockMetaDatas;
    }

    public void setFileSchema(MessageType schema){
        this.fileSchema = schema;
    }

    public void setFilteredBlocks(List<BlockMetaData> filteredBlocks){
        this.filteredBlocks = filteredBlocks;
    }

    /**
     * find the path fileMetaData
     * @return
     */
    public FileMetaData getFileMetaData(){
        if(footers != null && footers.size() > 0){
            return footers.get(0).getParquetMetadata().getFileMetaData();
        }else
            return null;
    }

    /**
     * @return the file path related to the specific file
     */
    public Path getFilePath(){
        return this.filePath;
    }


    /**
     * get the value of key in {@link FileMetaData}
     * @param key
     * @return
     */
    public String getMetaData(String key){
        return this.getFileMetaData().getKeyValueMetaData().get(key);
    }

    public MessageType getFileSchema(){
        return this.fileSchema;
    }

}
