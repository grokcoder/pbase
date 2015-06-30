#源码修改记录

1.parquet 依赖的schema信息

在建表的时候传入到hbase表的 desc文件中，最终序列化到meta表和.desc中
CreateTableHandler 中有修改

2.Put 操作修改（对meta表的数据修改不变，对用户表put自定义）

在client端put操作时添加atrribute信息 ，服务器端通过读取atrribute信息，执行自定义代码

3.Flush机制的修改（采用bottom-up 设计）flush是基于 mutation 的
(1).增加的文件：PFile PFileReader PFileWriter ParquetStoreFlusher(flushSnapShot)
(2).相关类中添加的代码：
     HStore: StoreFlusherImplV2, flushToParquet()
     StoreFile(ParquetStoreFile ?? ):WriterV2 WriterBuilderV2

/**
5.8
*/


4.MemStore机制的修改
(1).增加的文件：PMemStore PMemStoreImpl PMemStoreSnapshot

5.Mutation
(1)增加 asGroup()，将mutation转换为 Parquet可读写的group格式

6.Put
(1)增加 mergePut(), 合并两个rowkey 相同的put


5.12 重构部分代码, 完成PMemStore的snapshot功能
(1)增加文件 ：RowScanner

HStore:
1)增加pMemstore（PMemstore） 变量

StoreFlusher
1)增加 public abstract List<Path> flushSnapshot(PMemStoreSnapshot snapshot, long cacheFlushSeqNum,
                                             MonitoredTask status) throws IOException;
                                             
5.26
1.增加文件PStoreFile 提供对parquet文件的读写

5.27 
1.增加文件ColumnScannerHeap 存储StoreScanner和MemScanner
                                             
                                             
## TODO List

1.load the parquet file reader into memory after the HBase started.
HRegionFileSystem line(208) <done>

2.每次数据插入之后是否update了对应的HRegion的startkey 和 end key <done>

3.memstore 中的readpoint到底是干什么用的（HBase的多版本一致性是怎么实现的）

4.Bloom filter 是个什么技术

5.bulkload 是个什么东西/lazy seek / pre read / 

6.parquet.example 修改个名字

7.parquet 写入失败时 需要删除对应的文件 暂时的parquet版本还不支持

8.scan 的getScanner需要对scan的schema进行验证(是否是数据库表的schema子集)


rowkey 应该设置为等长

