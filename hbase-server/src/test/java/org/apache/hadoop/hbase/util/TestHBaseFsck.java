/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.assertErrors;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.assertNoErrors;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.doFsck;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.hfile.TestHFile;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.SplitTransaction;
import org.apache.hadoop.hbase.regionserver.TestEndToEndSplitTransaction;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;
import org.apache.hadoop.hbase.util.HBaseFsck.HbckInfo;
import org.apache.hadoop.hbase.util.HBaseFsck.PrintingErrorReporter;
import org.apache.hadoop.hbase.util.HBaseFsck.TableInfo;
import org.apache.hadoop.hbase.util.hbck.HFileCorruptionChecker;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.google.common.collect.Multimap;

/**
 * This tests HBaseFsck's ability to detect reasons for inconsistent tables.
 */
@Category(LargeTests.class)
public class TestHBaseFsck {
  static final int POOL_SIZE = 7;

  final static Log LOG = LogFactory.getLog(TestHBaseFsck.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static Configuration conf = TEST_UTIL.getConfiguration();
  private final static String FAM_STR = "fam";
  private final static byte[] FAM = Bytes.toBytes(FAM_STR);
  private final static int REGION_ONLINE_TIMEOUT = 800;
  private static RegionStates regionStates;
  private static ExecutorService tableExecutorService;
  private static ScheduledThreadPoolExecutor hbfsckExecutorService;
  private static ClusterConnection connection;
  private static Admin admin;

  // for the instance, reset every test run
  private HTable tbl;
  private final static byte[][] SPLITS = new byte[][] { Bytes.toBytes("A"),
    Bytes.toBytes("B"), Bytes.toBytes("C") };
  // one row per region.
  private final static byte[][] ROWKEYS= new byte[][] {
    Bytes.toBytes("00"), Bytes.toBytes("50"), Bytes.toBytes("A0"), Bytes.toBytes("A5"),
    Bytes.toBytes("B0"), Bytes.toBytes("B5"), Bytes.toBytes("C0"), Bytes.toBytes("C5") };

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf.setInt("hbase.regionserver.handler.count", 2);
    conf.setInt("hbase.regionserver.metahandler.count", 2);

    conf.setInt("hbase.htable.threads.max", POOL_SIZE);
    conf.setInt("hbase.hconnection.threads.max", 2 * POOL_SIZE);
    conf.setInt("hbase.hconnection.threads.core", POOL_SIZE);
    conf.setInt("hbase.hbck.close.timeout", 2 * REGION_ONLINE_TIMEOUT);
    TEST_UTIL.startMiniCluster(3);

    tableExecutorService = new ThreadPoolExecutor(1, POOL_SIZE, 60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), Threads.newDaemonThreadFactory("testhbck"));

    hbfsckExecutorService = new ScheduledThreadPoolExecutor(POOL_SIZE);

    AssignmentManager assignmentManager =
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    regionStates = assignmentManager.getRegionStates();

    connection = (ClusterConnection) TEST_UTIL.getConnection();

    admin = connection.getAdmin();
    admin.setBalancerRunning(false, true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    tableExecutorService.shutdown();
    hbfsckExecutorService.shutdown();
    admin.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test (timeout=180000)
  public void testHBaseFsck() throws Exception {
    assertNoErrors(doFsck(conf, false));
    TableName table = TableName.valueOf("tableBadMetaAssign");
    TEST_UTIL.createTable(table, FAM);

    // We created 1 table, should be fine
    assertNoErrors(doFsck(conf, false));

    // Now let's mess it up and change the assignment in hbase:meta to
    // point to a different region server
    Table meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(table+",,"));
    ResultScanner scanner = meta.getScanner(scan);
    HRegionInfo hri = null;

    Result res = scanner.next();
    ServerName currServer =
      ServerName.parseFrom(res.getValue(HConstants.CATALOG_FAMILY,
          HConstants.SERVER_QUALIFIER));
    long startCode = Bytes.toLong(res.getValue(HConstants.CATALOG_FAMILY,
        HConstants.STARTCODE_QUALIFIER));

    for (JVMClusterUtil.RegionServerThread rs :
        TEST_UTIL.getHBaseCluster().getRegionServerThreads()) {

      ServerName sn = rs.getRegionServer().getServerName();

      // When we find a diff RS, change the assignment and break
      if (!currServer.getHostAndPort().equals(sn.getHostAndPort()) ||
          startCode != sn.getStartcode()) {
        Put put = new Put(res.getRow());
        put.setDurability(Durability.SKIP_WAL);
        put.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
          Bytes.toBytes(sn.getHostAndPort()));
        put.add(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
          Bytes.toBytes(sn.getStartcode()));
        meta.put(put);
        hri = MetaTableAccessor.getHRegionInfo(res);
        break;
      }
    }

    // Try to fix the data
    assertErrors(doFsck(conf, true), new ERROR_CODE[]{
        ERROR_CODE.SERVER_DOES_NOT_MATCH_META});

    TEST_UTIL.getHBaseCluster().getMaster()
      .getAssignmentManager().waitForAssignment(hri);

    // Should be fixed now
    assertNoErrors(doFsck(conf, false));

    // comment needed - what is the purpose of this line
    Table t = connection.getTable(table, tableExecutorService);
    ResultScanner s = t.getScanner(new Scan());
    s.close();
    t.close();

    scanner.close();
    meta.close();
  }

  @Test(timeout=180000)
  public void testFixAssignmentsWhenMETAinTransition() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    admin.closeRegion(cluster.getServerHoldingMeta(), HRegionInfo.FIRST_META_REGIONINFO);
    regionStates.regionOffline(HRegionInfo.FIRST_META_REGIONINFO);
    new MetaTableLocator().deleteMetaLocation(cluster.getMaster().getZooKeeper());
    assertFalse(regionStates.isRegionOnline(HRegionInfo.FIRST_META_REGIONINFO));
    HBaseFsck hbck = doFsck(conf, true);
    assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.UNKNOWN, ERROR_CODE.NO_META_REGION,
        ERROR_CODE.NULL_META_REGION });
    assertNoErrors(doFsck(conf, false));
  }

  /**
   * Create a new region in META.
   */
  private HRegionInfo createRegion(final HTableDescriptor
      htd, byte[] startKey, byte[] endKey)
      throws IOException {
    Table meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
    HRegionInfo hri = new HRegionInfo(htd.getTableName(), startKey, endKey);
    MetaTableAccessor.addRegionToMeta(meta, hri);
    meta.close();
    return hri;
  }

  /**
   * Debugging method to dump the contents of meta.
   */
  private void dumpMeta(TableName tableName) throws IOException {
    List<byte[]> metaRows = TEST_UTIL.getMetaTableRows(tableName);
    for (byte[] row : metaRows) {
      LOG.info(Bytes.toString(row));
    }
  }

  /**
   * This method is used to undeploy a region -- close it and attempt to
   * remove its state from the Master.
   */
  private void undeployRegion(Connection conn, ServerName sn,
      HRegionInfo hri) throws IOException, InterruptedException {
    try {
      HBaseFsckRepair.closeRegionSilentlyAndWait((HConnection) conn, sn, hri);
      if (!hri.isMetaTable()) {
        admin.offline(hri.getRegionName());
      }
    } catch (IOException ioe) {
      LOG.warn("Got exception when attempting to offline region "
          + Bytes.toString(hri.getRegionName()), ioe);
    }
  }
  /**
   * Delete a region from assignments, meta, or completely from hdfs.
   * @param unassign if true unassign region if assigned
   * @param metaRow  if true remove region's row from META
   * @param hdfs if true remove region's dir in HDFS
   */
  private void deleteRegion(Configuration conf, final HTableDescriptor htd,
      byte[] startKey, byte[] endKey, boolean unassign, boolean metaRow,
      boolean hdfs) throws IOException, InterruptedException {
    deleteRegion(conf, htd, startKey, endKey, unassign, metaRow, hdfs, false);
  }

  /**
   * Delete a region from assignments, meta, or completely from hdfs.
   * @param unassign if true unassign region if assigned
   * @param metaRow  if true remove region's row from META
   * @param hdfs if true remove region's dir in HDFS
   * @param regionInfoOnly if true remove a region dir's .regioninfo file
   */
  private void deleteRegion(Configuration conf, final HTableDescriptor htd,
      byte[] startKey, byte[] endKey, boolean unassign, boolean metaRow,
      boolean hdfs, boolean regionInfoOnly) throws IOException, InterruptedException {
    LOG.info("** Before delete:");
    dumpMeta(htd.getTableName());

    List<HRegionLocation> locations = tbl.getAllRegionLocations();
    for (HRegionLocation location : locations) {
      HRegionInfo hri = location.getRegionInfo();
      ServerName hsa = location.getServerName();
      if (Bytes.compareTo(hri.getStartKey(), startKey) == 0
          && Bytes.compareTo(hri.getEndKey(), endKey) == 0) {

        LOG.info("RegionName: " +hri.getRegionNameAsString());
        byte[] deleteRow = hri.getRegionName();

        if (unassign) {
          LOG.info("Undeploying region " + hri + " from server " + hsa);
          undeployRegion(connection, hsa, hri);
        }

        if (regionInfoOnly) {
          LOG.info("deleting hdfs .regioninfo data: " + hri.toString() + hsa.toString());
          Path rootDir = FSUtils.getRootDir(conf);
          FileSystem fs = rootDir.getFileSystem(conf);
          Path p = new Path(FSUtils.getTableDir(rootDir, htd.getTableName()),
              hri.getEncodedName());
          Path hriPath = new Path(p, HRegionFileSystem.REGION_INFO_FILE);
          fs.delete(hriPath, true);
        }

        if (hdfs) {
          LOG.info("deleting hdfs data: " + hri.toString() + hsa.toString());
          Path rootDir = FSUtils.getRootDir(conf);
          FileSystem fs = rootDir.getFileSystem(conf);
          Path p = new Path(FSUtils.getTableDir(rootDir, htd.getTableName()),
              hri.getEncodedName());
          HBaseFsck.debugLsr(conf, p);
          boolean success = fs.delete(p, true);
          LOG.info("Deleted " + p + " sucessfully? " + success);
          HBaseFsck.debugLsr(conf, p);
        }

        if (metaRow) {
          try (Table meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService)) {
            Delete delete = new Delete(deleteRow);
            meta.delete(delete);
          }
        }
      }
      LOG.info(hri.toString() + hsa.toString());
    }

    TEST_UTIL.getMetaTableRows(htd.getTableName());
    LOG.info("*** After delete:");
    dumpMeta(htd.getTableName());
  }

  /**
   * Setup a clean table before we start mucking with it.
   *
   * It will set tbl which needs to be closed after test
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  void setupTable(TableName tablename) throws Exception {
    setupTableWithRegionReplica(tablename, 1);
  }

  /**
   * Setup a clean table with a certain region_replica count
   *
   * It will set tbl which needs to be closed after test
   *
   * @param tableName
   * @param replicaCount
   * @throws Exception
   */
  void setupTableWithRegionReplica(TableName tablename, int replicaCount) throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tablename);
    desc.setRegionReplication(replicaCount);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toString(FAM));
    desc.addFamily(hcd); // If a table has no CF's it doesn't get checked
    admin.createTable(desc, SPLITS);
    tbl = (HTable) connection.getTable(tablename, tableExecutorService);
    List<Put> puts = new ArrayList<Put>();
    for (byte[] row : ROWKEYS) {
      Put p = new Put(row);
      p.add(FAM, Bytes.toBytes("val"), row);
      puts.add(p);
    }
    tbl.put(puts);
    tbl.flushCommits();
  }

  /**
   * Counts the number of row to verify data loss or non-dataloss.
   */
  int countRows() throws IOException {
     Scan s = new Scan();
     ResultScanner rs = tbl.getScanner(s);
     int i = 0;
     while(rs.next() !=null) {
       i++;
     }
     return i;
  }

  /**
   * delete table in preparation for next test
   *
   * @param tablename
   * @throws IOException
   */
  void cleanupTable(TableName tablename) throws IOException {
    if (tbl != null) {
      tbl.close();
      tbl = null;
    }

    ((ClusterConnection) connection).clearRegionCache();
    TEST_UTIL.deleteTable(tablename);

  }

  /**
   * This creates a clean table and confirms that the table is clean.
   */
  @Test (timeout=180000)
  public void testHBaseFsckClean() throws Exception {
    assertNoErrors(doFsck(conf, false));
    TableName table = TableName.valueOf("tableClean");
    try {
      HBaseFsck hbck = doFsck(conf, false);
      assertNoErrors(hbck);

      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // We created 1 table, should be fine
      hbck = doFsck(conf, false);
      assertNoErrors(hbck);
      assertEquals(0, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * Test thread pooling in the case where there are more regions than threads
   */
  @Test (timeout=180000)
  public void testHbckThreadpooling() throws Exception {
    TableName table =
        TableName.valueOf("tableDupeStartKey");
    try {
      // Create table with 4 regions
      setupTable(table);

      // limit number of threads to 1.
      Configuration newconf = new Configuration(conf);
      newconf.setInt("hbasefsck.numthreads", 1);
      assertNoErrors(doFsck(newconf, false));

      // We should pass without triggering a RejectedExecutionException
    } finally {
      cleanupTable(table);
    }
  }

  @Test (timeout=180000)
  public void testHbckFixOrphanTable() throws Exception {
    TableName table = TableName.valueOf("tableInfo");
    FileSystem fs = null;
    Path tableinfo = null;
    try {
      setupTable(table);

      Path hbaseTableDir = FSUtils.getTableDir(
          FSUtils.getRootDir(conf), table);
      fs = hbaseTableDir.getFileSystem(conf);
      FileStatus status = FSTableDescriptors.getTableInfoPath(fs, hbaseTableDir);
      tableinfo = status.getPath();
      fs.rename(tableinfo, new Path("/.tableinfo"));

      //to report error if .tableinfo is missing.
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.NO_TABLEINFO_FILE });

      // fix OrphanTable with default .tableinfo (htd not yet cached on master)
      hbck = doFsck(conf, true);
      assertNoErrors(hbck);
      status = null;
      status = FSTableDescriptors.getTableInfoPath(fs, hbaseTableDir);
      assertNotNull(status);

      HTableDescriptor htd = admin.getTableDescriptor(table);
      htd.setValue("NOT_DEFAULT", "true");
      admin.disableTable(table);
      admin.modifyTable(table, htd);
      admin.enableTable(table);
      fs.delete(status.getPath(), true);

      // fix OrphanTable with cache
      htd = admin.getTableDescriptor(table); // warms up cached htd on master
      hbck = doFsck(conf, true);
      assertNoErrors(hbck);
      status = FSTableDescriptors.getTableInfoPath(fs, hbaseTableDir);
      assertNotNull(status);
      htd = admin.getTableDescriptor(table);
      assertEquals(htd.getValue("NOT_DEFAULT"), "true");
    } finally {
      fs.rename(new Path("/.tableinfo"), tableinfo);
      cleanupTable(table);
    }
  }

  /**
   * This test makes sure that parallel instances of Hbck is disabled.
   *
   * @throws Exception
   */
  @Test (timeout=180000)
  public void testParallelHbck() throws Exception {
    final ExecutorService service;
    final Future<HBaseFsck> hbck1,hbck2;

    class RunHbck implements Callable<HBaseFsck>{
      boolean fail = true;
      @Override
      public HBaseFsck call(){
        try{
          return doFsck(conf, false);
        } catch(Exception e){
          if (e.getMessage().contains("Duplicate hbck")) {
            fail = false;
          }
        }
        // If we reach here, then an exception was caught
        if (fail) fail();
        return null;
      }
    }
    service = Executors.newFixedThreadPool(2);
    hbck1 = service.submit(new RunHbck());
    hbck2 = service.submit(new RunHbck());
    service.shutdown();
    //wait for 15 seconds, for both hbck calls finish
    service.awaitTermination(15, TimeUnit.SECONDS);
    HBaseFsck h1 = hbck1.get();
    HBaseFsck h2 = hbck2.get();
    // Make sure only one of the calls was successful
    assert(h1 == null || h2 == null);
    if (h1 != null) {
      assert(h1.getRetCode() >= 0);
    }
    if (h2 != null) {
      assert(h2.getRetCode() >= 0);
    }
  }

  /**
   * This create and fixes a bad table with regions that have a duplicate
   * start key
   */
  @Test (timeout=180000)
  public void testDupeStartKey() throws Exception {
    TableName table =
        TableName.valueOf("tableDupeStartKey");
    try {
      setupTable(table);
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());

      // Now let's mess it up, by adding a region with a duplicate startkey
      HRegionInfo hriDupe =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A"), Bytes.toBytes("A2"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriDupe);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriDupe);
      ServerName server = regionStates.getRegionServerOfRegion(hriDupe);
      TEST_UTIL.assertRegionOnServer(hriDupe, server, REGION_ONLINE_TIMEOUT);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.DUPE_STARTKEYS,
            ERROR_CODE.DUPE_STARTKEYS});
      assertEquals(2, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows()); // seems like the "bigger" region won.

      // fix the degenerate region.
      doFsck(conf,true);

      // check that the degenerate region is gone and no data loss
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /*
   * This creates a table with region_replica > 1 and verifies hbck runs
   * successfully
   */
  @Test (timeout=180000)
  public void testHbckWithRegionReplica() throws Exception {
    TableName table =
        TableName.valueOf("tableWithReplica");
    try {
      setupTableWithRegionReplica(table, 2);
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * Get region info from local cluster.
   */
  Map<ServerName, List<String>> getDeployedHRIs(final HBaseAdmin admin) throws IOException {
    ClusterStatus status = admin.getClusterStatus();
    Collection<ServerName> regionServers = status.getServers();
    Map<ServerName, List<String>> mm =
        new HashMap<ServerName, List<String>>();
    for (ServerName hsi : regionServers) {
      AdminProtos.AdminService.BlockingInterface server = ((HConnection) connection).getAdmin(hsi);

      // list all online regions from this region server
      List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(server);
      List<String> regionNames = new ArrayList<String>();
      for (HRegionInfo hri : regions) {
        regionNames.add(hri.getRegionNameAsString());
      }
      mm.put(hsi, regionNames);
    }
    return mm;
  }

  /**
   * Returns the HSI a region info is on.
   */
  ServerName findDeployedHSI(Map<ServerName, List<String>> mm, HRegionInfo hri) {
    for (Map.Entry<ServerName,List <String>> e : mm.entrySet()) {
      if (e.getValue().contains(hri.getRegionNameAsString())) {
        return e.getKey();
      }
    }
    return null;
  }

  /**
   * This create and fixes a bad table with regions that have a duplicate
   * start key
   */
  @Test (timeout=180000)
  public void testDupeRegion() throws Exception {
    TableName table =
        TableName.valueOf("tableDupeRegion");
    try {
      setupTable(table);
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());

      // Now let's mess it up, by adding a region with a duplicate startkey
      HRegionInfo hriDupe =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A"), Bytes.toBytes("B"));

      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriDupe);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriDupe);
      ServerName server = regionStates.getRegionServerOfRegion(hriDupe);
      TEST_UTIL.assertRegionOnServer(hriDupe, server, REGION_ONLINE_TIMEOUT);

      // Yikes! The assignment manager can't tell between diff between two
      // different regions with the same start/endkeys since it doesn't
      // differentiate on ts/regionId!  We actually need to recheck
      // deployments!
      while (findDeployedHSI(getDeployedHRIs((HBaseAdmin) admin), hriDupe) == null) {
        Thread.sleep(250);
      }

      LOG.debug("Finished assignment of dupe region");

      // TODO why is dupe region different from dupe start keys?
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.DUPE_STARTKEYS,
            ERROR_CODE.DUPE_STARTKEYS});
      assertEquals(2, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows()); // seems like the "bigger" region won.

      // fix the degenerate region.
      doFsck(conf,true);

      // check that the degenerate region is gone and no data loss
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with regions that has startkey == endkey
   */
  @Test (timeout=180000)
  public void testDegenerateRegions() throws Exception {
    TableName table = TableName.valueOf("tableDegenerateRegions");
    try {
      setupTable(table);
      assertNoErrors(doFsck(conf,false));
      assertEquals(ROWKEYS.length, countRows());

      // Now let's mess it up, by adding a region with a duplicate startkey
      HRegionInfo hriDupe =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("B"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriDupe);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriDupe);
      ServerName server = regionStates.getRegionServerOfRegion(hriDupe);
      TEST_UTIL.assertRegionOnServer(hriDupe, server, REGION_ONLINE_TIMEOUT);

      HBaseFsck hbck = doFsck(conf,false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.DEGENERATE_REGION, ERROR_CODE.DUPE_STARTKEYS,
          ERROR_CODE.DUPE_STARTKEYS });
      assertEquals(2, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());

      // fix the degenerate region.
      doFsck(conf,true);

      // check that the degenerate region is gone and no data loss
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table where a region is completely contained
   * by another region.
   */
  @Test (timeout=180000)
  public void testContainedRegionOverlap() throws Exception {
    TableName table =
        TableName.valueOf("tableContainedRegionOverlap");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap in the metadata
      HRegionInfo hriOverlap =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A2"), Bytes.toBytes("B"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriOverlap);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriOverlap);
      ServerName server = regionStates.getRegionServerOfRegion(hriOverlap);
      TEST_UTIL.assertRegionOnServer(hriOverlap, server, REGION_ONLINE_TIMEOUT);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
          ERROR_CODE.OVERLAP_IN_REGION_CHAIN });
      assertEquals(2, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());

      // fix the problem.
      doFsck(conf, true);

      // verify that overlaps are fixed
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table where an overlap group of
   * 3 regions. Set HBaseFsck.maxMerge to 2 to trigger sideline overlapped
   * region. Mess around the meta data so that closeRegion/offlineRegion
   * throws exceptions.
   */
  @Test (timeout=180000)
  public void testSidelineOverlapRegion() throws Exception {
    TableName table =
        TableName.valueOf("testSidelineOverlapRegion");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      HMaster master = cluster.getMaster();
      HRegionInfo hriOverlap1 =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A"), Bytes.toBytes("AB"));
      master.assignRegion(hriOverlap1);
      master.getAssignmentManager().waitForAssignment(hriOverlap1);
      HRegionInfo hriOverlap2 =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("AB"), Bytes.toBytes("B"));
      master.assignRegion(hriOverlap2);
      master.getAssignmentManager().waitForAssignment(hriOverlap2);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {ERROR_CODE.DUPE_STARTKEYS,
        ERROR_CODE.DUPE_STARTKEYS, ERROR_CODE.OVERLAP_IN_REGION_CHAIN});
      assertEquals(3, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());

      // mess around the overlapped regions, to trigger NotServingRegionException
      Multimap<byte[], HbckInfo> overlapGroups = hbck.getOverlapGroups(table);
      ServerName serverName = null;
      byte[] regionName = null;
      for (HbckInfo hbi: overlapGroups.values()) {
        if ("A".equals(Bytes.toString(hbi.getStartKey()))
            && "B".equals(Bytes.toString(hbi.getEndKey()))) {
          regionName = hbi.getRegionName();

          // get an RS not serving the region to force bad assignment info in to META.
          int k = cluster.getServerWith(regionName);
          for (int i = 0; i < 3; i++) {
            if (i != k) {
              HRegionServer rs = cluster.getRegionServer(i);
              serverName = rs.getServerName();
              break;
            }
          }

          HBaseFsckRepair.closeRegionSilentlyAndWait((HConnection) connection,
              cluster.getRegionServer(k).getServerName(), hbi.getHdfsHRI());
          admin.offline(regionName);
          break;
        }
      }

      assertNotNull(regionName);
      assertNotNull(serverName);
      try (Table meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService)) {
        Put put = new Put(regionName);
        put.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
            Bytes.toBytes(serverName.getHostAndPort()));
        meta.put(put);
      }

      // fix the problem.
      HBaseFsck fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setFixAssignments(true);
      fsck.setFixMeta(true);
      fsck.setFixHdfsHoles(true);
      fsck.setFixHdfsOverlaps(true);
      fsck.setFixHdfsOrphans(true);
      fsck.setFixVersionFile(true);
      fsck.setSidelineBigOverlaps(true);
      fsck.setMaxMerge(2);
      fsck.onlineHbck();
      fsck.close();

      // verify that overlaps are fixed, and there are less rows
      // since one region is sidelined.
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertTrue(ROWKEYS.length > countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table where a region is completely contained
   * by another region, and there is a hole (sort of like a bad split)
   */
  @Test (timeout=180000)
  public void testOverlapAndOrphan() throws Exception {
    TableName table =
        TableName.valueOf("tableOverlapAndOrphan");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap in the metadata
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"),
          Bytes.toBytes("B"), true, true, false, true);
      admin.enableTable(table);

      HRegionInfo hriOverlap =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A2"), Bytes.toBytes("B"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriOverlap);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriOverlap);
      ServerName server = regionStates.getRegionServerOfRegion(hriOverlap);
      TEST_UTIL.assertRegionOnServer(hriOverlap, server, REGION_ONLINE_TIMEOUT);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
          ERROR_CODE.ORPHAN_HDFS_REGION, ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // fix the problem.
      doFsck(conf, true);

      // verify that overlaps are fixed
      HBaseFsck hbck2 = doFsck(conf,false);
      assertNoErrors(hbck2);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table where a region overlaps two regions --
   * a start key contained in another region and its end key is contained in
   * yet another region.
   */
  @Test (timeout=180000)
  public void testCoveredStartKey() throws Exception {
    TableName table =
        TableName.valueOf("tableCoveredStartKey");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap in the metadata
      HRegionInfo hriOverlap =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A2"), Bytes.toBytes("B2"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriOverlap);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .waitForAssignment(hriOverlap);
      ServerName server = regionStates.getRegionServerOfRegion(hriOverlap);
      TEST_UTIL.assertRegionOnServer(hriOverlap, server, REGION_ONLINE_TIMEOUT);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.OVERLAP_IN_REGION_CHAIN,
          ERROR_CODE.OVERLAP_IN_REGION_CHAIN });
      assertEquals(3, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());

      // fix the problem.
      doFsck(conf, true);

      // verify that overlaps are fixed
      HBaseFsck hbck2 = doFsck(conf, false);
      assertErrors(hbck2, new ERROR_CODE[0]);
      assertEquals(0, hbck2.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a missing region -- hole in meta
   * and data missing in the fs.
   */
  @Test (timeout=180000)
  public void testRegionHole() throws Exception {
    TableName table =
        TableName.valueOf("tableRegionHole");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
          Bytes.toBytes("C"), true, true, true);
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
          ERROR_CODE.HOLE_IN_REGION_CHAIN});
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf,false));
      assertEquals(ROWKEYS.length - 2 , countRows()); // lost a region so lost a row
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a missing region -- hole in meta
   * and data present but .regioinfino missing (an orphan hdfs region)in the fs.
   */
  @Test (timeout=180000)
  public void testHDFSRegioninfoMissing() throws Exception {
    TableName table = TableName.valueOf("tableHDFSRegioninfoMissing");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the meta data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
          Bytes.toBytes("C"), true, true, false, true);
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
          ERROR_CODE.ORPHAN_HDFS_REGION,
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          ERROR_CODE.HOLE_IN_REGION_CHAIN});
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a region that is missing meta and
   * not assigned to a region server.
   */
  @Test (timeout=180000)
  public void testNotInMetaOrDeployedHole() throws Exception {
    TableName table =
        TableName.valueOf("tableNotInMetaOrDeployedHole");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the meta data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
          Bytes.toBytes("C"), true, true, false); // don't rm from fs
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN});
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      assertErrors(doFsck(conf, true) , new ERROR_CODE[] {
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // check that hole fixed
      assertNoErrors(doFsck(conf,false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates fixes a bad table with a hole in meta.
   */
  @Test (timeout=180000)
  public void testNotInMetaHole() throws Exception {
    TableName table =
        TableName.valueOf("tableNotInMetaHole");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the meta data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
          Bytes.toBytes("C"), false, true, false); // don't rm from fs
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN});
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      assertErrors(doFsck(conf, true) , new ERROR_CODE[] {
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // check that hole fixed
      assertNoErrors(doFsck(conf,false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a region that is in meta but has
   * no deployment or data hdfs
   */
  @Test (timeout=180000)
  public void testNotInHdfs() throws Exception {
    TableName table =
        TableName.valueOf("tableNotInHdfs");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table);

      // Mess it up by leaving a hole in the hdfs data
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
          Bytes.toBytes("C"), false, false, true); // don't rm meta

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {ERROR_CODE.NOT_IN_HDFS});
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf,false));
      assertEquals(ROWKEYS.length - 2, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates entries in hbase:meta with no hdfs data.  This should cleanly
   * remove the table.
   */
  @Test (timeout=180000)
  public void testNoHdfsTable() throws Exception {
    TableName table = TableName.valueOf("NoHdfsTable");
    setupTable(table);
    assertEquals(ROWKEYS.length, countRows());

    // make sure data in regions, if in wal only there is no data loss
    admin.flush(table);

    // Mess it up by deleting hdfs dirs
    deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes(""),
        Bytes.toBytes("A"), false, false, true); // don't rm meta
    deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"),
        Bytes.toBytes("B"), false, false, true); // don't rm meta
    deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
        Bytes.toBytes("C"), false, false, true); // don't rm meta
    deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("C"),
        Bytes.toBytes(""), false, false, true); // don't rm meta

    // also remove the table directory in hdfs
    deleteTableDir(table);

    HBaseFsck hbck = doFsck(conf, false);
    assertErrors(hbck, new ERROR_CODE[] {ERROR_CODE.NOT_IN_HDFS,
        ERROR_CODE.NOT_IN_HDFS, ERROR_CODE.NOT_IN_HDFS,
        ERROR_CODE.NOT_IN_HDFS,});
    // holes are separate from overlap groups
    assertEquals(0, hbck.getOverlapGroups(table).size());

    // fix hole
    doFsck(conf, true); // detect dangling regions and remove those

    // check that hole fixed
    assertNoErrors(doFsck(conf,false));
    assertFalse("Table " + table + " should have been deleted", admin.tableExists(table));
  }

  public void deleteTableDir(TableName table) throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);
    Path p = FSUtils.getTableDir(rootDir, table);
    HBaseFsck.debugLsr(conf, p);
    boolean success = fs.delete(p, true);
    LOG.info("Deleted " + p + " sucessfully? " + success);
  }

  /**
   * when the hbase.version file missing, It is fix the fault.
   */
  @Test (timeout=180000)
  public void testNoVersionFile() throws Exception {
    // delete the hbase.version file
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);
    Path versionFile = new Path(rootDir, HConstants.VERSION_FILE_NAME);
    fs.delete(versionFile, true);

    // test
    HBaseFsck hbck = doFsck(conf, false);
    assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.NO_VERSION_FILE });
    // fix hbase.version missing
    doFsck(conf, true);

    // no version file fixed
    assertNoErrors(doFsck(conf, false));
  }

  /**
   * The region is not deployed when the table is disabled.
   */
  @Test (timeout=180000)
  public void testRegionShouldNotBeDeployed() throws Exception {
    TableName table =
        TableName.valueOf("tableRegionShouldNotBeDeployed");
    try {
      LOG.info("Starting testRegionShouldNotBeDeployed.");
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      assertTrue(cluster.waitForActiveAndReadyMaster());


      byte[][] SPLIT_KEYS = new byte[][] { new byte[0], Bytes.toBytes("aaa"),
          Bytes.toBytes("bbb"), Bytes.toBytes("ccc"), Bytes.toBytes("ddd") };
      HTableDescriptor htdDisabled = new HTableDescriptor(table);
      htdDisabled.addFamily(new HColumnDescriptor(FAM));

      // Write the .tableinfo
      FSTableDescriptors fstd = new FSTableDescriptors(conf);
      fstd.createTableDescriptor(htdDisabled);
      List<HRegionInfo> disabledRegions =
          TEST_UTIL.createMultiRegionsInMeta(conf, htdDisabled, SPLIT_KEYS);

      // Let's just assign everything to first RS
      HRegionServer hrs = cluster.getRegionServer(0);

      // Create region files.
      admin.disableTable(table);
      admin.enableTable(table);

      // Disable the table and close its regions
      admin.disableTable(table);
      HRegionInfo region = disabledRegions.remove(0);
      byte[] regionName = region.getRegionName();

      // The region should not be assigned currently
      assertTrue(cluster.getServerWith(regionName) == -1);

      // Directly open a region on a region server.
      // If going through AM/ZK, the region won't be open.
      // Even it is opened, AM will close it which causes
      // flakiness of this test.
      HRegion r = HRegion.openHRegion(
        region, htdDisabled, hrs.getWAL(region), conf);
      hrs.addToOnlineRegions(r);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.SHOULD_NOT_BE_DEPLOYED });

      // fix this fault
      doFsck(conf, true);

      // check result
      assertNoErrors(doFsck(conf, false));
    } finally {
      admin.enableTable(table);
      cleanupTable(table);
    }
  }

  /**
   * This creates two tables and mess both of them and fix them one by one
   */
  @Test (timeout=180000)
  public void testFixByTable() throws Exception {
    TableName table1 =
        TableName.valueOf("testFixByTable1");
    TableName table2 =
        TableName.valueOf("testFixByTable2");
    try {
      setupTable(table1);
      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table1);
      // Mess them up by leaving a hole in the hdfs data
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
        Bytes.toBytes("C"), false, false, true); // don't rm meta

      setupTable(table2);
      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table2);
      // Mess them up by leaving a hole in the hdfs data
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
        Bytes.toBytes("C"), false, false, true); // don't rm meta

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
        ERROR_CODE.NOT_IN_HDFS, ERROR_CODE.NOT_IN_HDFS});

      // fix hole in table 1
      doFsck(conf, true, table1);
      // check that hole in table 1 fixed
      assertNoErrors(doFsck(conf, false, table1));
      // check that hole in table 2 still there
      assertErrors(doFsck(conf, false, table2),
        new ERROR_CODE[] {ERROR_CODE.NOT_IN_HDFS});

      // fix hole in table 2
      doFsck(conf, true, table2);
      // check that hole in both tables fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length - 2, countRows());
    } finally {
      cleanupTable(table1);
      cleanupTable(table2);
    }
  }
  /**
   * A split parent in meta, in hdfs, and not deployed
   */
  @Test (timeout=180000)
  public void testLingeringSplitParent() throws Exception {
    TableName table =
        TableName.valueOf("testLingeringSplitParent");
    Table meta = null;
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table);
      HRegionLocation location = tbl.getRegionLocation("B");

      // Delete one region from meta, but not hdfs, unassign it.
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"),
        Bytes.toBytes("C"), true, true, false);

      // Create a new meta entry to fake it as a split parent.
      meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
      HRegionInfo hri = location.getRegionInfo();

      HRegionInfo a = new HRegionInfo(tbl.getName(),
        Bytes.toBytes("B"), Bytes.toBytes("BM"));
      HRegionInfo b = new HRegionInfo(tbl.getName(),
        Bytes.toBytes("BM"), Bytes.toBytes("C"));

      hri.setOffline(true);
      hri.setSplit(true);

      MetaTableAccessor.addRegionToMeta(meta, hri, a, b);
      meta.close();
      admin.flush(TableName.META_TABLE_NAME);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
        ERROR_CODE.LINGERING_SPLIT_PARENT, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // regular repair cannot fix lingering split parent
      hbck = doFsck(conf, true);
      assertErrors(hbck, new ERROR_CODE[] {
        ERROR_CODE.LINGERING_SPLIT_PARENT, ERROR_CODE.HOLE_IN_REGION_CHAIN });
      assertFalse(hbck.shouldRerun());
      hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
        ERROR_CODE.LINGERING_SPLIT_PARENT, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // fix lingering split parent
      hbck = new HBaseFsck(conf, hbfsckExecutorService);
      hbck.connect();
      hbck.setDisplayFullReport(); // i.e. -details
      hbck.setTimeLag(0);
      hbck.setFixSplitParents(true);
      hbck.onlineHbck();
      assertTrue(hbck.shouldRerun());
      hbck.close();

      Get get = new Get(hri.getRegionName());
      Result result = meta.get(get);
      assertTrue(result.getColumnCells(HConstants.CATALOG_FAMILY,
        HConstants.SPLITA_QUALIFIER).isEmpty());
      assertTrue(result.getColumnCells(HConstants.CATALOG_FAMILY,
        HConstants.SPLITB_QUALIFIER).isEmpty());
      admin.flush(TableName.META_TABLE_NAME);

      // fix other issues
      doFsck(conf, true);

      // check that all are fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
      IOUtils.closeQuietly(meta);
    }
  }

  /**
   * Tests that LINGERING_SPLIT_PARENT is not erroneously reported for
   * valid cases where the daughters are there.
   */
  @Test (timeout=180000)
  public void testValidLingeringSplitParent() throws Exception {
    TableName table =
        TableName.valueOf("testLingeringSplitParent");
    Table meta = null;
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table);
      HRegionLocation location = tbl.getRegionLocation(Bytes.toBytes("B"));

      meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
      HRegionInfo hri = location.getRegionInfo();

      // do a regular split
      byte[] regionName = location.getRegionInfo().getRegionName();
      admin.splitRegion(location.getRegionInfo().getRegionName(), Bytes.toBytes("BM"));
      TestEndToEndSplitTransaction.blockUntilRegionSplit(conf, 60000, regionName, true);

      // TODO: fixHdfsHoles does not work against splits, since the parent dir lingers on
      // for some time until children references are deleted. HBCK erroneously sees this as
      // overlapping regions
      HBaseFsck hbck = doFsck(
        conf, true, true, false, false, false, true, true, true, false, false, false, null);
      assertErrors(hbck, new ERROR_CODE[] {}); //no LINGERING_SPLIT_PARENT reported

      // assert that the split hbase:meta entry is still there.
      Get get = new Get(hri.getRegionName());
      Result result = meta.get(get);
      assertNotNull(result);
      assertNotNull(MetaTableAccessor.getHRegionInfo(result));

      assertEquals(ROWKEYS.length, countRows());

      // assert that we still have the split regions
      assertEquals(tbl.getStartKeys().length, SPLITS.length + 1 + 1); //SPLITS + 1 is # regions pre-split.
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(table);
      IOUtils.closeQuietly(meta);
    }
  }

  /**
   * Split crashed after write to hbase:meta finished for the parent region, but
   * failed to write daughters (pre HBASE-7721 codebase)
   */
  @Test(timeout=75000)
  public void testSplitDaughtersNotInMeta() throws Exception {
    TableName table = TableName.valueOf("testSplitdaughtersNotInMeta");
    Table meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table);
      HRegionLocation location = tbl.getRegionLocation(Bytes.toBytes("B"));

      HRegionInfo hri = location.getRegionInfo();

      // do a regular split
      byte[] regionName = location.getRegionInfo().getRegionName();
      admin.splitRegion(location.getRegionInfo().getRegionName(), Bytes.toBytes("BM"));
      TestEndToEndSplitTransaction.blockUntilRegionSplit(conf, 60000, regionName, true);

      PairOfSameType<HRegionInfo> daughters =
          MetaTableAccessor.getDaughterRegions(meta.get(new Get(regionName)));

      // Delete daughter regions from meta, but not hdfs, unassign it.
      Map<HRegionInfo, ServerName> hris = tbl.getRegionLocations();
      undeployRegion(connection, hris.get(daughters.getFirst()), daughters.getFirst());
      undeployRegion(connection, hris.get(daughters.getSecond()), daughters.getSecond());

      List<Delete> deletes = new ArrayList<>();
      deletes.add(new Delete(daughters.getFirst().getRegionName()));
      deletes.add(new Delete(daughters.getSecond().getRegionName()));
      meta.delete(deletes);

      // Remove daughters from regionStates
      RegionStates regionStates = TEST_UTIL.getMiniHBaseCluster().getMaster().
          getAssignmentManager().getRegionStates();
      regionStates.deleteRegion(daughters.getFirst());
      regionStates.deleteRegion(daughters.getSecond());

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck,
          new ERROR_CODE[] { ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
              ERROR_CODE.HOLE_IN_REGION_CHAIN }); //no LINGERING_SPLIT_PARENT

      // now fix it. The fix should not revert the region split, but add daughters to META
      hbck = doFsck(
        conf, true, true, false, false, false, false, false, false, false, false, false, null);
      assertErrors(hbck,
          new ERROR_CODE[] { ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
              ERROR_CODE.HOLE_IN_REGION_CHAIN });

      // assert that the split hbase:meta entry is still there.
      Get get = new Get(hri.getRegionName());
      Result result = meta.get(get);
      assertNotNull(result);
      assertNotNull(MetaTableAccessor.getHRegionInfo(result));

      assertEquals(ROWKEYS.length, countRows());

      // assert that we still have the split regions
      assertEquals(tbl.getStartKeys().length, SPLITS.length + 1 + 1); //SPLITS + 1 is # regions pre-split.
      assertNoErrors(doFsck(conf, false)); //should be fixed by now
    } finally {
      meta.close();
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a missing region which is the 1st region -- hole in
   * meta and data missing in the fs.
   */
  @Test(timeout=120000)
  public void testMissingFirstRegion() throws Exception {
    TableName table = TableName.valueOf("testMissingFirstRegion");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes(""), Bytes.toBytes("A"), true,
          true, true);
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.FIRST_REGION_STARTKEY_NOT_EMPTY });
      // fix hole
      doFsck(conf, true);
      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a missing region which is the 1st region -- hole in
   * meta and data missing in the fs.
   */
  @Test(timeout=120000)
  public void testRegionDeployedNotInHdfs() throws Exception {
    TableName table =
        TableName.valueOf("testSingleRegionDeployedNotInHdfs");
    try {
      setupTable(table);
      admin.flush(table);

      // Mess it up by deleting region dir
      deleteRegion(conf, tbl.getTableDescriptor(),
        HConstants.EMPTY_START_ROW, Bytes.toBytes("A"), false,
        false, true);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.NOT_IN_HDFS });
      // fix hole
      doFsck(conf, true);
      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with missing last region -- hole in meta and data missing in
   * the fs.
   */
  @Test(timeout=120000)
  public void testMissingLastRegion() throws Exception {
    TableName table =
        TableName.valueOf("testMissingLastRegion");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("C"), Bytes.toBytes(""), true,
          true, true);
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.LAST_REGION_ENDKEY_NOT_EMPTY });
      // fix hole
      doFsck(conf, true);
      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * Test -noHdfsChecking option can detect and fix assignments issue.
   */
  @Test (timeout=180000)
  public void testFixAssignmentsAndNoHdfsChecking() throws Exception {
    TableName table =
        TableName.valueOf("testFixAssignmentsAndNoHdfsChecking");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by closing a region
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"),
        Bytes.toBytes("B"), true, false, false, false);

      // verify there is no other errors
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
        ERROR_CODE.NOT_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // verify that noHdfsChecking report the same errors
      HBaseFsck fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.onlineHbck();
      assertErrors(fsck, new ERROR_CODE[] {
        ERROR_CODE.NOT_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN});
      fsck.close();

      // verify that fixAssignments works fine with noHdfsChecking
      fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.setFixAssignments(true);
      fsck.onlineHbck();
      assertTrue(fsck.shouldRerun());
      fsck.onlineHbck();
      assertNoErrors(fsck);

      assertEquals(ROWKEYS.length, countRows());

      fsck.close();
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * Test -noHdfsChecking option can detect region is not in meta but deployed.
   * However, it can not fix it without checking Hdfs because we need to get
   * the region info from Hdfs in this case, then to patch the meta.
   */
  @Test (timeout=180000)
  public void testFixMetaNotWorkingWithNoHdfsChecking() throws Exception {
    TableName table =
        TableName.valueOf("testFixMetaNotWorkingWithNoHdfsChecking");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by deleting a region from the metadata
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"),
        Bytes.toBytes("B"), false, true, false, false);

      // verify there is no other errors
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck,
          new ERROR_CODE[] { ERROR_CODE.NOT_IN_META, ERROR_CODE.HOLE_IN_REGION_CHAIN });

      // verify that noHdfsChecking report the same errors
      HBaseFsck fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.onlineHbck();
      assertErrors(fsck,
          new ERROR_CODE[] { ERROR_CODE.NOT_IN_META, ERROR_CODE.HOLE_IN_REGION_CHAIN });
      fsck.close();

      // verify that fixMeta doesn't work with noHdfsChecking
      fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.setFixAssignments(true);
      fsck.setFixMeta(true);
      fsck.onlineHbck();
      assertFalse(fsck.shouldRerun());
      assertErrors(fsck,
          new ERROR_CODE[] { ERROR_CODE.NOT_IN_META, ERROR_CODE.HOLE_IN_REGION_CHAIN });
      fsck.close();

      // fix the cluster so other tests won't be impacted
      fsck = doFsck(conf, true);
      assertTrue(fsck.shouldRerun());
      fsck = doFsck(conf, true);
      assertNoErrors(fsck);
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * Test -fixHdfsHoles doesn't work with -noHdfsChecking option,
   * and -noHdfsChecking can't detect orphan Hdfs region.
   */
  @Test (timeout=180000)
  public void testFixHdfsHolesNotWorkingWithNoHdfsChecking() throws Exception {
    TableName table =
        TableName.valueOf("testFixHdfsHolesNotWorkingWithNoHdfsChecking");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating an overlap in the metadata
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("A"),
        Bytes.toBytes("B"), true, true, false, true);
      admin.enableTable(table);

      HRegionInfo hriOverlap =
          createRegion(tbl.getTableDescriptor(), Bytes.toBytes("A2"), Bytes.toBytes("B"));
      TEST_UTIL.getHBaseCluster().getMaster().assignRegion(hriOverlap);
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
        .waitForAssignment(hriOverlap);
      ServerName server = regionStates.getRegionServerOfRegion(hriOverlap);
      TEST_UTIL.assertRegionOnServer(hriOverlap, server, REGION_ONLINE_TIMEOUT);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {
        ERROR_CODE.ORPHAN_HDFS_REGION, ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
        ERROR_CODE.HOLE_IN_REGION_CHAIN});

      // verify that noHdfsChecking can't detect ORPHAN_HDFS_REGION
      HBaseFsck fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.onlineHbck();
      assertErrors(fsck, new ERROR_CODE[] {
        ERROR_CODE.HOLE_IN_REGION_CHAIN});
      fsck.close();

      // verify that fixHdfsHoles doesn't work with noHdfsChecking
      fsck = new HBaseFsck(conf, hbfsckExecutorService);
      fsck.connect();
      fsck.setDisplayFullReport(); // i.e. -details
      fsck.setTimeLag(0);
      fsck.setCheckHdfs(false);
      fsck.setFixHdfsHoles(true);
      fsck.setFixHdfsOverlaps(true);
      fsck.setFixHdfsOrphans(true);
      fsck.onlineHbck();
      assertFalse(fsck.shouldRerun());
      assertErrors(fsck, new ERROR_CODE[] { ERROR_CODE.HOLE_IN_REGION_CHAIN});
      fsck.close();
    } finally {
      if (admin.isTableDisabled(table)) {
        admin.enableTable(table);
      }
      cleanupTable(table);
    }
  }

  /**
   * We don't have an easy way to verify that a flush completed, so we loop until we find a
   * legitimate hfile and return it.
   * @param fs
   * @param table
   * @return Path of a flushed hfile.
   * @throws IOException
   */
  Path getFlushedHFile(FileSystem fs, TableName table) throws IOException {
    Path tableDir= FSUtils.getTableDir(FSUtils.getRootDir(conf), table);
    Path regionDir = FSUtils.getRegionDirs(fs, tableDir).get(0);
    Path famDir = new Path(regionDir, FAM_STR);

    // keep doing this until we get a legit hfile
    while (true) {
      FileStatus[] hfFss = fs.listStatus(famDir);
      if (hfFss.length == 0) {
        continue;
      }
      for (FileStatus hfs : hfFss) {
        if (!hfs.isDirectory()) {
          return hfs.getPath();
        }
      }
    }
  }

  /**
   * This creates a table and then corrupts an hfile.  Hbck should quarantine the file.
   */
  @Test(timeout=180000)
  public void testQuarantineCorruptHFile() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());
      admin.flush(table); // flush is async.

      FileSystem fs = FileSystem.get(conf);
      Path hfile = getFlushedHFile(fs, table);

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      admin.disableTable(table);

      // create new corrupt file called deadbeef (valid hfile name)
      Path corrupt = new Path(hfile.getParent(), "deadbeef");
      TestHFile.truncateFile(fs, hfile, corrupt);
      LOG.info("Created corrupted file " + corrupt);
      HBaseFsck.debugLsr(conf, FSUtils.getRootDir(conf));

      // we cannot enable here because enable never finished due to the corrupt region.
      HBaseFsck res = HbckTestingUtil.doHFileQuarantine(conf, table);
      assertEquals(res.getRetCode(), 0);
      HFileCorruptionChecker hfcc = res.getHFilecorruptionChecker();
      assertEquals(hfcc.getHFilesChecked(), 5);
      assertEquals(hfcc.getCorrupted().size(), 1);
      assertEquals(hfcc.getFailures().size(), 0);
      assertEquals(hfcc.getQuarantined().size(), 1);
      assertEquals(hfcc.getMissing().size(), 0);

      // Its been fixed, verify that we can enable.
      admin.enableTable(table);
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * Test that use this should have a timeout, because this method could potentially wait forever.
  */
  private void doQuarantineTest(TableName table, HBaseFsck hbck, int check,
                                int corrupt, int fail, int quar, int missing) throws Exception {
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());
      admin.flush(table); // flush is async.

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      admin.disableTable(table);

      String[] args = {"-sidelineCorruptHFiles", "-repairHoles", "-ignorePreCheckPermission",
          table.getNameAsString()};
      HBaseFsck res = hbck.exec(hbfsckExecutorService, args);

      HFileCorruptionChecker hfcc = res.getHFilecorruptionChecker();
      assertEquals(hfcc.getHFilesChecked(), check);
      assertEquals(hfcc.getCorrupted().size(), corrupt);
      assertEquals(hfcc.getFailures().size(), fail);
      assertEquals(hfcc.getQuarantined().size(), quar);
      assertEquals(hfcc.getMissing().size(), missing);

      // its been fixed, verify that we can enable
      admin.enableTableAsync(table);
      while (!admin.isTableEnabled(table)) {
        try {
          Thread.sleep(250);
        } catch (InterruptedException e) {
          e.printStackTrace();
          fail("Interrupted when trying to enable table " + table);
        }
      }
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates a table and simulates the race situation where a concurrent compaction or split
   * has removed an hfile after the corruption checker learned about it.
   */
  @Test(timeout=180000)
  public void testQuarantineMissingHFile() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());

    // inject a fault in the hfcc created.
    final FileSystem fs = FileSystem.get(conf);
    HBaseFsck hbck = new HBaseFsck(conf, hbfsckExecutorService) {
      @Override
      public HFileCorruptionChecker createHFileCorruptionChecker(boolean sidelineCorruptHFiles) throws IOException {
        return new HFileCorruptionChecker(conf, executor, sidelineCorruptHFiles) {
          AtomicBoolean attemptedFirstHFile = new AtomicBoolean(false);
          @Override
          protected void checkHFile(Path p) throws IOException {
            if (attemptedFirstHFile.compareAndSet(false, true)) {
              assertTrue(fs.delete(p, true)); // make sure delete happened.
            }
            super.checkHFile(p);
          }
        };
      }
    };
    doQuarantineTest(table, hbck, 4, 0, 0, 0, 1); // 4 attempted, but 1 missing.
    hbck.close();
  }

  /**
   * This creates a table and simulates the race situation where a concurrent compaction or split
   * has removed an colfam dir before the corruption checker got to it.
   */
  // Disabled because fails sporadically.  Is this test right?  Timing-wise, there could be no
  // files in a column family on initial creation -- as suggested by Matteo.
  @Ignore @Test(timeout=180000)
  public void testQuarantineMissingFamdir() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    // inject a fault in the hfcc created.
    final FileSystem fs = FileSystem.get(conf);
    HBaseFsck hbck = new HBaseFsck(conf, hbfsckExecutorService) {
      @Override
      public HFileCorruptionChecker createHFileCorruptionChecker(boolean sidelineCorruptHFiles) throws IOException {
        return new HFileCorruptionChecker(conf, executor, sidelineCorruptHFiles) {
          AtomicBoolean attemptedFirstHFile = new AtomicBoolean(false);
          @Override
          protected void checkColFamDir(Path p) throws IOException {
            if (attemptedFirstHFile.compareAndSet(false, true)) {
              assertTrue(fs.delete(p, true)); // make sure delete happened.
            }
            super.checkColFamDir(p);
          }
        };
      }
    };
    doQuarantineTest(table, hbck, 3, 0, 0, 0, 1);
    hbck.close();
  }

  /**
   * This creates a table and simulates the race situation where a concurrent compaction or split
   * has removed a region dir before the corruption checker got to it.
   */
  @Test(timeout=180000)
  public void testQuarantineMissingRegionDir() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    // inject a fault in the hfcc created.
    final FileSystem fs = FileSystem.get(conf);
    HBaseFsck hbck = new HBaseFsck(conf, hbfsckExecutorService) {
      @Override
      public HFileCorruptionChecker createHFileCorruptionChecker(boolean sidelineCorruptHFiles)
      throws IOException {
        return new HFileCorruptionChecker(conf, executor, sidelineCorruptHFiles) {
          AtomicBoolean attemptedFirstHFile = new AtomicBoolean(false);
          @Override
          protected void checkRegionDir(Path p) throws IOException {
            if (attemptedFirstHFile.compareAndSet(false, true)) {
              assertTrue(fs.delete(p, true)); // make sure delete happened.
            }
            super.checkRegionDir(p);
          }
        };
      }
    };
    doQuarantineTest(table, hbck, 3, 0, 0, 0, 1);
    hbck.close();
  }

  /**
   * Test fixing lingering reference file.
   */
  @Test (timeout=180000)
  public void testLingeringReferenceFile() throws Exception {
    TableName table =
        TableName.valueOf("testLingeringReferenceFile");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by creating a fake reference file
      FileSystem fs = FileSystem.get(conf);
      Path tableDir= FSUtils.getTableDir(FSUtils.getRootDir(conf), table);
      Path regionDir = FSUtils.getRegionDirs(fs, tableDir).get(0);
      Path famDir = new Path(regionDir, FAM_STR);
      Path fakeReferenceFile = new Path(famDir, "fbce357483ceea.12144538");
      fs.create(fakeReferenceFile);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.LINGERING_REFERENCE_HFILE });
      // fix reference file
      doFsck(conf, true);
      // check that reference file fixed
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * Test mission REGIONINFO_QUALIFIER in hbase:meta
   */
  @Test (timeout=180000)
  public void testMissingRegionInfoQualifier() throws Exception {
    Connection connection = ConnectionFactory.createConnection(conf);
    TableName table = TableName.valueOf("testMissingRegionInfoQualifier");
    try {
      setupTable(table);

      // Mess it up by removing the RegionInfo for one region.
      final List<Delete> deletes = new LinkedList<Delete>();
      Table meta = connection.getTable(TableName.META_TABLE_NAME, hbfsckExecutorService);
      MetaScanner.metaScan(connection, new MetaScanner.MetaScannerVisitor() {

        @Override
        public boolean processRow(Result rowResult) throws IOException {
          HRegionInfo hri = MetaTableAccessor.getHRegionInfo(rowResult);
          if (hri != null && !hri.getTable().isSystemTable()) {
            Delete delete = new Delete(rowResult.getRow());
            delete.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
            deletes.add(delete);
          }
          return true;
        }

        @Override
        public void close() throws IOException {
        }
      });
      meta.delete(deletes);

      // Mess it up by creating a fake hbase:meta entry with no associated RegionInfo
      meta.put(new Put(Bytes.toBytes(table + ",,1361911384013.810e28f59a57da91c66")).add(
        HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER, Bytes.toBytes("node1:60020")));
      meta.put(new Put(Bytes.toBytes(table + ",,1361911384013.810e28f59a57da91c66")).add(
        HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER, Bytes.toBytes(1362150791183L)));
      meta.close();

      HBaseFsck hbck = doFsck(conf, false);
      assertTrue(hbck.getErrors().getErrorList().contains(ERROR_CODE.EMPTY_META_CELL));

      // fix reference file
      hbck = doFsck(conf, true);

      // check that reference file fixed
      assertFalse(hbck.getErrors().getErrorList().contains(ERROR_CODE.EMPTY_META_CELL));
    } finally {
      cleanupTable(table);
    }
    connection.close();
  }

  /**
   * Test pluggable error reporter. It can be plugged in
   * from system property or configuration.
   */
  @Test (timeout=180000)
  public void testErrorReporter() throws Exception {
    try {
      MockErrorReporter.calledCount = 0;
      doFsck(conf, false);
      assertEquals(MockErrorReporter.calledCount, 0);

      conf.set("hbasefsck.errorreporter", MockErrorReporter.class.getName());
      doFsck(conf, false);
      assertTrue(MockErrorReporter.calledCount > 20);
    } finally {
      conf.set("hbasefsck.errorreporter",
        PrintingErrorReporter.class.getName());
      MockErrorReporter.calledCount = 0;
    }
  }

  static class MockErrorReporter implements ErrorReporter {
    static int calledCount = 0;

    @Override
    public void clear() {
      calledCount++;
    }

    @Override
    public void report(String message) {
      calledCount++;
    }

    @Override
    public void reportError(String message) {
      calledCount++;
    }

    @Override
    public void reportError(ERROR_CODE errorCode, String message) {
      calledCount++;
    }

    @Override
    public void reportError(ERROR_CODE errorCode, String message, TableInfo table) {
      calledCount++;
    }

    @Override
    public void reportError(ERROR_CODE errorCode,
        String message, TableInfo table, HbckInfo info) {
      calledCount++;
    }

    @Override
    public void reportError(ERROR_CODE errorCode, String message,
        TableInfo table, HbckInfo info1, HbckInfo info2) {
      calledCount++;
    }

    @Override
    public int summarize() {
      return ++calledCount;
    }

    @Override
    public void detail(String details) {
      calledCount++;
    }

    @Override
    public ArrayList<ERROR_CODE> getErrorList() {
      calledCount++;
      return new ArrayList<ERROR_CODE>();
    }

    @Override
    public void progress() {
      calledCount++;
    }

    @Override
    public void print(String message) {
      calledCount++;
    }

    @Override
    public void resetErrors() {
      calledCount++;
    }

    @Override
    public boolean tableHasErrors(TableInfo table) {
      calledCount++;
      return false;
    }
  }

  @Test(timeout=180000)
  public void testCheckTableLocks() throws Exception {
    IncrementingEnvironmentEdge edge = new IncrementingEnvironmentEdge(0);
    EnvironmentEdgeManager.injectEdge(edge);
    // check no errors
    HBaseFsck hbck = doFsck(conf, false);
    assertNoErrors(hbck);

    ServerName mockName = ServerName.valueOf("localhost", 60000, 1);

    // obtain one lock
    final TableLockManager tableLockManager = TableLockManager.createTableLockManager(conf, TEST_UTIL.getZooKeeperWatcher(), mockName);
    TableLock writeLock = tableLockManager.writeLock(TableName.valueOf("foo"),
        "testCheckTableLocks");
    writeLock.acquire();
    hbck = doFsck(conf, false);
    assertNoErrors(hbck); // should not have expired, no problems

    edge.incrementTime(conf.getLong(TableLockManager.TABLE_LOCK_EXPIRE_TIMEOUT,
        TableLockManager.DEFAULT_TABLE_LOCK_EXPIRE_TIMEOUT_MS)); // let table lock expire

    hbck = doFsck(conf, false);
    assertErrors(hbck, new ERROR_CODE[] {ERROR_CODE.EXPIRED_TABLE_LOCK});

    final CountDownLatch latch = new CountDownLatch(1);
    new Thread() {
      @Override
      public void run() {
        TableLock readLock = tableLockManager.writeLock(TableName.valueOf("foo"),
            "testCheckTableLocks");
        try {
          latch.countDown();
          readLock.acquire();
        } catch (IOException ex) {
          fail();
        } catch (IllegalStateException ex) {
          return; // expected, since this will be reaped under us.
        }
        fail("should not have come here");
      };
    }.start();

    latch.await(); // wait until thread starts
    Threads.sleep(300); // wait some more to ensure writeLock.acquire() is called

    hbck = doFsck(conf, false);
    assertErrors(hbck, new ERROR_CODE[] {ERROR_CODE.EXPIRED_TABLE_LOCK}); // still one expired, one not-expired

    edge.incrementTime(conf.getLong(TableLockManager.TABLE_LOCK_EXPIRE_TIMEOUT,
        TableLockManager.DEFAULT_TABLE_LOCK_EXPIRE_TIMEOUT_MS)); // let table lock expire

    hbck = doFsck(conf, false);
    assertErrors(hbck, new ERROR_CODE[] {ERROR_CODE.EXPIRED_TABLE_LOCK, ERROR_CODE.EXPIRED_TABLE_LOCK}); // both are expired

    conf.setLong(TableLockManager.TABLE_LOCK_EXPIRE_TIMEOUT, 1); // reaping from ZKInterProcessWriteLock uses znode cTime,
                                                                 // which is not injectable through EnvironmentEdge
    Threads.sleep(10);
    hbck = doFsck(conf, true); // now fix both cases

    hbck = doFsck(conf, false);
    assertNoErrors(hbck);

    // ensure that locks are deleted
    writeLock = tableLockManager.writeLock(TableName.valueOf("foo"),
        "should acquire without blocking");
    writeLock.acquire(); // this should not block.
    writeLock.release(); // release for clean state
  }

  /**
   * Test orphaned table ZNode (for table states)
   */
  @Test
  public void testOrphanedTableZNode() throws Exception {
    TableName table = TableName.valueOf("testOrphanedZKTableEntry");

    try {
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getTableStateManager()
      .setTableState(table, ZooKeeperProtos.Table.State.ENABLING);

      try {
        setupTable(table);
        Assert.fail(
          "Create table should fail when its ZNode has already existed with ENABLING state.");
      } catch(TableExistsException t) {
        //Expected exception
      }
      // The setup table was interrupted in some state that needs to some cleanup.
      try {
        cleanupTable(table);
      } catch (IOException e) {
        // Because create table failed, it is expected that the cleanup table would
        // throw some exception.  Ignore and continue.
      }

      HBaseFsck hbck = doFsck(conf, false);
      assertTrue(hbck.getErrors().getErrorList().contains(ERROR_CODE.ORPHANED_ZK_TABLE_ENTRY));

      // fix the orphaned ZK entry
      hbck = doFsck(conf, true);

      // check that orpahned ZK table entry is gone.
      hbck = doFsck(conf, false);
      assertFalse(hbck.getErrors().getErrorList().contains(ERROR_CODE.ORPHANED_ZK_TABLE_ENTRY));
      // Now create table should succeed.
      setupTable(table);
    } finally {
      // This code could be called that either a table was created successfully or set up
      // table failed in some unknown state.  Therefore, clean up can either succeed or fail.
      try {
        cleanupTable(table);
      } catch (IOException e) {
        // The cleanup table would throw some exception if create table failed in some state.
        // Ignore this exception
      }
    }
  }

  @Test (timeout=180000)
  public void testMetaOffline() throws Exception {
    // check no errors
    HBaseFsck hbck = doFsck(conf, false);
    assertNoErrors(hbck);
    deleteMetaRegion(conf, true, false, false);
    hbck = doFsck(conf, false);
    // ERROR_CODE.UNKNOWN is coming because we reportError with a message for the hbase:meta
    // inconsistency and whether we will be fixing it or not.
    assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.NO_META_REGION, ERROR_CODE.UNKNOWN });
    hbck = doFsck(conf, true);
    assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.NO_META_REGION, ERROR_CODE.UNKNOWN });
    hbck = doFsck(conf, false);
    assertNoErrors(hbck);
  }

  private void deleteMetaRegion(Configuration conf, boolean unassign, boolean hdfs,
      boolean regionInfoOnly) throws IOException, InterruptedException {
    HRegionLocation metaLocation = connection.getRegionLocator(TableName.META_TABLE_NAME)
        .getRegionLocation(HConstants.EMPTY_START_ROW);
    ServerName hsa = metaLocation.getServerName();
    HRegionInfo hri = metaLocation.getRegionInfo();
    if (unassign) {
      LOG.info("Undeploying meta region " + hri + " from server " + hsa);
      try (Connection unmanagedConnection = ConnectionFactory.createConnection(conf)) {
        undeployRegion(unmanagedConnection, hsa, hri);
      }
    }

    if (regionInfoOnly) {
      LOG.info("deleting hdfs .regioninfo data: " + hri.toString() + hsa.toString());
      Path rootDir = FSUtils.getRootDir(conf);
      FileSystem fs = rootDir.getFileSystem(conf);
      Path p = new Path(rootDir + "/" + TableName.META_TABLE_NAME.getNameAsString(),
          hri.getEncodedName());
      Path hriPath = new Path(p, HRegionFileSystem.REGION_INFO_FILE);
      fs.delete(hriPath, true);
    }

    if (hdfs) {
      LOG.info("deleting hdfs data: " + hri.toString() + hsa.toString());
      Path rootDir = FSUtils.getRootDir(conf);
      FileSystem fs = rootDir.getFileSystem(conf);
      Path p = new Path(rootDir + "/" + TableName.META_TABLE_NAME.getNameAsString(),
          hri.getEncodedName());
      HBaseFsck.debugLsr(conf, p);
      boolean success = fs.delete(p, true);
      LOG.info("Deleted " + p + " sucessfully? " + success);
      HBaseFsck.debugLsr(conf, p);
    }
  }

  @Test (timeout=180000)
  public void testTableWithNoRegions() throws Exception {
    // We might end up with empty regions in a table
    // see also testNoHdfsTable()
    TableName table =
        TableName.valueOf(name.getMethodName());
    try {
      // create table with one region
      HTableDescriptor desc = new HTableDescriptor(table);
      HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toString(FAM));
      desc.addFamily(hcd); // If a table has no CF's it doesn't get checked
      admin.createTable(desc);
      tbl = (HTable) connection.getTable(table, tableExecutorService);

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      deleteRegion(conf, tbl.getTableDescriptor(), HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_END_ROW, false, false, true);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.NOT_IN_HDFS });

      doFsck(conf, true);

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(table);
    }

  }

  @Test (timeout=180000)
  public void testHbckAfterRegionMerge() throws Exception {
    TableName table = TableName.valueOf("testMergeRegionFilesInHdfs");
    Table meta = null;
    try {
      // disable CatalogJanitor
      TEST_UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(false);
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table);
      HRegionInfo region1 = tbl.getRegionLocation(Bytes.toBytes("A")).getRegionInfo();
      HRegionInfo region2 = tbl.getRegionLocation(Bytes.toBytes("B")).getRegionInfo();

      int regionCountBeforeMerge = tbl.getRegionLocations().size();

      assertNotEquals(region1, region2);

      // do a region merge
      admin.mergeRegions(region1.getEncodedNameAsBytes(),
          region2.getEncodedNameAsBytes(), false);

      // wait until region merged
      long timeout = System.currentTimeMillis() + 30 * 1000;
      while (true) {
        if (tbl.getRegionLocations().size() < regionCountBeforeMerge) {
          break;
        } else if (System.currentTimeMillis() > timeout) {
          fail("Time out waiting on region " + region1.getEncodedName()
              + " and " + region2.getEncodedName() + " be merged");
        }
        Thread.sleep(10);
      }

      assertEquals(ROWKEYS.length, countRows());

      HBaseFsck hbck = doFsck(conf, false);
      assertNoErrors(hbck); // no errors

    } finally {
      TEST_UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(true);
      cleanupTable(table);
      IOUtils.closeQuietly(meta);
    }
  }

  @Test (timeout = 180000)
  public void testRegionBoundariesCheck() throws Exception {
    HBaseFsck hbck = doFsck(conf, false);
    assertNoErrors(hbck); // no errors
    try {
      hbck.checkRegionBoundaries();
    } catch (IllegalArgumentException e) {
      if (e.getMessage().endsWith("not a valid DFS filename.")) {
        fail("Table directory path is not valid." + e.getMessage());
      }
    }
  }

  @org.junit.Rule
  public TestName name = new TestName();

  @Test (timeout=180000)
  public void testReadOnlyProperty() throws Exception {
    HBaseFsck hbck = doFsck(conf, false);
    Assert.assertEquals("shouldIgnorePreCheckPermission", true,
      hbck.shouldIgnorePreCheckPermission());

    hbck = doFsck(conf, true);
    Assert.assertEquals("shouldIgnorePreCheckPermission", false,
      hbck.shouldIgnorePreCheckPermission());

    hbck = doFsck(conf, true);
    hbck.setIgnorePreCheckPermission(true);
    Assert.assertEquals("shouldIgnorePreCheckPermission", true,
      hbck.shouldIgnorePreCheckPermission());
  }

  @Test (timeout=180000)
  public void testCleanUpDaughtersNotInMetaAfterFailedSplit() throws Exception {
    TableName table = TableName.valueOf("testCleanUpDaughtersNotInMetaAfterFailedSplit");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f")));
      admin.createTable(desc);
      tbl = new HTable(cluster.getConfiguration(), desc.getTableName());
      for (int i = 0; i < 5; i++) {
        Put p1 = new Put(("r" + i).getBytes());
        p1.add(Bytes.toBytes("f"), "q1".getBytes(), "v".getBytes());
        tbl.put(p1);
      }
      admin.flush(desc.getTableName());
      List<HRegion> regions = cluster.getRegions(desc.getTableName());
      int serverWith = cluster.getServerWith(regions.get(0).getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(serverWith);
      cluster.getServerWith(regions.get(0).getRegionName());
      SplitTransaction st = new SplitTransaction(regions.get(0), Bytes.toBytes("r3"));
      st.prepare();
      st.stepsBeforePONR(regionServer, regionServer, false);
      AssignmentManager am = cluster.getMaster().getAssignmentManager();
      Map<String, RegionState> regionsInTransition = am.getRegionStates().getRegionsInTransition();
      for (RegionState state : regionsInTransition.values()) {
        am.regionOffline(state.getRegion());
      }
      ZKAssign.deleteNodeFailSilent(regionServer.getZooKeeper(), regions.get(0).getRegionInfo());
      Map<HRegionInfo, ServerName> regionsMap = new HashMap<HRegionInfo, ServerName>();
      regionsMap.put(regions.get(0).getRegionInfo(), regionServer.getServerName());
      am.assign(regionsMap);
      am.waitForAssignment(regions.get(0).getRegionInfo());
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED });
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      assertErrors(
        doFsck(
          conf, false, true, false, false, false, false, false, false, false, false, false, null),
        new ERROR_CODE[] { ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED });

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(5, countRows());
    } finally {
      if (tbl != null) {
        tbl.close();
        tbl = null;
      }
      cleanupTable(table);
    }
  }

}
