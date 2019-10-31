package org.darkphoenixs.hbase.repair;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.*;

import static org.apache.hadoop.hbase.regionserver.HRegionFileSystem.REGION_INFO_FILE;

@Slf4j
@Component
public class HbaseRepairRunner implements ApplicationRunner {

    @Value("${zookeeper.address}")
    private String zookeeperAddress;

    @Value("${zookeeper.nodeParent}")
    private String zookeeperNodeParent;

    @Value("${hdfs.root.dir}")
    private String hdfsRootDir;

    @Value("${repair.tableName}")
    private String repairTableName;

    final String TABLE = "hbase:meta";
    final String FAMILY = "info";
    final String SN = "sn";
    final String SERVER = "server";
    final String STATE = "state";

    @Override
    public void run(ApplicationArguments args) throws Exception {

        String HBASE_QUORUM = "hbase.zookeeper.quorum";
        String HBASE_ROOTDIR = "hbase.rootdir";
        String HBASE_ZNODE_PARENT = "zookeeper.znode.parent";

        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HBASE_QUORUM, zookeeperAddress);
        configuration.set(HBASE_ROOTDIR, hdfsRootDir);
        configuration.set(HBASE_ZNODE_PARENT, zookeeperNodeParent);

        Set<String> metaRegions = getMetaRegions(configuration, repairTableName);

        log.warn(JSON.toJSONString(metaRegions));

        Map<String, RegionInfo> hdfsRegions = getHdfsRegions(configuration, repairTableName);

        Set<String> hdfsRegionNames = hdfsRegions.keySet();

        log.warn(JSON.toJSONString(hdfsRegionNames));

        metaRegions.removeAll(hdfsRegionNames);

        log.warn("Delete hbase Metadata:" + JSON.toJSONString(metaRegions));

        Connection conn = ConnectionFactory.createConnection(configuration);

        Admin admin = conn.getAdmin();

        ServerName[] regionServers = admin.getRegionServers().toArray(new ServerName[0]);

        log.warn(JSON.toJSONString(regionServers));

        Table table = conn.getTable(TableName.valueOf(TABLE));

        int rsLength = regionServers.length;
        int i = 0;
        for (String regionName : hdfsRegionNames) {

            String sn = regionServers[i % rsLength].getServerName();
            String[] snSig = sn.split(",");

            RegionInfo hri = hdfsRegions.get(regionName);
            Put info = MetaTableAccessor.makePutFromRegionInfo(hri, EnvironmentEdgeManager.currentTime());
            info.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(SN), Bytes.toBytes(sn));
            info.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(SERVER), Bytes.toBytes(snSig[0] + ":" + snSig[1]));
            info.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(STATE), Bytes.toBytes("OPEN"));

            table.put(info);
            i++;
        }

        for (String regionName : metaRegions) {

            table.delete(new Delete(Bytes.toBytes(regionName)));
        }

        conn.close();
    }

    public Set<String> getMetaRegions(Configuration conf, String tableName) throws Exception {

        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(TABLE));

        PrefixFilter filter = new PrefixFilter(Bytes.toBytes(tableName + ","));

        Scan scan = new Scan();
        scan.setFilter(filter);

        Set<String> metaRegions = new HashSet<>();

        Iterator<Result> iterator = table.getScanner(scan).iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            metaRegions.add(Bytes.toString(result.getRow()));
        }

        conn.close();

        return metaRegions;
    }


    public Map<String, RegionInfo> getHdfsRegions(Configuration conf, String tablePath) throws Exception {

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(hdfsRootDir + "/data/default/" + tablePath + "/");

        Map<String, RegionInfo> hdfsRegions = new HashMap<>();

        FileStatus[] list = fs.listStatus(path);
        for (FileStatus status : list) {
            if (!status.isDirectory()) {
                continue;
            }

            boolean isRegion = false;
            FileStatus[] regions = fs.listStatus(status.getPath());
            for (FileStatus regionStatus : regions) {
                if (regionStatus.toString().contains(REGION_INFO_FILE)) {
                    isRegion = true;
                    break;
                }
            }

            if (!isRegion) {
                continue;
            }

            RegionInfo hri = HRegionFileSystem.loadRegionInfoFileContent(fs, status.getPath());
            hdfsRegions.put(hri.getRegionNameAsString(), hri);

        }

        return hdfsRegions;
    }


}
