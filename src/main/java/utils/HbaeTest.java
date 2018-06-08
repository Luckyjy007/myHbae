package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.filter.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: myhbase
 * @description:
 * @author: jiangyun
 * @create: 2018-06-08 14:16
 **/
public class HbaeTest {

    private static final Logger logger = LoggerFactory.getLogger(HbaeTest.class);

    private static Configuration conf=null;
    private static Connection conn;
    private static AggregationClient ac;

    static {
        conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://HadoopCluster");
        conf.set("dfs.nameservices", "HadoopCluster");
        conf.set("dfs.ha.namenodes.HadoopCluster", "namenode1,namenode2");
        conf.set("dfs.namenode.rpc-address.HadoopCluster.namenode1", "hadoop-001.qianjin.com:8020");
        conf.set("dfs.namenode.rpc-address.HadoopCluster.namenode2", "hadoop-002.qianjin.com:8020");
        conf.set("dfs.client.failover.proxy.provider.HadoopCluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        // conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setInt("dfs.datanode.socket.write.timeout", 600000);
        conf.setInt("hbase.rpc.timeout",20000);
        conf.setInt("hbase.client.operation.timeout",30000);
        conf.setInt("hbase.client.scanner.timeout.period",200000);
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "192.168.40.26,192.168.40.27,192.168.40.28,192.168.40.29,192.168.40.25,192.168.32.42");
        ac = new AggregationClient(conf);
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //666 d2416fa720a7955b97facef8e4dd021a
    //666 d2416fa720a7955b97facef8e4dd021a

private static String concateStartKey(String strategyId ,String userId){

     return    Md5Util.getHash(strategyId+"20180608")+userId;

}
    private static String getEndKey(String strategyId ){

        return    Md5Util.getHash(strategyId+"20180608")+"9999999999";

    }

    public static long getUserId(String tableName,long userId,String strategyId,long pagePerNum,long count){
        count = count(tableName, userId/2, strategyId);

        if (count<=pagePerNum){
            logger.warn("-------------得到合适user_id"+userId+"------------");
            return userId;
        }else {
            return getUserId( tableName, userId/2, strategyId, pagePerNum,count);}


    }

    public static ResultScanner getresult(String tableName,long userId,String strategyId ,long pagePerNum) throws Throwable {

        long count = count(tableName, userId, strategyId);
        long id=1;
        if (count>pagePerNum){
            id = getUserId(tableName, userId, strategyId, pagePerNum,count);
        }


        logger.warn("得到"+count+"条结果");
        long page=1;
        if (count%1000!=0){
            page=count/1000;
            logger.warn("--------------分页"+page+"-----------------");
        }else {
            page = count/1000+1;
            logger.warn("--------------分页"+page+"-----------------");
        }

        System.out.println(id);


        return null;

    }

 public static long count(String tableName,long userId,String strategyId) {
     String startKey = concateStartKey(strategyId, String.valueOf(userId));
     String endKey = getEndKey(strategyId);
     Scan scan = null;
     try {
         scan = HBaseUtil.preCount(startKey, endKey);
     } catch (IOException e) {
         e.printStackTrace();
     }
     long l = 0;
     try {
         l = HBaseUtil.rowCount2(tableName, scan);
     } catch (Throwable throwable) {
         throwable.printStackTrace();
     }

     System.out.println(1);
        return l;

 }


    public static List<Result> getNumRegexRow(String tableName,String startRowKey,String endRowKey, String regxKey,int num) {
        Table table=null;
        List<Result> list = null;
        try {

             table = HBaseUtil.getTable(tableName);

            //创建一个过滤器容器，并设置其关系（AND/OR）
            FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            //设置正则过滤器
            RegexStringComparator rc = new RegexStringComparator(regxKey);
            RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL, rc);
            //过滤获取的条数
            Filter filterNum = new PageFilter(num);//每页展示条数
            //过滤器的添加
            fl.addFilter(rf);
            fl.addFilter(filterNum);
            Scan scan = new Scan();
            //设置取值范围
            scan.setStartRow(startRowKey.getBytes());//开始的key
            scan.setStopRow(endRowKey.getBytes());//结束的key
            scan.setFilter(fl);//为查询设置过滤器的list
            ResultScanner scanner = table.getScanner(scan) ;
            list = new ArrayList<Result>() ;
            for (Result rs : scanner) {
                list.add(rs) ;
            }
        } catch (Exception e) {
            e.printStackTrace() ;
        }
        finally
        {
            try {
                table.close() ;

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println(list.size());
        return list;
    }


    public static void main(String[] args) throws Throwable {

        List<Result> numRegexRow = getNumRegexRow("golden_compass:HBASE_TEST", "d2416fa720a7955b97facef8e4dd021a0000000000", "d2416fa720a7955b97facef8e4dd021a9999999999", "", 1000);

        System.out.println(numRegexRow);
        //long count = count("golden_compass:HBASE_TEST", "1200", "666");

     //   getresult("golden_compass:HBASE_TEST",1200, "666",1000L);
       // System.out.println(count);



        // ResultScanner rowByRange = HBaseUtil.getRowByRange("golden_compass:HBASE_TEST", "d2416fa720a7955b97facef8e4dd021a0000000000", "d2416fa720a7955b97facef8e4dd021a0000000009");
        //List<String> list = HBaseUtil.resultScannerToList(rowByRange);



    }



}