package utils;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;


/*
Create by jiangyun on 2017/12/25
*/
public class Test {

    public static void main(String[] args) throws Throwable {

        long start = System.currentTimeMillis();

     ResultScanner rowByRange = HBaseUtil.getRowByRange("golden_compass:HBASE_TEST ", "0000163b3e3120000a3c2a4bddc79c2b152799524", "0000163b3e3120000a3c2a4bddc79c2b152799525");
      List<String> list = HBaseUtil.resultScannerToList(rowByRange);
      for (String s:list){
          System.out.println(s);
       }
      Scan user_group_detail_ex = HBaseUtil.preCount( "0000000000000000", "ZZZZZZZZZZZZZZZ");
    HBaseUtil.rowCount2("golden_compass:appLogIqj_test",user_group_detail_ex);
        long end = System.currentTimeMillis();
        System.out.println(end-start+" ms");
      String[] cfs= {"CF"};

   HBaseUtil.createTable("golden_compass:HBASE_TEST",cfs,true);
    System.out.println("successful");


    }
}