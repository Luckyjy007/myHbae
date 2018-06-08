package hive;

import java.sql.*;
import java.util.Scanner;

import static sun.jvm.hotspot.runtime.PerfMemory.start;

/**
 * @program: phoenixtest
 * @description: 测试类的主类
 * @author: jiangyun
 * @create: 2018-05-09 15:08
 **/
public class ApplicationRun {

    public static void main(String[] args) throws IllegalArgumentException {


//        Executor executor = new Executor();
//        new Thread(executor, "线程一").start();
//        new Thread(executor, "线程二").start();
//        new Thread(executor, "线程三").start();
//        new Thread(etestxecutor, "线程四").start();
//        new Thread(executor, "线程五").start();
//        new Thread(executor, "线程六").start();
        String databaeName="";
        String tablename="";
        String key="";

        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入库名");
        databaeName= scanner.next();
        System.out.println("请输入表名");
        tablename= scanner.next();
        System.out.println("请输入作为key的表字段");
        key= scanner.next();
        HiveUtil hiveUtil = new HiveUtil();

        System.out.println(hiveUtil.hbaseHiveCreatTable(databaeName, tablename, key));


    }

}