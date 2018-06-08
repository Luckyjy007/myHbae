package hive;

import java.sql.*;
import java.util.Scanner;

/**
 * @program: phoenixtest
 * @description: hive连接测试
 * @author: jiangyun
 * @create: 2018-05-11 11:17
 **/
public class HiveTest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
       // System.out.println("please input database name");
        //String next = scanner.next();
       // System.out.println(next);

        try {
        Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

       Connection connection = DriverManager.getConnection("jdbc:hive2://192.168.40.21:10000/","hive","");
        Statement statement = connection.createStatement();
        statement.setQueryTimeout(60000);
        ResultSet resultSet = statement.executeQuery("SELECT * from sa.sa08_pre_forum_post limit 10");
        ResultSet show_databaes = statement.executeQuery("show databases");
        ResultSetMetaData metaData1 = show_databaes.getMetaData();
        int columnCount1 = metaData1.getColumnCount();
        while (show_databaes.next()){
            for (int i=1;i<=columnCount1;i++){
                System.out.println(show_databaes.getString(i));
            }
        }

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

//while (resultSet.next()){
//    for (int i=1;i<=columnCount;i++){
//      System.out.print(resultSet.getString(i)+" ,");
//    }
//    System.out.println();
//}



    }

}