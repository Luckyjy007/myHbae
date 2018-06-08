//import org.testng.annotations.Configuration;
//import org.apache.hadoop.security.UserGroupInformation;
//import org.testng.annotations.Test;
//
//import java.sql.*;
//
///**
// * @program: phoenixtest
// * @description: phoenix连接查询测试
// * @author: jiangyun
// * @create: 2018-05-10 20:09
// **/
//public class PhoenixTest {
//
//  public static Statement statement;
//    private static org.apache.hadoop.conf.Configuration testConfiguration;
//static {
//    try { testConfiguration = new Configuration();
//        //testConfiguration.set("keytab.file" , "/Users/apple/Desktop/phoenixtest/src/main/resources/hdfs.keytab" );
//       // testConfiguration.set("hadoop.security.authentication", "kerberos");
//      //  testConfiguration.set("kerberos.principal","hdfs/_HOST@hadoop_aqj");
//        testConfiguration.set("hbase.master.kerberos.principal", "hbase/_HOST@hadoop_aqj");
//        testConfiguration.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@hadoop_aqj");
//        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
//       // String url = "jdbc:phoenix:192.168.32.42,192.168.32.43,192.168.32.44:2181";
//        String url = "jdbc:phoenix:10.10.201.17:2181";
//        System.setProperty("java.security.krb5.conf","/Users/apple/Desktop/phoenixtest/src/main/resources/krb5.conf");
//        UserGroupInformation.setConfiguration(testConfiguration);
//        UserGroupInformation. loginUserFromKeytab("hbase", "/Users/apple/Desktop/phoenixtest/src/main/resources/hbase.keytab" );
//
//        Connection conn = DriverManager.getConnection(url);
//
//         statement = conn.createStatement();
//    }catch (Exception e){
//        e.printStackTrace();
//    }
//
//}
//
//    @Test
//    public void select1() throws Exception{
//       statement.setQueryTimeout(600000);
//
//        ResultSet resultSet = statement.executeQuery(" select * from SYSTEM.CATALOG ");
//        ResultSetMetaData metaData = resultSet.getMetaData();
//        int columnCount = metaData.getColumnCount();
//
//
//        while (resultSet.next()){
//            for (int i=1;i<=columnCount;i++){
//                System.out.print(resultSet.getString(i)+"\t");
//            }
//            System.out.println();
//        }
//
//
//    }
//    @Test
//    public void select2() throws  Exception{
//
//
//        ResultSet resultSet = statement.executeQuery("SELECT  *  from HBASE_HIVE_TEST_CRM_USER_RESULT WHERE  ZCB1_ADD  IN ('1','7','3') limit 10");
//
//        ResultSetMetaData metaData = resultSet.getMetaData();
//
//        int columnCount = metaData.getColumnCount();
//        while (resultSet.next()){
//
//            for (int i=1;i<=columnCount;i++){
//
//                System.out.print(resultSet.getString(i)+"\t");
//
//            }
//            System.out.println("\n");
//        }
//
//    }
//
//    public void select3() throws  Exception{
//        ResultSet resultSet = statement.executeQuery("SELECT  *  from HBASE_HIVE_TEST_CRM_USER_RESULT WHERE  ZCB1_ADD  NOT IN ('1','2','3') limit 10");
//        ResultSetMetaData metaData = resultSet.getMetaData();
//        int columnCount = metaData.getColumnCount();
//        while (resultSet.next()){
//            for (int i=1;i<=columnCount;i++){
//
//                System.out.print(resultSet.getString(i)+"\t");
//
//            }
//            System.out.println("\n");
//        }
//
//    }
//}