package hive;

import java.sql.*;
import java.util.*;

/**
 * @program: phoenixtest
 * @description: 生成sql执行语句
 * @author: jiangyun
 * @create: 2018-05-10 23:14
 **/
public class Sql {
    private static String url = "jdbc:phoenix:192.168.32.42,192.168.32.43,192.168.32.44:2181";
    static {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            String url = "jdbc:phoenix:192.168.32.42,192.168.32.43,192.168.32.44:2181";
        }catch (Exception e){
            e.printStackTrace();
        }

    }




    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url);
    }
    private static HashSet<Integer> generateNumbers(int numberLenth,int number ){
        HashSet<Integer> numbers=new HashSet<Integer>(20);
        Random random= new Random();
        Integer init=1;
        for (int i=1;i<numberLenth;i++){
            init=init*10;
        }
        for (int i=0;i<number;i++){
            numbers.add(init+random.nextInt(init));
        }


        return numbers;
    }

    private static String getNumbers(int numberLenth,int number){

        String sql=" ( ";
        HashSet<Integer> hashSet = generateNumbers(numberLenth, number);
        for (Integer integer:hashSet){
            sql+="'"+integer+"',";
        }

return sql.substring(0,sql.length()-1)+")";
    }

    //获取所有表的表名
    private static Set<String> showTables() throws SQLException {
        Connection connection = getConnection();

        Set<String> tableList = new HashSet<String>(100);
        Statement statement = connection.createStatement();
        statement.setQueryTimeout(600000);
        ResultSet resultSet = statement.executeQuery("select * from SYSTEM.CATALOG");
        while (resultSet.next()){
           tableList.add( resultSet.getString(3));
        }

        return tableList;

    }

    //获取所有列名
    private static List<String> describeTable(String tableName) throws SQLException {
        Connection connection = getConnection();
        List<String> columes = new ArrayList<>(100);
        Set<String> tableList = showTables();
        if (tableName==null||!tableList.contains(tableName)){
            throw new SQLException("表不存在======");
        }

        Statement statement = connection.createStatement();
        statement.setQueryTimeout(600000);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM "+tableName+" WHERE 1=2" );
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i=1;i<=columnCount;i++){
                columes.add( metaData.getColumnName(i));
        }

        return columes;

    }

    //获取查询结果
    private static List<String> query(String sql) throws SQLException {
        Connection connection = getConnection();
        List<String> result = new ArrayList<String>(100);
        Statement statement = connection.createStatement();
        statement.setQueryTimeout(6000000);
        ResultSet resultSet = statement.executeQuery(sql);
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();


        while (resultSet.next()){
            String line="";
        for (int i=1;i<=columnCount;i++){
            line+=resultSet.getString(i)+",";
        }
        result.add(line.substring(0,line.length()-1));
        }

        return result;
    }





}

