package hive;//import com.mchange.v2.c3p0.ComboPooledDataSource;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @program: phoenixtest
 * @description: 数据库连接池
 * @author: jiangyun
 * @create: 2018-05-10 22:24
 **/
public class DBPool {

    private static DBPool dbPool;
    private ComboPooledDataSource dataSource;

    static {
        dbPool = new DBPool();
    }

    public DBPool() {
        try {
            dataSource = new ComboPooledDataSource();
           // dataSource.setUser("root");
           // dataSource.setPassword("123456");
            dataSource.setJdbcUrl("jdbc:phoenix:192.168.32.42,192.168.32.43,192.168.32.44:2181");
            dataSource.setDriverClass("org.apache.phoenix.jdbc.PhoenixDriver");
            // 设置初始连接池的大小！
            dataSource.setInitialPoolSize(2);
            // 设置连接池的最小值！
            dataSource.setMinPoolSize(3);
            // 设置连接池的最大值！
            dataSource.setMaxPoolSize(20);
            // 设置连接池中的最大Statements数量！
            dataSource.setMaxStatements(50);
            // 设置连接池的最大空闲时间！
            dataSource.setMaxIdleTime(600000);

        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
    }

    public final static DBPool getInstance() {
        return dbPool;
    }

    public final Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("无法从数据源获取连接 ", e);
        }

    }



    public static void main(String[] args) {
        DBPool instance = getInstance();
        Connection connection = instance.getConnection();
        try {
            Statement statement = connection.createStatement();

            ResultSet resultSet = statement.executeQuery("select * from HBASE_HIVE_TEST_CRM_USER_RESULT  limit 10");
            while (resultSet.next()){
                System.out.println(resultSet.getString(1));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}