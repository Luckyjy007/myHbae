package hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 * @program: phoenixtest
 * @description:
 * @author: jiangyun
 * @create: 2018-05-10 23:05
 **/
public class Executor implements Runnable {

static DBPool dbPool;
static {dbPool = new DBPool();}
private volatile String sql="";
static  volatile long time=0;
static volatile int numbers=0;
static volatile int errors=0;


    @Override
    public void run() {

        while (true){
            long start = System.currentTimeMillis();
            sql="";
            Random random = new Random();
            Connection connection = dbPool.getConnection();
            try {
                Statement statement = connection.createStatement();
                statement.setQueryTimeout(6000000);
                ResultSet resultSet = statement.executeQuery(sql);
                long end = System.currentTimeMillis();
                long spend = end - start;
                System.out.println("执行该条sql\t"+sql+"花了"+spend+"毫秒");
                time+=spend;
                numbers+=1;
              //  System.out.println("总共耗时"+time+"毫秒："+" 执行sql "+numbers+"次"+"平均耗时"+(time/numbers));

            } catch (SQLException e) {
                e.printStackTrace();
                errors++;
              System.out.println("sql错误"+errors+" 次 错误SQL:  "+sql);
            }finally {
                try {
                    System.out.println("总共耗时"+time+"毫秒："+" 执行sql:"+numbers+"次\t"+"   错误sql次数:"+errors+"次\t平均每条sql耗时:"+(time/numbers)+" 毫秒");


                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(random.nextInt(100000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}