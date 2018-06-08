package hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: phoenixtest
 * @description: phoniex工具
 * @author: jiangyun
 * @create: 2018-05-13 00:37
 **/
public class HiveUtil {

    private static Logger logger = LoggerFactory.getLogger(HiveUtil.class);
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static Connection connection = null;
    private static Map<String,String> tablescoulmes = null;
   // private static List tablescoulmes = null;
    static {
        try {
            Class.forName(driverName);
            connection = DriverManager.getConnection("jdbc:hive2://192.168.40.21:10000/", "hive", "");
            tablescoulmes=new HashMap<>(100);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("获取connection异常");
        }
    }

    //获取表的字段和字段属性
    private Map<String,String> gettablescoulmes(String databaeName,String tableName){

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(" describe "+databaeName+"."+tableName);
            while (resultSet.next()){

                    if (resultSet.getString(1)!=null&&!resultSet.getString(1).startsWith("#")&&resultSet.getString(1)!=null&&resultSet.getString(1).length()>=1){
                        String key = resultSet.getString(1).trim().replace(" ", "").replace("    ", "").replace("\n", "");
                        String value = resultSet.getString(2).trim().replace(" ", "").replace("    ", "").replace("\n", "");

                        if (key.length()>=1){
                            tablescoulmes.put(key,value); }
                    }



            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tablescoulmes;

    }
    //生成hive建表语句
    private String hiveCreateTable(String databaeName,String tableName){
        String sql="create table if not exists ".toUpperCase()+databaeName+"."+tableName+" (";
        String columes="";
        gettablescoulmes(databaeName,tableName);
        for (Map.Entry<String,String> entry:tablescoulmes.entrySet()){
            sql+= entry.getKey() + " " + entry.getValue() + ",";
            columes+=entry.getKey()+",";
        }

        return sql.substring(0,sql.length()-1)+") select "+columes.substring(0,columes.length()-1)+" from ";
    }



    //生成hive集成hbase语句
    public String hbaseHiveCreatTable(String databaeName,String tableName,String key) throws IllegalArgumentException {

        String hive="create table if not exists ".toUpperCase()+"dma_hb"+"."+tableName+" (key ";
        String hbase="STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES  (\"hbase.columns.mapping\" = \":key,";
        String phoenix="CREATE VIEW HBASE_HIVE_TEST_"+databaeName.toUpperCase()+"_"+tableName.toUpperCase()+" (\"key\" varchar primary key,";
        String hivetmp="";
        String keyvalue="";
        String inserOverWrite="insert overwrite table ".toUpperCase()+"dma_hb"+"."+tableName+" "+" select".toUpperCase()+" "+key+",";
        boolean keyIsRight=false;

        gettablescoulmes(databaeName,tableName);

        for (Map.Entry<String,String> entry:tablescoulmes.entrySet()){

          if (!entry.getKey().toLowerCase().equals(key.toLowerCase())){
              hivetmp+= entry.getKey() + " " + entry.getValue() + ",";
              hbase+="CF:"+entry.getKey().toUpperCase()+",";
              phoenix+="\"CF\"."+"\""+entry.getKey().toUpperCase()+"\""+" varchar,";
              inserOverWrite+=entry.getKey()+",";
          }else {
              keyvalue=entry.getValue()+",";
              keyIsRight=true;
          }
        }
        if (!keyIsRight){  throw new IllegalArgumentException(" key字段不存在表字段中 ");}
        if (tablescoulmes.containsKey("etl_tx_dt")){
            System.out.println("表字段一共："+(tablescoulmes.size()-1)+"个\n");
        }else { System.out.println("表字段一共："+tablescoulmes.size()+"个\n");}
        String hivetmps = keyvalue + hivetmp;
        hive+=hivetmps.substring(0,hivetmps.length()-1)+")";
        return "hivehabseCreateTable=>\n"+hive+hbase.substring(0,hbase.length()-1)+"\")"+"TBLPROPERTIES (\"hbase.table.name\" = \""+"golden_compass:"+databaeName.toUpperCase()+"_"+tableName.toUpperCase()+"\")"
                +"\nphoenixView =>\n"+phoenix.substring(0,phoenix.length()-1)+")\nhiveInserOverwrite=>\n"+inserOverWrite.substring(0,inserOverWrite.length()-1)+" FROM "+databaeName+"."+tableName+" WHERE etl_tx_dt=20180510";

    }


}