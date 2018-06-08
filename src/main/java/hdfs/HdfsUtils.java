package hdfs;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.common.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
Create by jiangyun on 2018/1/5
*/
public class HdfsUtils {

    private static final Logger logger = LoggerFactory.getLogger(HdfsUtils.class);
    private static HdfsUtils hdfsUtils = null;
    private static Configuration configuration;
   // private static Configuration testConfiguration;
    private final static FileSystem fs = null;
    private static String uri = "hdfs://HadoopCluster";
   // private final static String testUri = "hdfs://test-001.qianjin.com:8020";

    static {
        configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://HadoopCluster");
        configuration.set("dfs.nameservices", "HadoopCluster");
        configuration.set("dfs.ha.namenodes.HadoopCluster", "namenode1,namenode2");
        configuration.set("dfs.namenode.rpc-address.HadoopCluster.namenode1", "hadoop-001.qianjin.com:8020");
        configuration.set("dfs.namenode.rpc-address.HadoopCluster.namenode2", "hadoop-002.qianjin.com:8020");
        configuration.set("dfs.client.failover.proxy.provider.HadoopCluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        configuration.setBoolean("dfs.support.append", true);
        configuration.setInt("dfs.client.socket-timeout", 600000);
        configuration.setInt("dfs.socket.timeout", 600000);
        configuration.setInt("dfs.datanode.socket.write.timeout", 600000);
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");


    }

   // static {
//        testConfiguration = new Configuration();
//        testConfiguration.setBoolean("dfs.support.append", true);
//        testConfiguration.setInt("dfs.client.socket-timeout", 600000);
//        testConfiguration.setInt("dfs.socket.timeout", 600000);
//        testConfiguration.setInt("dfs.datanode.socket.write.timeout", 600000);
//        testConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
//        testConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
//        System.setProperty("HADOOP_USER_NAME", "hadoop_aqj");


      //  System.setProperty("java.security.krb5.conf","/Users/apple/Desktop/phoenixtest/src/main/resources/krb5.conf");
       // testConfiguration.setBoolean("hadoop.security.authentication",true);
//        testConfiguration.set("keytab.file" , "/Users/apple/Desktop/phoenixtest/src/main/resources/hdfs.keytab" );
//        testConfiguration.set("hadoop.security.authentication", "kerberos");
//        testConfiguration.set("kerberos.principal", "hdfs/_HOST@hadoop_aqj");
      //  testConfiguration.set("java.security.krb5.realm", "TDH");
       // testConfiguration.set("dfs.namenode.kerberos.principal","hdfs/_HOST@hadoop_aqj");//hdfs-site.xml中配置信息
       // testConfiguration.set("dfs.datanode.kerberos.principal","hdfs/_HOST@hadoop_aqj");//hdfs-site.xml中配置信息
       // System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
        //UserGroupInformation.setConfiguration(testConfiguration);
       // try {
         //   UserGroupInformation. loginUserFromKeytab("hdfs", "/Users/apple/Desktop/phoenixtest/src/main/resources/hdfs.keytab" );
       // } catch (IOException e) {
        //    e.printStackTrace();
      //  }
   // }


    public static boolean downLoad(String hdfsUrl,String localUrl)  {

        Path hdfsPAth = new Path(hdfsUrl);
        Path localPath = new Path(localUrl);
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsUrl), configuration);
            fs.copyToLocalFile(hdfsPAth,localPath);
        } catch (IOException e) {
            logger.error(".......................从hdfs下载文件出现错误,请检查url是否正确.......................");
            return false;
        }
        return true;

    }

    public static List<String> listAllFile(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return new ArrayList<String>();
        }
        dir = uri + dir;
        FileSystem fs = FileSystem.get(URI.create(dir), configuration);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path(dir), true);
        List<String> all = new ArrayList<String>();
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            String name = next.getPath().toString();
            all.add(name);

        }
        fs.close();
        return all;
    }

    /**
     * list files/directories/links names under a directory, not include embed
     * objects
     *
     * @param dir a folder path may like '/tmp/testdir'
     * @return List<String> list of file names
     * @throws IOException file io exception
     */
    public static List<String> listCurrentDir(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return new ArrayList<String>();
        }
        dir = uri + dir;
        FileSystem fs = FileSystem.get(URI.create(dir), configuration);
        FileStatus[] stats = fs.listStatus(new Path(dir));
        List<String> names = new ArrayList<String>();
        for (int i = 0; i < stats.length; ++i) {
            if (stats[i].isFile()) {
                // regular file
                names.add(stats[i].getPath().toString());
            } else if (stats[i].isDirectory()) {
                // dir
                names.add(stats[i].getPath().toString());
            } else if (stats[i].isSymlink()) {
                // is s symlink in linux
                names.add(stats[i].getPath().toString());
            }
        }

        fs.close();
        return names;
    }

    /*
     * upload the local file to the hds,
     * notice that the path is full like /tmp/test.txt
     * if local file not exists, it will throw a FileNotFoundException
     *
     * @param localFile local file path, may like F:/test.txt or /usr/local/test.txt
     *
     * @param hdfsFile hdfs file path, may like /tmp/dir
     * @return boolean true-success, false-failed
     *
     * @throws IOException file io exception
     */
    public static boolean uploadLocalFile2HDFS(String localFile, String hdfsFile) throws IOException {
        if (StringUtils.isBlank(localFile) || StringUtils.isBlank(hdfsFile)) {
            return false;
        }
        hdfsFile = uri + hdfsFile;
        FileSystem hdfs = FileSystem.get(URI.create(uri), configuration);
        Path src = new Path(localFile);
        Path dst = new Path(hdfsFile);
        hdfs.copyFromLocalFile(src, dst);
        hdfs.close();
        return true;
    }

    /*
     * create a new file in the hdfs.
     *
     * notice that the toCreateFilePath is the full path
     *
     * and write the content to the hdfs file.
     */

    /**
     * create a new file in the hdfs.
     * if dir not exists, it will create one
     *
     * @param newPath new file path, a full path name, may like '/tmp/test.txt'
     * @param content file content
     * @return boolean true-success, false-failed
     * @throws IOException file io exception
     */
    public static boolean createNewHDFSFile(String newPath, String content) throws Exception {
        if (StringUtils.isBlank(newPath) || null == content) {
            return false;
        }
        FileSystem hdfs = FileSystem.get(URI.create(newPath), configuration);
        FSDataOutputStream os = hdfs.create(new Path(newPath));

        append(uri, content);

        // os.write(content.getBytes("UTF-8"));
        os.close();
        hdfs.close();
        return true;
    }

    /**
     * make a new dir in the hdfs
     *
     * @param path the dir may like '/tmp/testdir'
     * @return boolean true-success, false-failed
     * @throws IOException something wrong happends when operating files
     */
    public static boolean mkdir(String path) throws IOException {
        if (StringUtils.isBlank(path)) {
            return false;
        }
        path = uri + path;
        FileSystem fs = FileSystem.get(URI.create(path), configuration);
        if (!fs.exists(new Path(path))) {
            fs.mkdirs(new Path(path));
        }

        fs.close();
        return true;
    }

    /**
     * delete the hdfs file
     *
     * @param hdfsFile a full path name, may like '/tmp/test.txt'
     * @return boolean true-success, false-failed
     * @throws IOException file io exception
     */
    public static boolean deleteHDFSFile(String hdfsFile) throws IOException {
        if (StringUtils.isBlank(hdfsFile)) {
            return false;
        }
        hdfsFile = uri + hdfsFile;
        FileSystem hdfs = FileSystem.get(URI.create(hdfsFile), configuration);
        Path path = new Path(hdfsFile);
        boolean isDeleted = hdfs.delete(path, true);
        hdfs.close();
        return isDeleted;
    }

    /**
     * read the hdfs file content
     *
     * @param hdfsFile a full path name, may like '/tmp/test.txt'
     * @return byte[] file content
     * @throws IOException file io exception
     */
    public static byte[] readHDFSFile(String hdfsFile) throws Exception {
        if (StringUtils.isBlank(hdfsFile)) {
            return null;
        }
        hdfsFile = uri + hdfsFile;
        FileSystem fs = FileSystem.get(URI.create(hdfsFile), configuration);
        // check if the file exists
        Path path = new Path(hdfsFile);
        if (fs.exists(path)) {
            FSDataInputStream is = fs.open(path);
            // get the file info to create the buffer
            FileStatus stat = fs.getFileStatus(path);
            // create the buffer
            byte[] buffer = new byte[Integer.parseInt(String.valueOf((stat.getLen())))];
            is.readFully(0, buffer);
            is.close();
            fs.close();
            return buffer;
        } else {
            throw new Exception("the file is not found .");
        }
    }

    /**
     * append something to file dst
     *
     * @param hdfsFile a full path name, may like '/tmp/test.txt'
     * @param content  string
     * @return boolean true-success, false-failed
     * @throws Exception something wrong
     */
    public static boolean append(String hdfsFile, String content) throws Exception {
        if (StringUtils.isBlank(hdfsFile)) {
            return false;
        }
        if (StringUtils.isEmpty(content)) {
            return true;
        }

        hdfsFile = uri + hdfsFile;
        FileSystem fs = FileSystem.get(URI.create(hdfsFile), configuration);
        // check if the file exists
        Path path = new Path(hdfsFile);
        if (fs.exists(path)) {
            try {
                InputStream in = new ByteArrayInputStream(content.getBytes());
                OutputStream out = fs.append(new Path(hdfsFile));
                IOUtils.copyBytes(in, out, 4096, true);
                out.close();
                in.close();
                fs.close();
            } catch (Exception ex) {
                fs.close();
                throw ex;
            }
        } else {
            HdfsUtils.createNewHDFSFile(hdfsFile, content);
        }
        return true;
    }

    public static long getMaxSizeFile(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            throw new FileNotFoundException("Path does not exist");
        }
        dir = uri + dir;
        FileSystem fs = FileSystem.get(URI.create(dir), configuration);
        FileStatus[] fileStatuses = fs.listStatus(new Path(dir));
        long maxSzieFile = 0;
        for (int i = 0; i < fileStatuses.length; i++) {
            if (fileStatuses[i].isFile()) {
                long fileSize = fileStatuses[i].getLen();
                if (fileSize >= maxSzieFile) {
                    maxSzieFile = fileSize;
                }
            }

        }
        fs.close();
        return maxSzieFile;
    }

    public static double getMaxSizeFileMB(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            throw new FileNotFoundException("Path does not exist");
        }
        dir = uri + dir;
        FileSystem fs = FileSystem.get(URI.create(dir), configuration);
        FileStatus[] fileStatuses = fs.listStatus(new Path(dir));
        long maxSzieFile = 0;
        for (int i = 0; i < fileStatuses.length; i++) {
            if (fileStatuses[i].isFile()) {
                long fileSize = fileStatuses[i].getLen();
                if (fileSize >= maxSzieFile) {
                    maxSzieFile = fileSize;
                }
            }

        }
        double tmp = maxSzieFile / (1024.0 * 1024.0);
        BigDecimal bigDecimal = new BigDecimal(tmp);
        double result = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        fs.close();
        return result;
    }


    //获取文件夹下文件大小 单位MB
    public static double getDirSize(String dir) throws IOException {
        dir = uri + dir;
        logger.info("-----------------获取 "+dir+" 文件夹下文件的大小 单位MB---------------------------");
        double totalSize=0.0;
        FileSystem fs = FileSystem.get(URI.create(dir), configuration);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path(dir), true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            long blockSize = next.getBlockSize();
            totalSize+=blockSize;

        }
        totalSize = totalSize / (1024.0 * 1024.0);
        BigDecimal bigDecimal = new BigDecimal(totalSize);
        double result = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        fs.close();



        return totalSize;
    }



    public static long getMiniFileSize(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            throw new FileNotFoundException("Path does not exist");
        }
        dir = uri + dir;
        FileSystem fs = FileSystem.get(URI.create(dir), configuration);
        FileStatus[] fileStatuses = fs.listStatus(new Path(dir));
        long miniSzieFile = 4;
        for (int i = 0; i < fileStatuses.length; i++) {
            if (fileStatuses[i].isFile()) {
                long fileSize = fileStatuses[i].getLen();
                if (fileSize <= miniSzieFile) {
                    miniSzieFile = fileSize;
                }
            }

        }
        fs.close();
        return miniSzieFile;
    }

    public static int getFileNumber(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            throw new FileNotFoundException("Path does not exist");
        }
        dir = uri + dir;
        FileSystem fs = FileSystem.get(URI.create(dir), configuration);
        FileStatus[] fileStatuses = fs.listStatus(new Path(dir));
        int numbers = 0;
        for (int i = 0; i < fileStatuses.length; i++) {
            if (fileStatuses[i].isFile()) {
                numbers++;
            }
        }
        fs.close();
        return numbers;
    }

    public static int getDirNumber(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            throw new FileNotFoundException("Path does not exist");
        }
        dir = uri + dir;
        FileSystem fs = FileSystem.get(URI.create(dir), configuration);
        FileStatus[] fileStatuses = fs.listStatus(new Path(dir));
        int numbers = 0;
        for (int i = 0; i < fileStatuses.length; i++) {
            if (fileStatuses[i].isDirectory()) {
                numbers++;
            }
        }
        fs.close();
        return numbers;
    }

    public static int getSymlinkNumber(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            throw new FileNotFoundException("Path does not exist");
        }
        dir = uri + dir;
        FileSystem fs = FileSystem.get(URI.create(dir), configuration);
        FileStatus[] fileStatuses = fs.listStatus(new Path(dir));
        int numbers = 0;
        for (int i = 0; i < fileStatuses.length; i++) {
            if (fileStatuses[i].isSymlink()) {
                numbers++;
            }
        }
        fs.close();
        return numbers;
    }

    public static FileStatus getLastFile(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            throw new FileNotFoundException("Path does not exist");
        }
        dir = uri + dir;
        FileSystem fs = FileSystem.get(URI.create(dir), configuration);
        FileStatus[] fileStatuses = fs.listStatus(new Path(dir));
        FileStatus fileStatus = null;
        long lastTime = 0;
        for (int i = 0; i < fileStatuses.length; i++) {
            if (fileStatuses[i].isFile()) {
                while (fileStatuses[i].getModificationTime() > lastTime) {
                    lastTime = fileStatuses[i].getModificationTime();
                    fileStatus = fileStatuses[i];
                }
            }
        }
        fs.close();
        return fileStatus;
    }

    public static long getEarliestFileTime(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            throw new FileNotFoundException("Path does not exist");
        }
        dir = uri + dir;
        FileSystem fs = FileSystem.get(URI.create(dir), configuration);
        FileStatus[] fileStatuses = fs.listStatus(new Path(dir));
        long lastTime = Long.MAX_VALUE;
        for (int i = 0; i < fileStatuses.length; i++) {
            if (fileStatuses[i].isFile()) {
                while (fileStatuses[i].getModificationTime() < lastTime) {
                    lastTime = fileStatuses[i].getModificationTime();
                }
            }
        }
        fs.close();
        return lastTime;
    }

    public static int getFileNumberRecursion(String dir) throws IOException {
        return listAllFile(dir).size();
    }

    public static FileStatus getFile(String path) throws IOException {

        if (StringUtils.isBlank(path)) {
            throw new FileNotFoundException("Path does not exist");
        }
        path = uri + path;
        Path hpath = new Path(path);
        FileSystem fs = FileSystem.get(URI.create(path), configuration);
        if (fs.exists(hpath)) {

            return fs.getFileStatus(hpath);
        }else {
            FSDataOutputStream os = fs.create(hpath);
            os.close();
            return fs.getFileStatus(hpath);
        }


    }

    public static void main(String[] args) throws IOException {


      System.out.println(getDirSize("/hbase/data/golden_compass/DM_USER_ACT_INFO"));
      System.out.println(getDirSize("/user/hive/warehouse/dm/dm_user_act_info/etl_tx_dt=20180604"));
       // System.out.println(getDirSize("/hbase"));

    }
}
