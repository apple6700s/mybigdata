package com.datastory.commons3.es.copyData;

import com.datastory.commons3.es.utils.GetShardMeta;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;


/**
 * com.datastory.commons3.es.copyData.CopyHdfsData
 *
 * @author zhaozhen
 * @since 2017/6/14
 */


public class CopyHdfsData extends GetShardMeta {


    private String shardHost;
    private String hdfsDir;
    private String subDir;


    public static String getHostname() {
        InetAddress ip = null;

        try {
            ip = InetAddress.getLocalHost();
            return ip.getHostName();
        } catch (UnknownHostException var2) {
            var2.printStackTrace();
            return null;
        }
    }

    //拉取hdfs数据到磁盘数据目录
    public void doCopy(int totalShards, String dir, String index, Configuration conf) throws IOException {

        for (int i = 0; i < totalShards; i++) {
            int toShard = i;
            shardHost = getShardHost(toShard, index);
            hdfsDir = getHdfsDir(toShard, index);
            subDir = getSubDir(toShard, index);
            //拉取本机对应shard的数据压缩包，
            if (shardHost.equals(getHostname())) {
                Path path = new Path(hdfsDir);
                FileSystem fs = FileSystem.get(URI.create(shardHost), conf);
                fs.copyToLocalFile(path, new Path(dir + subDir));
                System.out.println("download: from" + hdfsDir + " to " + dir + subDir);
                fs.close();
            }
        }
    }

    //删除hdfs数据
    public void delHdfsData(String filePath) throws IOException {

        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(path, true);
        System.out.println("Delete: " + filePath);

    }

    /**
     * 解压到指定目录
     *
     * @param totalShards
     */
    public void unZipFiles(int totalShards, String dir, String index) throws IOException {

        for (int i = 0; i < totalShards; i++) {
            int toShard = i;
            unZipFiles(new File(dir + getSubDir(toShard, index)), dir + getSubDir(toShard, index));
            deleteFile(dir + getHdfsDir(toShard, index));
        }

    }

    /**
     * 解压文件到指定目录
     *
     * @param zipFile
     * @param descDir
     */

    public static void unZipFiles(File zipFile, String descDir) throws IOException {
        File pathFile = new File(descDir);
        if (!pathFile.exists()) {
            pathFile.mkdirs();
        }
        ZipFile zip = new ZipFile(zipFile);
        for (Enumeration entries = zip.entries(); entries.hasMoreElements(); ) {
            ZipEntry entry = (ZipEntry) entries.nextElement();
            String zipEntryName = entry.getName();
            InputStream in = zip.getInputStream(entry);
            String outPath = (descDir + zipEntryName).replaceAll("\\*", "/");
//            判断路径是否存在,不存在则创建文件路径
            File file = new File(outPath.substring(0, outPath.lastIndexOf('/')));
            if (!file.exists()) {
                System.out.println("[ERROR]:The target path does not exist！");
            }

            //输出文件路径信息
            System.out.println(outPath);

            OutputStream out = new FileOutputStream(outPath);
            byte[] buf1 = new byte[1024];
            int len;
            while ((len = in.read(buf1)) > 0) {
                out.write(buf1, 0, len);
            }
            in.close();
            out.close();
        }
        System.out.println("******************解压完毕,删除源文件********************");
    }

    public static void ZipDecompress(String frompath, String topath) throws IOException {
        ZipFile zf = new ZipFile(new File(frompath));
        InputStream inputStream;
        Enumeration en = zf.entries();
        while (en.hasMoreElements()) {
            ZipEntry zn = (ZipEntry) en.nextElement();
            if (!zn.isDirectory()) {
                inputStream = zf.getInputStream(zn);
                File f = new File(topath + zn.getName());
                File file = f.getParentFile();
                file.mkdirs();
                System.out.println(zn.getName() + "---" + zn.getSize());

                FileOutputStream outputStream = new FileOutputStream(topath + zn.getName());
                int len = 0;
                byte bufer[] = new byte[1024];
                while (-1 != (len = inputStream.read(bufer))) {
                    outputStream.write(bufer, 0, len);
                }
                outputStream.close();
            }
        }
    }

    //删除文件
    public boolean deleteFile(String sPath) {
        boolean flag = false;
        File file = new File(sPath);
        // 路径为文件且不为空则进行删除
        if (file.isFile() && file.exists()) {
            file.delete();
            flag = true;
        }
        return flag;
    }

}


