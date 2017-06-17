package table;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;


/**
 * @author zhaozhen
 */
public class Mysql2ddl {
    public static boolean scwj(String path, String FileName, String body) {
        try {
            File f = new File(path);
            f.mkdirs();
            path = path + "/" + FileName;
            f = new File(path);
            PrintWriter out;
            out = new PrintWriter(new FileWriter(f));
            out.print(body + "\n");
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public Connection getconConnection(String Url, String Username, String Password) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection("jdbc:mysql://" + Url + ":3306?user=" + Username + " & password=" +Password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void CreatDDL(String HTablename, String Path, String Url, String Username, String Password, String MTablename, String DBName) {
        Connection conn = null;
        try {
            //MYSQL com.mysql.jdbc.Driver "jdbc:mysql://localhost:3306/manager","root","root"
            conn = getconConnection(Url, Username, Password);
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(DBName, null, null, null); //数据库，数据库用户，表名，table/view
            StringBuffer ddl = new StringBuffer();
            while (rs.next()) {
                // 取表名
                String Tablename = rs.getString(3);
                if (Tablename.equals(MTablename)) {
                    if (StringUtils.equalsIgnoreCase(rs.getString(4), "TABLE")
                            ) {
//                    System.out.println(counti + "-" + Tablename);
                        String comment = "";
                        ResultSet rscol = dbmd.getColumns(null, null, Tablename, null);
                        String colstr = "";
                        while (rscol.next()) {
                            String ColumnName = rscol.getString(4);//获取列名
//                        String ColumnTypeName = rscol.getString(6);//字段类型
                            String REMARKS = rscol.getString(12);    //注释
                            if (StringUtils.isNotBlank(REMARKS)) {
                                comment = comment + "COMMENT" + "\t" + " '" + REMARKS + "' ; \n";
                            }

                            if (StringUtils.isNotBlank(colstr)) {
                                colstr = colstr + ",\n";
                            }

                            colstr = colstr + "\t" + ColumnName + "\t";
                        }
                        ddl.append("source /data/home/hadoop/config/set_env.sh\n\n");
                        ddl.append("echo $1/set_env.sh\n\n");
                        ddl.append("use ${DATABASE};");
                        ddl.append("drop table if exists" + HTablename);
                        ddl.append("CREATE EXTERNAL TABLE " + HTablename + "\n(" + colstr + "String" + comment + "\n)\n\n");
                        ddl.append("PARTITIONED BY(ymd string)\n" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001'\n" + "LINES TERMINATED BY '\\n'\n");
                        ddl.append("STORED AS TEXTFILE\n");
                        ddl.append("LOCATION" + "\t'" + Path + "';");
                        ddl.append("alter table" + Tablename + "SET SERDEPROPERTIES('serialization.null.format' = '');\n" + "EOF");
                        ddl.append("[ $? -ne 0 ] && exit 2 ;\n" + "exit  0;");
                    }
                }
            }
            scwj("./", HTablename + ".ddl", ddl.toString());
            rs.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (SQLException e) {
            }
        }

    }

    public static void main(String[] args) {
        Mysql2ddl mysql2ddl = new Mysql2ddl();
        mysql2ddl.CreatDDL(args[0], args[1], args[2], args[3],args[4], args[5], args[6]);  //hive表名,LOCATION路径，mysql的url，mysql登录名，mysql密码,mysql表名,mysql数据库名
    }
}