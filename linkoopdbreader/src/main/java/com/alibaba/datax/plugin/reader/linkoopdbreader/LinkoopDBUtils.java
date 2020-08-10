package com.alibaba.datax.plugin.reader.linkoopdbreader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;

/**
 * @author: zzp
 * @date 2020/5/14 17:15
 * @description:
 */
public class LinkoopDBUtils {

    static Logger logger = LoggerFactory.getLogger(com.alibaba.datax.plugin.reader.linkoopdbreader.LinkoopDBUtils.class);
    
    public static String ldburl = "";
    public static String ldbusername = "";
    private static String ldbpassword = "";
    private static String ldbdriver = "";

    public static String saveDdlInfo(String fileName, String tableName, String ldbdriver, String url, String username, String password) {
        init(ldbdriver, url, username, password);
        ShowCreateTable showCreateTable = getShowTable(tableName);
        List<TableProperties> properties = getTableProperties(tableName);
        String sql = showCreateTable.getCreate_table();
        String insert = "";
        int size = properties.size();
        if (size > 0) {
            int t = sql.lastIndexOf(")");
            String head = sql.substring(0, t + 1);
            String tail = sql.substring(t + 1, sql.length());
            insert = head + " properties (";
            for (int i = 0; i < size; i++) {
                TableProperties tableProperties = properties.get(i);
                insert += "'";
                insert += tableProperties.getName();
                insert += "'";
                insert += ":";
                insert += "'";
                insert += tableProperties.getValue();
                insert += "',";
            }
            // 去掉最后一个逗号
            insert = insert.substring(0, insert.length() - 1);
            insert += ") ";
            insert += tail;
        } else {
            insert = sql;
        }

        saveFile(fileName, toByteArray(insert));
        return fileName;
    }

    public static File checkFile(String fileName) {
        File file = new File(fileName);
        try {
            if (!file.exists()) {
                file.getParentFile().mkdirs();//构建文件夹
                file.createNewFile();//构建文件
            }
            return file;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean saveFile(String fileName, byte[] content) {
        File fileWrite = checkFile(fileName);
        if (fileWrite == null) {
            logger.error("没有此文件夹");
            return false;
        }
        try {
            // true 为接着文件往后写， false 为重新创建文件写入
            // FileOutputStream outputStream = new FileOutputStream(fileName, true);
            FileOutputStream outputStream = new FileOutputStream(fileName, false);
            outputStream.write(content);
            outputStream.flush();
            outputStream.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static byte[] toByteArray (Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray ();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;
    }

    public static ShowCreateTable getShowTable(String tableName) {
        String sql = "show create table " + tableName;
        ShowCreateTable ret = getTable(ShowCreateTable.class, sql);
        return ret;
    }

    public static List<TableProperties> getTableProperties(String tableName) {
        String sql = "show table properties " + tableName;
        List<TableProperties> list = getList(TableProperties.class, sql);
        return list;
    }

    public static <T> List<T> getList(Class<T> clz, String sql, String... args) {
        Connection connection = getConnection();
        try {
            PreparedStatement statement = LinkoopDBUtils.getStatement(connection, sql, args);
            ResultSet resultSet = statement.executeQuery();
            List<T> resList = new ArrayList<>();
            while (resultSet.next()) {
                T table = clz.newInstance();
                LinkoopDBUtils.preparedStatement2Object(resultSet, table);
                resList.add(table);
            }
            LinkoopDBUtils.closeConnection(connection, resultSet, statement);
            return resList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("get shard info error!");
        }
    }

    public static <T> T getTable(Class<T> clz, String sql, String... args) {
        Connection connection = LinkoopDBUtils.getConnection();
        try {
            PreparedStatement statement = LinkoopDBUtils.getStatement(connection, sql, args);
            ResultSet resultSet = statement.executeQuery();
            T table = clz.newInstance();
            while (resultSet.next()) {
                LinkoopDBUtils.preparedStatement2Object(resultSet, table);
            }
            LinkoopDBUtils.closeConnection(connection, resultSet, statement);
            return table;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("jdbc error");
        }
    }

    public static boolean createTable(String sql) {
        Connection connection = getConnection();
        try {
            PreparedStatement statement = getStatement(connection, sql);
            boolean ret = statement.execute();

            closeConnection(connection, null, statement);
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("get shard info error!");
        }
    }

    public static void main(String[] args) {
        String driverName = "com.datapps.linkoopdb.jdbc.JdbcDriver";
        String url = "jdbc:linkoopdb:tcp://test91:9105/ldb";
        String username = "admin";
        String password = "123456";
        String tableName = "TEST2";
        String fileName = "./testsLinkoopdbDdl";
        saveDdlInfo(fileName, tableName, driverName, url, username, password);
    }

    public static Connection getConnection() {
        System.out.println("==============");
        System.out.println(ldbdriver + ldburl + ldbusername + ldbpassword);
        Connection conn = null;
        try {
            Class.forName(ldbdriver);
            conn = DriverManager.getConnection(ldburl, ldbusername, ldbpassword);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return conn;
    }

    public static void init(String driver, String url, String username, String password) {
        ldbdriver = driver;
        ldburl = url;
        ldbusername = username;
        ldbpassword = password;
    }

    public static PreparedStatement getStatement(Connection connection, String sql, String ...args) {
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(sql);
            for (int i = 0; i < args.length; i++) {
                String str = args[i];
                statement.setString(i + 1, str);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return statement;
    }

    public static void closeConnection(Connection conn, ResultSet rs, PreparedStatement ps) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            // do nothing
        }
    }

    /**
     * PreparedStatement 转 object
     * @param resultSet
     * @param object
     */
    public static void preparedStatement2Object(ResultSet resultSet , Object object) {
        Class<?> c = object.getClass();
        Map<String, Object> map = new HashMap<>();
        Field[] fields=c.getDeclaredFields();//获取属性数组

        for(int i=0; i<fields.length; i++){
            try {
                String key = fields[i].getName();
                Object value = resultSet.getObject(key);
                map.put(key, value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Method[] methods = c.getMethods();//获取对象方法
        for (Method method : methods) {
            if (method.getName().startsWith("set")) {
                String name = method.getName();
                name = name.substring(3, 4).toLowerCase() + name.substring(4, name.length());//获取属性名
                if (map.containsKey(name)) {
                    try {
                        method.invoke(object, map.get(name));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

}
