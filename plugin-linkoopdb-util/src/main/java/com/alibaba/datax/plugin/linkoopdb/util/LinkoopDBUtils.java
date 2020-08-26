package com.alibaba.datax.plugin.linkoopdb.util;

import com.alibaba.datax.plugin.linkoopdb.entity.DBConnectInfo;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * @author: zzp
 * @date 2020/5/14 17:15
 * @description:
 */
public class LinkoopDBUtils {

    public static String ldburl = "";
    public static String ldbusername = "";
    private static String ldbpassword = "";
    private static String ldbdriver = "";

    public static void init (String username, String password, String driver, String url) {
        ldbusername = username;
        ldbpassword = password;
        ldbdriver = driver;
        ldburl = url;
    }

    public static void init (DBConnectInfo db) {
        ldbusername = db.getUsername();
        ldbpassword = db.getPassword();
        ldbdriver = db.getDriver();
        ldburl = db.getUrl();
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


    public static int count(String sql) {

        Connection connection = getConnection();
        try {
            PreparedStatement statement = LinkoopDBUtils.getStatement(connection, sql);
            ResultSet ret = statement.executeQuery(sql);
            int num = 0;

            if(ret.next()){
                num = ret.getInt(1);
            }
            LinkoopDBUtils.closeConnection(connection, null, statement);
            return num;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("get shard info error!");
        }
    }


    public static boolean createTable(String sql) {
        Connection connection = getConnection();
        try {
            PreparedStatement statement = LinkoopDBUtils.getStatement(connection, sql);
            boolean ret = statement.execute();

            LinkoopDBUtils.closeConnection(connection, null, statement);
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("get shard info error!");
        }
    }

    public static void main(String[] args) {
        String driverName = "com.datapps.linkoopdb.jdbc.JdbcDriver";
        String url = "jdbc:linkoopdb:tcp://test91:9105/ldb?query_iterator=1";
        String username = "admin";
        String password = "123456";
        String sql = "select * from TEST3";
//        String t = "PUBLIC.TEST4";
//        String sql = "show create table " + t;
        try {

            Connection connection = getConnection(driverName, url, username, password);
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);

//            PreparedStatement statement = LinkoopDBUtils.getStatement(connection, sql, args);
            statement.setFetchSize(5);

            ResultSet resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                String temp = resultSet.getString(1);
            }

            LinkoopDBUtils.closeConnection(connection, resultSet, null);
            statement.close();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("jdbc error");
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


    public static boolean delete(String sql, String... args) {
        boolean ret = false;
        Connection connection = LinkoopDBUtils.getConnection();
        try {
            PreparedStatement statement = LinkoopDBUtils.getStatement(connection, sql, args);
            ret = statement.execute();

            LinkoopDBUtils.closeConnection(connection, null, statement);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("jdbc error");
        }
        return ret;
    }


    public static int update(String sql, String... args) {
        int res = 0;
        Connection connection = LinkoopDBUtils.getConnection();
        try {
            PreparedStatement statement = LinkoopDBUtils.getStatement(connection, sql, args);
            res = statement.executeUpdate();
            LinkoopDBUtils.closeConnection(connection, null, statement);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("get shard info error!");
        }
        return res;
    }


    public static int insert(String sql, Object object) {
        int res = 0;
        Connection connection = LinkoopDBUtils.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            res = LinkoopDBUtils.insert(statement, object);
            LinkoopDBUtils.closeConnection(connection, null, statement);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("get shard info error!");
        }
        return res;
    }

    public static int insertList(String sql, List<? extends Object> list) {
        int ret = 0;
        Connection connection = LinkoopDBUtils.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            for (int j = 0; j < list.size(); j++) {
                Object object = list.get(j);
                ret += insert(statement, object);
            }
            LinkoopDBUtils.closeConnection(connection, null, statement);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("get shard info error!");
        }
        return ret;
    }


    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(ldbdriver);
            conn = DriverManager.getConnection(ldburl, ldbusername, ldbpassword);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return conn;
    }

    public static Connection getConnection(String driver, String url, String username, String password) {
        ldbdriver = driver;
        ldburl = url;
        ldbusername = username;
        ldbpassword = password;
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return conn;
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


    public static int insert(PreparedStatement statement, Object object) {
        int ret = 0;
        try {

            Class<?> clz = object.getClass();
            Field[] fields= clz.getDeclaredFields();//获取属性数组
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                String key = field.getName();
                String name = "get" + key.substring(0, 1).toUpperCase() + key.substring(1, key.length());//获取属性名
                Method method = clz.getDeclaredMethod(name);
                Type t = method.getAnnotatedReturnType().getType();
                String typeName = t.getTypeName();
                if (typeName.contains(".")) {
                    String[] tmp = typeName.split("\\.");
                    typeName = tmp[tmp.length - 1];
                }
                int num = i + 1;
                if (typeName.equals("int")) {
                    int str = (int)method.invoke(object);
                    statement.setInt(num, str);

                } else if (typeName.equals("double")) {
                    double str = (double)method.invoke(object);
                    statement.setDouble(num, str);
                } else if (typeName.equals("float")) {
                    float str = (float)method.invoke(object);
                    statement.setFloat(num, str);
                } else if (typeName.equals("BigDecimal")) {
                    BigDecimal str = (BigDecimal)method.invoke(object);
                    statement.setBigDecimal(num, str);
                } else if (typeName.equals("Date")) {
                    Date str = (Date)method.invoke(object);
                    statement.setDate(num, str);
                } else if (typeName.equals("String")) {
                    String str = (String)method.invoke(object);
                    statement.setString(num, str);
                } else if (typeName.equals("Timestamp")) {
                    Timestamp str = (Timestamp)method.invoke(object);
                    statement.setTimestamp(num, str);
                } else if (typeName.equals("char")) {
                    String str = (String)method.invoke(object);
                    statement.setString(num, str);
                } else if (typeName.equals("boolean")) {
                    boolean str = (boolean)method.invoke(object);
                    statement.setBoolean(num, str);
                }

//                System.out.println(statement.get);
//                System.out.println(" +++++++  " + str);
//                Object info = null;
//                System.out.println("--------------------------------------");
//                System.out.println("------------------" + str + "--------------------");
//
//                if (null != str) {
//                    info = str;
//                } else if ("".equals(str)) {
//                    System.out.println("这里");
//                } else if ("" == str) {
//                    System.out.println("nali");
//                } else if (null == str) {
//                    System.out.println("null");
//                } else {
//                    System.out.println("什么玩意儿");
//                    str.getClass();
//                }
//                    statement.setObject(i + 1, info);
//                Type type = method.getAnnotatedReturnType().getType();
//                if (type.getClass().isInstance(str)) {
//                    System.out.println("吼吼");
//                    System.out.println(type.getClass().getName());
//                } else {
//                    System.out.println(type.getTypeName());
//                }

//                String para = field.getType().getName();
//                if (para.contains(".")) {
//                    String[] tmp = para.split("\\.");
//                    para = tmp[tmp.length - 1];
//                }
//                String stateName = "set" + para.substring(0, 1).toUpperCase() + para.substring(1, para.length());//获取属性名
//                if (stateName.equals("setChar")) {
//                    stateName = "setString";
//                }
//                Class<?> clss = statement.getClass().getInterfaces()[0];
//                Method[] m = clss.getMethods();
//                Class<?> fieldCLass = field.getType();
//                if (fieldCLass.equals(char.class)) {
//                    fieldCLass = java.lang.String.class;
//                }
//                System.out.println(" +++++++  " + fieldCLass);

//                if (fieldCLass.isInstance(str)) {
//                    System.out.println("_______" + fieldCLass.getName());
//                }
//                Object t1 = fieldCLass.(str);
//                System.out.println(t);
//                t.invoke(i + 1, fieldCLass.cast(str));
//                statement.getClass().getSuperclass().getDeclaredMethod(stateName).invoke(i + 1, (int)str);

//                statement.set
//                Class<?> stateClz = statement.getClass();
//                Method stateMod = stateClz.getDeclaredMethod()

            }
            ret = statement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
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
