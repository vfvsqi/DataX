package com.linkoopdb.datax.plugin;

import com.alibaba.datax.plugin.linkoopdb.util.LinkoopDBUtils;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Calendar;

public class InsertLinkoopdbDatasTest {
    public static void main(String[]args) {
//        args = new String[9];
//        args[0] = "jdbc:linkoopdb:tcp://test91:9105/ldb;query_iterator=1";
//        args[1] = "admin";
//        args[2] = "123456";
//        args[3] = "TEST";
//
//        args[4] = "mysql";
//        args[5] = "jdbc:mysql://192.168.1.93:3306/testtrans";
//        args[6] = "ldb";
//        args[7] = "123456";
//        args[8] = "DATAX_INSERT_TEST1";

//        args[4] = "excel";
//        args[5] = "test.xlsx";
        createLinkoopdbTables(args);
    }

    private static void createLinkoopdbTables(String[] args) {

        String url = "jdbc:linkoopdb:tcp://192.168.1.92:9105/ldb";

        String driver = "com.datapps.linkoopdb.jdbc.JdbcDriver";
        String username = "admin";
        String password = "123456";
        LinkoopDBUtils.init(username, password, driver, url);

        String sql1 = create("data_type_test1", "info SMALLINT");
        String sql2 = create("data_type_test2", "info integer");
        String sql3 = create("data_type_test3", "info bigint");
        String sql4 = create("data_type_test4", "info real");
        String sql5 = create("data_type_test5", "info double");
        String sql6 = create("data_type_test6", "info float");
        String sql7 = create("data_type_test7", "info decimal");
        String sql8 = create("data_type_test8", "info numeric");

        // mysql 数据类型会有问题 少一天 已经手动加一天了
        String sql9 = create("data_type_test9", "info date");
        String sql10 = create("data_type_test10", "info timestamp");
        String sql11 = create("data_type_test11", "info varchar(128)");
        String sql12 = create("data_type_test12", "info char(128)");
        String sql13 = create("data_type_test13", "info boolean");
//        // hdfs 不支持 clob
        String sql14 = create("data_type_test14", "info clob");
        String sql15 = create("data_type_test15", "info bit(1)");
//        String sql15 = create("data_type_test15", "info bit");

        // hdfs 不支持 blob
        String sql16 = create("data_type_test16", "info blob");
        String sql17 = create("data_type_test17", "info binary(255)");
        String sql18 = create("data_type_test18", "info VARBINARY(255)");

        Connection connection = LinkoopDBUtils.getConnection();
        int ret = 0;
        try {
            PreparedStatement statement1 = connection.prepareStatement(sql1);
            PreparedStatement statement2 = connection.prepareStatement(sql2);
            PreparedStatement statement3 = connection.prepareStatement(sql3);
            PreparedStatement statement4 = connection.prepareStatement(sql4);
            PreparedStatement statement5 = connection.prepareStatement(sql5);
            PreparedStatement statement6 = connection.prepareStatement(sql6);
            PreparedStatement statement7 = connection.prepareStatement(sql7);
            PreparedStatement statement8 = connection.prepareStatement(sql8);
            PreparedStatement statement9 = connection.prepareStatement(sql9);
            PreparedStatement statement10 = connection.prepareStatement(sql10);
            PreparedStatement statement11 = connection.prepareStatement(sql11);
            PreparedStatement statement12 = connection.prepareStatement(sql12);
            PreparedStatement statement13 = connection.prepareStatement(sql13);
            PreparedStatement statement14 = connection.prepareStatement(sql14);
            PreparedStatement statement15 = connection.prepareStatement(sql15);
            PreparedStatement statement16 = connection.prepareStatement(sql16);
            PreparedStatement statement17 = connection.prepareStatement(sql17);
            PreparedStatement statement18 = connection.prepareStatement(sql18);

            java.sql.Date date = new java.sql.Date(Calendar.getInstance().getTime().getTime());
            String test = "hello world";
            byte[] bytes = test.getBytes();
            int i = 1;
            statement1.setInt(i, 1);
            statement2.setInt(i,2);
            statement3.setInt(i, 3);
            statement4.setDouble(i, 4.5);
            statement5.setDouble(i, 5.5);
            statement6.setFloat(i, 6.5f);
            statement7.setBigDecimal(i, new BigDecimal(7));
            statement8.setInt(i, 8);
            statement9.setDate(i, date);
            statement10.setTimestamp(i, new Timestamp(System.currentTimeMillis()));
            statement11.setString(i, "11");
            statement12.setObject(i, "s");
            statement13.setBoolean(i, true);
            statement14.setString(i, "14");
//            statement15.setBoolean(i, true);
            statement15.setBoolean(i, false);
            statement16.setBytes(i, bytes);
            statement17.setBytes(i, bytes);
            statement18.setBytes(i, bytes);

//            statement1.setInt(i, 1);
//            statement2.setInt(i, 2);
//            statement3.setInt(i, 3);
//            statement4.setDouble(i, 4.5);
//            statement5.setDouble(i, 5.5);
//            statement6.setFloat(i, 6.5f);
//            statement7.setBigDecimal(i, null);
//            statement8.setInt(i, 8);
//            statement9.setDate(i, null);
//            statement10.setTimestamp(i, null);
//            statement11.setString(i, null);
//            statement12.setObject(i, null);
//            statement13.setBoolean(i, false);
//            statement14.setString(i, null);
//            statement15.setBoolean(i, true);
//            statement16.setBytes(i, null);
//            statement17.setBytes(i, null);
//            statement18.setBytes(i, null);

            ret = statement1.executeUpdate();
            ret = statement2.executeUpdate();
            ret = statement3.executeUpdate();
            ret = statement4.executeUpdate();
            ret = statement5.executeUpdate();
            ret = statement6.executeUpdate();
            ret = statement7.executeUpdate();
            ret = statement8.executeUpdate();
            ret = statement9.executeUpdate();
            ret = statement10.executeUpdate();
            ret = statement11.executeUpdate();
            ret = statement12.executeUpdate();
            ret = statement13.executeUpdate();
            ret = statement14.executeUpdate();
            ret = statement15.executeUpdate();
            ret = statement16.executeUpdate();
            ret = statement17.executeUpdate();
            ret = statement18.executeUpdate();
            LinkoopDBUtils.closeConnection(connection, null, statement1);
            LinkoopDBUtils.closeConnection(connection, null, statement2);
            LinkoopDBUtils.closeConnection(connection, null, statement3);
            LinkoopDBUtils.closeConnection(connection, null, statement4);
            LinkoopDBUtils.closeConnection(connection, null, statement5);
            LinkoopDBUtils.closeConnection(connection, null, statement6);
            LinkoopDBUtils.closeConnection(connection, null, statement7);
            LinkoopDBUtils.closeConnection(connection, null, statement8);
            LinkoopDBUtils.closeConnection(connection, null, statement9);
            LinkoopDBUtils.closeConnection(connection, null, statement10);
            LinkoopDBUtils.closeConnection(connection, null, statement11);
            LinkoopDBUtils.closeConnection(connection, null, statement12);
            LinkoopDBUtils.closeConnection(connection, null, statement13);
            LinkoopDBUtils.closeConnection(connection, null, statement14);
            LinkoopDBUtils.closeConnection(connection, null, statement15);
            LinkoopDBUtils.closeConnection(connection, null, statement16);
            LinkoopDBUtils.closeConnection(connection, null, statement17);
            LinkoopDBUtils.closeConnection(connection, null, statement18);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("get shard info error!");
        }
    }

    public static String create(String tableName, String type) {
        String schema = "TESTHDFS";
        String createsql = "CREATE TABLE IF NOT EXISTS " + schema + "." + tableName + "(" + type + ")";
//        String createsql = "CREATE TABLE IF NOT EXISTS " + schema + "." + tableName + "(" + type + ") engine pallas;";
        LinkoopDBUtils.createTable(createsql);
        String sql  = "insert into " + schema + "." + tableName + " values(?);";
        return sql;
    }

}
