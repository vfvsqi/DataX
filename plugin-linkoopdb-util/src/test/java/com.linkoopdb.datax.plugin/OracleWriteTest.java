package com.linkoopdb.datax.plugin;

import com.alibaba.datax.plugin.linkoopdb.util.LinkoopDBUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class OracleWriteTest {
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

    public static void main(String[] args) {
        String url = "jdbc:oracle:thin:@192.168.1.72:1521:xe";
        String driver = "oracle.jdbc.driver.OracleDriver";
        String username = "linkoopdb";
        String password = "123456";
        LinkoopDBUtils.init(username, password, driver, url);
        Connection connection = LinkoopDBUtils.getConnection();
        set(connection);
        connection = LinkoopDBUtils.getConnection();
        get(connection);
    }

    private static void set(Connection connection) {
        String sql = "insert into data_type_test13(info) values (?)";
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setLong(1, 0);
            int ret = statement.executeUpdate();
            System.out.println(ret);
            LinkoopDBUtils.closeConnection(connection, null, statement);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void get(Connection connection) {
        String sql = "select * from data_type_test13";
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet ret = statement.executeQuery();
//            System.out.println(ret.next());
            while (ret.next()) {
                System.out.println(ret.getInt(1));
            }
            LinkoopDBUtils.closeConnection(connection, null, statement);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
