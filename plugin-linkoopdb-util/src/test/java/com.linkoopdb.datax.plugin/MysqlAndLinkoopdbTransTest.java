package com.linkoopdb.datax.plugin;

import com.alibaba.datax.plugin.linkoopdb.util.LinkoopDBUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class MysqlAndLinkoopdbTransTest {
    public static void main(String[] args) {
        String url = "jdbc:linkoopdb:tcp://test91:9105/ldb;query_iterator=1";
        String driver = "com.datapps.linkoopdb.jdbc.JdbcDriver";
        String username = "admin";
        String password = "123456";
        LinkoopDBUtils.init(username, password, driver, url);
        String sql29 = InsertLinkoopdbDatasTest.create("data_type_test29", "info boolean");
        Connection connection = LinkoopDBUtils.getConnection();
        try {
            PreparedStatement statement29 = connection.prepareStatement(sql29);
//            statement29.setBoolean(1, 1);
            statement29.setLong(1, 0);
            int ret = statement29.executeUpdate();
            LinkoopDBUtils.closeConnection(connection, null, statement29);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
