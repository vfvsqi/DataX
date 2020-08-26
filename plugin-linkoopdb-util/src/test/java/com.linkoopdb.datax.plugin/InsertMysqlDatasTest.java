package com.linkoopdb.datax.plugin;

import com.alibaba.datax.plugin.linkoopdb.util.LinkoopDBUtils;

public class InsertMysqlDatasTest {
    public static void main(String[] args) {
        String url = "jdbc:mysql://127.0.0.1:13306/mySchema";
        String driver = "com.mysql.jdbc.Driver";
        String username = "root";
        String password = "123456";
        LinkoopDBUtils.init(username, password, driver, url);

        String sql1  = create("data_type_test1", "info TINYINT");
        String sql2  = create("data_type_test2", "info SMALLINT");
        String sql3  = create("data_type_test3", "info BIGINT");
        String sql4  = create("data_type_test4", "info DOUBLE");
        String sql5  = create("data_type_test5", "info DOUBLE");
        String sql6  = create("data_type_test6", "info FLOAT");
        String sql7  = create("data_type_test7", "info DECIMAL");
        String sql8  = create("data_type_test8", "info DECIMAL");
        String sql9  = create("data_type_test9", "info datetime");
        String sql10 = create("data_type_test10", "info timestamp");
        String sql11 = create("data_type_test11", "info VARCHAR(128)");
        String sql12 = create("data_type_test12", "info char(128)");
        String sql13 = create("data_type_test13", "info boolean");
        String sql14 = create("data_type_test14", "info boolean");
        String sql15 = create("data_type_test15", "info BINARY");
        String sql16 = create("data_type_test16", "info blob");
        String sql17 = create("data_type_test17", "info VARBINARY(255)");

        String sql24 = create("data_type_test24", "info text");
        String sql25 = create("data_type_test25", "info boolean");
        String sql27 = create("data_type_test27", "info VARBINARY(255)");
        String sql28 = create("data_type_test28", "info VARBINARY(255)");
    }

    public static String create(String tableName, String type) {
        String createsql = "CREATE TABLE IF NOT EXISTS " + tableName + "(" + type + ");";
        LinkoopDBUtils.createTable(createsql);
        return "";
    }
}
