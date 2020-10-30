package com.alibaba.datax.plugin.linkoopdb.service;

import com.alibaba.datax.plugin.linkoopdb.dao.ShardInfoDao;
import com.alibaba.datax.plugin.linkoopdb.entity.DBConnectInfo;
import com.alibaba.datax.plugin.linkoopdb.entity.SchemaAndTableName;
import com.alibaba.datax.plugin.linkoopdb.entity.ShowCreateTable;
import com.alibaba.datax.plugin.linkoopdb.enums.TransEnums;
import com.alibaba.datax.plugin.linkoopdb.util.LinkoopDBUtils;
import java.util.List;

public class CreateTable {

    public String createTableInOtherDatabase(DBConnectInfo linkoopdb, DBConnectInfo otherDb, String linkName) {
        ShardInfoDao shardInfoDao = new ShardInfoDao();
        String ret = "";
        try {
            createLink(linkoopdb, linkName, otherDb);
            String tableName = linkoopdb.getTableName();
            String[] tmp = tableName.split("\\.");
            if (tmp.length == 2) {
                boolean create = createTable(linkName, linkoopdb, otherDb);
                if (create) {
                    System.out.println(tableName + "," + otherDb.getTableName());
                }
            } else {
                List<SchemaAndTableName> list = shardInfoDao.getList(tableName);
                for (SchemaAndTableName schemaAndTableName : list) {
                    String schemaName = schemaAndTableName.getTable_schem();
                    String schemaTableName = schemaAndTableName.getTable_name();
                    String ldbTableName = schemaName + "." +  schemaTableName;
                    linkoopdb.setTableName(ldbTableName);
                    otherDb.setTableName(schemaTableName);
                    boolean create = createTable(linkName, linkoopdb, otherDb);
                    if (create) {
                        System.out.println(ldbTableName + "," + otherDb.getTableName());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 删除 dblink
            deleteLink(linkName);
        }
        return "";
    }

    private static void checkIfMysqlTableExists(DBConnectInfo linkoopdb, DBConnectInfo otherDb) {
        if (otherDb.getDbType() == null) {

        }
        String tableName = otherDb.getTableName();
        LinkoopDBUtils.init(otherDb);
        String sql = "SHOW TABLES LIKE '" + tableName + "'";
        boolean exists = LinkoopDBUtils.checkIfMysqlTableExists(sql);
        if (exists) {
            tableName += "_TMP";
            otherDb.setTableName(tableName);
            checkIfMysqlTableExists(linkoopdb, otherDb);
        } else {
            LinkoopDBUtils.init(linkoopdb);
        }
    }

    public boolean createTable(String linkName, DBConnectInfo linkoopdb, DBConnectInfo otherDb) {
        checkIfMysqlTableExists(linkoopdb, otherDb);

        String tableName = linkoopdb.getTableName();
        TransEnums.DataBaseType dbType = otherDb.getDbType();
        String otherTableName = otherDb.getTableName();
        String linkDbTableName = linkName + "." + otherTableName;
        ShardInfoDao shardInfoDao = new ShardInfoDao();
        ShowCreateTable showCreateTable = shardInfoDao.getShowTable(tableName);
        String sql = showCreateTable.getCreate_table();
        int head = sql.indexOf("(");
        int tail = sql.lastIndexOf(")");
        String inner = sql.substring(head + 1, tail).toUpperCase();

        // 拉飞线，判断 CLOB BLOB 类型的 hdfs 表不支持转换（其实是数据库不支持这两种类型的 hdfs）
//        if (!sql.contains("ENGINE PALLAS")) {
        if (sql.contains("CLOB")) {
            System.out.println("error ：不支持包含 CLOB 类型的表，所以不支持表" + tableName + "的迁移");
            return false;
        }
        if (sql.contains("BLOB")) {
            System.out.println("error ：不支持包含 BLOB 类型的表，所以不支持表" + tableName + "的迁移");
            return false;
        }
//        }

        if (dbType.equals(TransEnums.DataBaseType.Oracle)){
            if (inner.contains("BOOLEAN")) {
                inner = inner.replaceAll("BOOLEAN", "INT");
            }
        }
        String createTalbe = "create table " + linkDbTableName + " (" + inner + ")";
        LinkoopDBUtils.createTable(createTalbe);
        return true;
    }

    public static void createLink(DBConnectInfo linkoopdb, String linkName, DBConnectInfo otherDb) {

        TransEnums.DataBaseType dbType = otherDb.getDbType();
        String createLink = "CREATE DATABASE LINK " + linkName + " CONNECT TO '" + otherDb.getUsername() + "' IDENTIFIED BY '" + otherDb.getPassword() + "' USING '" + otherDb.getUrl() + "'";
//        if (dbType.equals(TransEnums.DataBaseType.MySql)) {
//            createLink += "' PROPERTIES ('caseSensitive':'true'" + ");";
//        } else if (dbType.equals(TransEnums.DataBaseType.Oracle)) {
//            createLink += "' PROPERTIES ('caseSensitive':'true'" + ");";
//        } else if (dbType.equals(TransEnums.DataBaseType.PostgreSQL)) {
//            createLink += "' PROPERTIES ('maxActive':'10'" + ");";
//        }
        LinkoopDBUtils.createTable(createLink);
    }

    public static void main(String[] args) {
        String a = "test a clob ";
        String b = "clob";
        String c = "CLOB";

        System.out.println(a.contains(b));
        System.out.println(a.contains(c));
        System.out.println(a.indexOf(b));
        System.out.println(a.indexOf(c));
    }

    public static void deleteLink(String linkName) {
        String dropLink = "DROP DATABASE LINK " + linkName + " CASCADE;";
        LinkoopDBUtils.createTable(dropLink);
    }
}
