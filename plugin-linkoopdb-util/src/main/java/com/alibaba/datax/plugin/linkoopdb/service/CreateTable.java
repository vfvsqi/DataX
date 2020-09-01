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
            createLink(linkName, otherDb);
            String tableName = linkoopdb.getTableName();
            String[] tmp = tableName.split("\\.");
            if (tmp.length == 2) {
                createTable(linkName, tableName, otherDb.getTableName(), otherDb.getDbType());
                System.out.println(tableName + "," + otherDb.getTableName());
            } else {

                List<SchemaAndTableName> list = shardInfoDao.getList(tableName);
                for (SchemaAndTableName schemaAndTableName : list) {
                    String tmpTableName = schemaAndTableName.getTable_name();
//                    List<String> lists = Arrays.asList(new String[]{"DATA_TYPE_TEST13"});
//                    if (!lists.contains(tmpTableName)) {
                        createTable(linkName, schemaAndTableName.getTable_schem() + "." +  tmpTableName, tmpTableName, otherDb.getDbType());
                        System.out.println(tableName + "." + tmpTableName + "," + tmpTableName);
//                    }
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

    private void delete(DBConnectInfo linkoopdb, DBConnectInfo otherDb, String tableName) {
        LinkoopDBUtils.init(otherDb);
        String sql = "drop table " + tableName;
        LinkoopDBUtils.delete(sql);
        LinkoopDBUtils.init(linkoopdb);
    }

    public void createTable(String linkName, String tableName, String otherTableName, TransEnums.DataBaseType dbType) {
        String linkDbTableName = linkName + "." + otherTableName;
        ShardInfoDao shardInfoDao = new ShardInfoDao();
        ShowCreateTable showCreateTable = shardInfoDao.getShowTable(tableName);
        String sql = showCreateTable.getCreate_table();
        int head = sql.indexOf("(");
        int tail = sql.lastIndexOf(")");
        String inner = sql.substring(head + 1, tail).toUpperCase();

        if (dbType.equals(TransEnums.DataBaseType.Oracle)){
            if (inner.contains("BOOLEAN")) {
                inner = inner.replaceAll("BOOLEAN", "INT");
            }
        }

        String createTalbe = "create table " + linkDbTableName + " (" + inner + ")";
        System.out.println(createTalbe);
        LinkoopDBUtils.createTable(createTalbe);
    }

    public static void createLink(String linkName, DBConnectInfo otherDb) {
        TransEnums.DataBaseType dbType = otherDb.getDbType();
        String createLink = "CREATE DATABASE LINK " + linkName + " CONNECT TO '" + otherDb.getUsername() + "' IDENTIFIED BY '" + otherDb.getPassword() + "' USING '" + otherDb.getUrl() + "'";
//        if (dbType.equals(TransEnums.DataBaseType.MySql)) {
//            createLink += "' PROPERTIES ('caseSensitive':'true'" + ");";
//        } else if (dbType.equals(TransEnums.DataBaseType.Oracle)) {
//            createLink += "' PROPERTIES ('caseSensitive':'true'" + ");";
//        } else if (dbType.equals(TransEnums.DataBaseType.PostgreSQL)) {
//            createLink += "' PROPERTIES ('maxActive':'10'" + ");";
//        }
        System.out.println(createLink);
        LinkoopDBUtils.createTable(createLink);
    }

    public static void deleteLink(String linkName) {
        String dropLink = "DROP DATABASE LINK " + linkName + " CASCADE;";
        LinkoopDBUtils.createTable(dropLink);
    }
}
