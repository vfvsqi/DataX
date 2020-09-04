package com.alibaba.datax.plugin.linkoopdb;

import com.alibaba.datax.plugin.linkoopdb.dao.ShardInfoDao;
import com.alibaba.datax.plugin.linkoopdb.entity.DBConnectInfo;
import com.alibaba.datax.plugin.linkoopdb.entity.SchemaAndTableName;
import com.alibaba.datax.plugin.linkoopdb.enums.TransEnums;
import com.alibaba.datax.plugin.linkoopdb.service.CreateTable;
import com.alibaba.datax.plugin.linkoopdb.util.LinkoopDBUtils;

import java.util.List;

public class Application {
    public static void main(String[] args) throws Exception {

//        int len = args.length;
//        System.out.println(len);
//        for (int i = 0; i < len; i++) {
//            System.out.println(args[i]);
//        }

//        args = new String[9];
//        args[0] = "jdbc:linkoopdb:tcp://192.168.1.91:9105/ldb;query_iterator=1";
//        args[1] = "admin";
//        args[2] = "123456";
//        args[3] = "DATATRANS";
////
//        args[4] = "oracle";
//        args[5] = "jdbc:oracle:thin:@192.168.1.72:1521:xe";
//        args[6] = "linkoopdb";
//        args[7] = "123456";
//        args[8] = "DATAX_INSERT_TEST1";
//        args[4] = "excel";
//        args[5] = "test.xlsx";


//        jdbc:postgresql://192.168.1.72:5432/postgres postgres 123456

//        if (args.length > 5) {
//            String ret = "入参有误，入参应按照 linkoopdb url,username,password,table, 异构数据库类型,url,user,pass,table 这个顺序来";
//            throw new Exception(ret);
//        }
        trans(args);
    }

    public static void trans(String[] args) {
        DBConnectInfo linkoopdb = new DBConnectInfo();
        DBConnectInfo otherDb = new DBConnectInfo();
        linkoopdb.setUrl(args[0]);
        linkoopdb.setUsername(args[1]);
        linkoopdb.setPassword(args[2]);
        linkoopdb.setTableName(args[3]);
        linkoopdb.setDriver(TransEnums.DataBaseType.Linkoopdb.getDriverClassName());

        String dbType = args[4];
        TransEnums.DataBaseType driver = TransEnums.DataBaseType.get(dbType);
        LinkoopDBUtils.init(linkoopdb);

        if (null == driver) {
            // 如果不是 db 类型的写入数据，则 arg 后面几个顺序为 type, filename
            String tableName = linkoopdb.getTableName();
            String[] tmp = tableName.split("\\.");
            String fileName = args[5];
            if (dbType.equals("txtfile")) {
                fileName = ".txt";
            } else if (dbType.equals("excel")) {
                fileName = ".xlsx";
            }

            if (tmp.length == 1) {
                ShardInfoDao shardInfoDao = new ShardInfoDao();
                List<SchemaAndTableName> list = shardInfoDao.getList(tableName);
                for (SchemaAndTableName schemaAndTableName : list) {
                    String tmpTableName = schemaAndTableName.getTable_name();
                    String name = tmpTableName + fileName;
                    System.out.println(tableName + "." + tmpTableName + "," + name);
                }
            } else {
                String name = tableName + fileName;
                System.out.println(tableName + "," + name);
            }
        } else {
            otherDb.setDriver(driver.getDriverClassName());
            otherDb.setUrl(args[5]);
            otherDb.setUsername(args[6]);
            otherDb.setPassword(args[7]);
            // 创建 dblink
            String linkName = dbType.toUpperCase() + "_LINK_TEST";
            otherDb.setTableName(args[8]);
            otherDb.setDbType(driver);
            CreateTable createTable = new CreateTable();
            createTable.createTableInOtherDatabase(linkoopdb, otherDb, linkName);
        }
    }
}
