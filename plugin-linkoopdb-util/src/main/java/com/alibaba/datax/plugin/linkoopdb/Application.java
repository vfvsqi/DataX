package com.alibaba.datax.plugin.linkoopdb;

import com.alibaba.datax.plugin.linkoopdb.entity.DBConnectInfo;
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
//        args[0] = "jdbc:linkoopdb:tcp://test91:9105/ldb;query_iterator=1";
//        args[1] = "admin";
//        args[2] = "123456";
//        args[3] = "TEST.BACKUP";
////        args[4] = "jdbc:mysql://192.168.1.72:3306/tools";
////        args[5] = "test";
////        args[6] = "123456";
////        args[7] = "mysql";
////        args[8] = "DATAX_INSERT_TEST1";
//
//        args[4] = "jdbc:mysql://192.168.1.93:3306/testtrans";
//        args[5] = "ldb";
//        args[6] = "123456";
//        args[7] = "mysql";
//        args[8] = "DATAX_INSERT_TEST1";


//        jdbc:postgresql://192.168.1.72:5432/postgres postgres 123456

        if (args.length != 9) {
            String ret = "入参有误，入参应按照 linkoopdb url,username,password,table, 异构数据库 url,user,pass,类型,table 这个顺序来";
            throw new Exception(ret);
        }

        DBConnectInfo linkoopdb = new DBConnectInfo();
        DBConnectInfo otherDb = new DBConnectInfo();
        linkoopdb.setUrl(args[0]);
        linkoopdb.setUsername(args[1]);
        linkoopdb.setPassword(args[2]);
        linkoopdb.setTableName(args[3]);
        linkoopdb.setDriver(TransEnums.DataBaseType.Linkoopdb.getDriverClassName());

        otherDb.setUrl(args[4]);
        otherDb.setUsername(args[5]);
        otherDb.setPassword(args[6]);
        String dbType = args[7];
        otherDb.setDriver(TransEnums.DataBaseType.get(dbType).getDriverClassName());
        // 创建 dblink
        String linkName = dbType.toUpperCase() + "_LINK_TEST";
        otherDb.setTableName(args[8]);
        LinkoopDBUtils.init(linkoopdb);

        CreateTable createTable = new CreateTable();
        createTable.createTableInOtherDatabase(linkoopdb, otherDb, linkName);

    }

}
