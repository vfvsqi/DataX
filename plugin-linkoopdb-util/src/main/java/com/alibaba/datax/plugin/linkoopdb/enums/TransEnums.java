package com.alibaba.datax.plugin.linkoopdb.enums;
public class TransEnums {

    public enum DataBaseType {
        /**
         *
         */
        MySql("mysql", "com.mysql.jdbc.Driver"),
        Tddl("mysql", "com.mysql.jdbc.Driver"),
        DRDS("drds", "com.mysql.jdbc.Driver"),
        Oracle("oracle", "oracle.jdbc.OracleDriver"),
        SQLServer("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
        PostgreSQL("postgresql", "org.postgresql.Driver"),
        RDBMS("rdbms", "com.alibaba.datax.plugin.rdbms.util.DataBaseType"),
        DB2("db2", "com.ibm.db2.jcc.DB2Driver"),
        ADS("ads","com.mysql.jdbc.Driver"),
        ClickHouse("clickhouse", "ru.yandex.clickhouse.ClickHouseDriver"),
        Linkoopdb("linkoopdb", "com.datapps.linkoopdb.jdbc.JdbcDriver");

        public static DataBaseType get(String value) {
            if (MySql.getTypeName().equals(value)) {
                return MySql;
            } else if (Oracle.getTypeName().equals(value)) {
                return Oracle;
            } else if (SQLServer.getTypeName().equals(value)) {
                return SQLServer;
            } else if (PostgreSQL.getTypeName().equals(value)) {
                return PostgreSQL;
            } else {
                return null;
            }
        }


        private String typeName;
        private String driverClassName;

        DataBaseType(String typeName, String driverClassName) {
            this.typeName = typeName;
            this.driverClassName = driverClassName;
        }

        public String getDriverClassName() {
            return this.driverClassName;
        }

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }
    }

}
