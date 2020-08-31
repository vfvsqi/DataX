package com.alibaba.datax.plugin.linkoopdb.entity;

public class SchemaAndTableName {
    public String getTable_schem() {
        return table_schem;
    }

    public void setTable_schem(String table_schem) {
        this.table_schem = table_schem;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    @Override
    public String toString() {
        return "SchemaAndTableName{" +
                "table_schema='" + table_schem + '\'' +
                ", table_name='" + table_name + '\'' +
                "}\n";
    }

    private String table_schem;
    private String table_name;
}
