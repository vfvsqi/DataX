package com.alibaba.datax.plugin.linkoopdb.entity;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class DBConnectInfo {
    private String url;
    private String username;
    private String password;
    private String driver;
    private String tableName;
}
