package com.alibaba.datax.plugin.linkoopdb.dao;

import com.alibaba.datax.plugin.linkoopdb.entity.SchemaAndTableName;
import com.alibaba.datax.plugin.linkoopdb.entity.ShowCreateTable;
import com.alibaba.datax.plugin.linkoopdb.util.LinkoopDBUtils;
import java.util.List;

/**
 * @author: zzp
 * @date 2020/5/15 15:10
 * @description:
 */
public class ShardInfoDao {

    public void createTable(String sql) {
        LinkoopDBUtils.update(sql);
    }

    public ShowCreateTable getShowTable(String tableName) {
        String sql = "show create table " + tableName;
        ShowCreateTable ret = LinkoopDBUtils.getTable(ShowCreateTable.class, sql);
        return ret;
    }

    public List<SchemaAndTableName> getList(String schemaName) {
        String sql = "select TABLE_SCHEM, table_name from information_schema.system_tables";
        List<SchemaAndTableName> ret;
        if (null == schemaName || schemaName.equals("")) {
            ret = LinkoopDBUtils.getList(SchemaAndTableName.class, sql);
        } else {
            sql = sql + " where TABLE_SCHEM = ?";
            ret = LinkoopDBUtils.getList(SchemaAndTableName.class, sql, schemaName);
        }
        return ret;
    }
}
