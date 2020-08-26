package com.alibaba.datax.plugin.linkoopdb.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author: zzp
 * @date 2020/7/17 15:48
 * @description:
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ShowCreateTable {
    private String table;
    private String create_table;
}
