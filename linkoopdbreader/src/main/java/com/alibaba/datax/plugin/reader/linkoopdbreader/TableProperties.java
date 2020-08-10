package com.alibaba.datax.plugin.reader.linkoopdbreader;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author: zzp
 * @date 2020/7/21 11:07
 * @description:
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class TableProperties {
    private String name;
    private String value;
}
