package com.alibaba.datax.plugin.reader.linkoopdbreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum LinkoopdbReaderErrorCode implements ErrorCode {
    /**
     *
     */
    REQUIRED_VALUE("LinkoopdbReader-00", "缺失必要的值"),
    ILLEGAL_VALUE("LinkoopdbReader-01", "值非法"),
    NOT_SUPPORT_TYPE("LinkoopdbReader-02", "不支持的column类型"),;


    private final String code;
    private final String description;

    private LinkoopdbReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
