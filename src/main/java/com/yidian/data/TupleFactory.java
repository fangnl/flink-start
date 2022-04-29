package com.yidian.data;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

public class TupleFactory {

    public static String defaultKey = StringUtils.EMPTY;

    public static Tuple2<String, byte[]> createTuple(String key, byte[] value) {
        return new Tuple2<>(key, value);
    }


    public static Tuple2<String, byte[]> createTuple(byte[] value) {
        return new Tuple2<>(defaultKey, value);
    }


}
