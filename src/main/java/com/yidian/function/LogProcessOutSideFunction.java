package com.yidian.function;

import com.yidian.annatations.Operator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
@Operator(childId = "4",sideOutTag = "5",id = "5",para = 2,name = "sideOut")
public class LogProcessOutSideFunction  extends ProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>> {
    @Override
    public void processElement(Tuple2<String, byte[]> stringTuple2, ProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>>.Context context, Collector<Tuple2<String, byte[]>> collector) throws Exception {
        collector.collect(stringTuple2);
    }
}
