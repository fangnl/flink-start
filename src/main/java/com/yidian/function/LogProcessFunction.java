package com.yidian.function;

import com.yidian.annatations.Operator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


@Operator(name = "log.process",  id = "2", childId ={"4","5"})
public class LogProcessFunction extends ProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>> {

    @Override
    public void processElement(Tuple2<String, byte[]> bytes, ProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>>.Context context, Collector<Tuple2<String, byte[]>> collector) throws Exception {
        collector.collect(bytes);
        context.output(new OutputTag<Tuple2<String, byte[]>>("12"){},bytes);

    }

}
