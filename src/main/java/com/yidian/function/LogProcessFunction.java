package com.yidian.function;

import com.yidian.annatations.Operator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


@Operator(name = "log.process", id = "2", childId = {"5", "1"})
public class LogProcessFunction extends ProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>> {
    OutputTag<Tuple2<String, byte[]>> outputTag;
    OutputTag<Tuple2<String, byte[]>> outputTag5;

    @Override
    public void open(Configuration parameters) throws Exception {

        outputTag = new OutputTag<Tuple2<String, byte[]>>("1") {
        };
        outputTag5 = new OutputTag<Tuple2<String, byte[]>>("5") {
        };

    }

    @Override
    public void processElement(Tuple2<String, byte[]> bytes, ProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>>.Context context, Collector<Tuple2<String, byte[]>> collector) throws Exception {
        if (bytes.f0.hashCode() % 2 == 0)
            context.output(outputTag, bytes);
        else
            context.output(outputTag5, bytes);

    }

}
