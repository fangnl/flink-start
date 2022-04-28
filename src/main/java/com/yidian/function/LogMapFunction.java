package com.yidian.function;


import com.yidian.annatations.Operator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
@Operator(name = "log.mapping",keyed = true,operatorType = "FLATMAP",id="1",childId = "4")
public class LogMapFunction extends RichFlatMapFunction<Tuple2<String,byte []>,Tuple2<String,byte []>> {
    @Override
    public void flatMap(Tuple2<String,byte []> s, Collector<Tuple2<String,byte []>> collector) throws Exception {
        collector.collect(s);
    }
}
