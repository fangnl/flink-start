package com.yidian.function;

import com.yidian.annatations.Operator;
import com.yidian.annatations.Window;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
@Window(time = 8000)
@Operator(name = "log.mapping.window", keyed = true,  id = "6", childId = "4")
public class TestWindowFunction extends ProcessWindowFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, String, TimeWindow>.Context context, Iterable<Tuple2<String, byte[]>> iterable, Collector<Tuple2<String, byte[]>> collector) throws Exception {


        iterable.forEach(t-> System.out.println(t.f1));

        iterable.forEach(collector::collect);




    }
}
