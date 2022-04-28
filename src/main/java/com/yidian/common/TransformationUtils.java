package com.yidian.common;



import com.yidian.data.VertexConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.util.List;


public class TransformationUtils {


    public static DataStream<Tuple2<String, byte[]>> transformation(Object o, DataStream<Tuple2<String, byte[]>> stream, VertexConfig config) throws Exception {
        if (StringUtils.isNoneBlank(config.getSideOutTag())) {
            OutputTag<Tuple2<String, byte[]>> tuple2OutputTag = new OutputTag<Tuple2<String, byte[]>>(config.getSideOutTag()){};
            stream = ((SingleOutputStreamOperator<Tuple2<String, byte[]>>) stream).getSideOutput(tuple2OutputTag);
        }
        if (o instanceof KeyedProcessFunction) {
            return stream.keyBy(s -> s.f0).process((KeyedProcessFunction<String, Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o)
                    .name(config.getName())
                    .setParallelism(config.getParallelism());
        } else if (o instanceof ProcessWindowFunction) {
            return stream.keyBy(s -> s.f0)
                    .window(TumblingProcessingTimeWindows.of(Time.milliseconds(config.getWindowSize())))
                    .process((ProcessWindowFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, String, TimeWindow>) o)
                    .name(config.getName())
                    .setParallelism(config.getParallelism());
        } else if (o instanceof ProcessFunction) {
            return stream.process((ProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o)
                    .name(config.getName())
                    .setParallelism(config.getParallelism());
        } else if (o instanceof RichFlatMapFunction) {
            if (config.isKeyed()) {
                stream = stream.keyBy(f -> f.f0);
            }
            return stream.flatMap((RichFlatMapFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o)
                    .name(config.getName())
                    .setParallelism(config.getParallelism());
        } else if (o instanceof RichMapFunction) {
            if (config.isKeyed()) {
                stream = stream.keyBy(f -> f.f0);
            }
            return stream.map((RichMapFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o)
                    .name(config.getName())
                    .setParallelism(config.getParallelism());
        } else if (o instanceof SinkFunction) {
            stream.addSink((SinkFunction<Tuple2<String, byte[]>>) o)
                    .name(config.getName())
                    .setParallelism(config.getParallelism());
            return null;
        }


        throw new RuntimeException();
    }

    public static DataStream<Tuple2<String, byte[]>> transformation(Class<? extends Object> aClass, VertexConfig config, List<DataStream<Tuple2<String, byte[]>>> stream) throws Exception {
        Object o = aClass.newInstance();
        String connectType = config.getConnectType();
        boolean keyed = config.isKeyed();
        if (stream.size() == 1) {
            return transformation(o, stream.get(0), config);
        } else if ("CONNECT".equalsIgnoreCase(connectType) || o instanceof CoProcessFunction) {
            DataStream<Tuple2<String, byte[]>> tuple2DataStream1 = stream.get(0);
            DataStream<Tuple2<String, byte[]>> tuple2DataStream2 = stream.get(1);
            return tuple2DataStream1
                    .connect(tuple2DataStream2)
                    .process((CoProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o).name(config.getName())
                    .setParallelism(config.getParallelism());

        } else if (("CONNECT".equalsIgnoreCase(connectType) && keyed) || o instanceof KeyedCoProcessFunction) {
            DataStream<Tuple2<String, byte[]>> tuple2DataStream1 = stream.get(0);
            DataStream<Tuple2<String, byte[]>> tuple2DataStream2 = stream.get(1);
            return tuple2DataStream1.keyBy(s -> s.f0)
                    .connect(tuple2DataStream2.keyBy(s -> s.f0))
                    .process((KeyedCoProcessFunction<String, Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o).name(config.getName())
                    .setParallelism(config.getParallelism());

        } else if ("COGROUP".equalsIgnoreCase(connectType) || o instanceof CoGroupFunction) {
            DataStream<Tuple2<String, byte[]>> tuple2DataStream1 = stream.get(0);
            DataStream<Tuple2<String, byte[]>> tuple2DataStream2 = stream.get(1);
            return tuple2DataStream1
                    .coGroup(tuple2DataStream2)
                    .where(s -> s.f0)
                    .equalTo(s -> s.f0)
                    .window(TumblingProcessingTimeWindows.of(Time.milliseconds(config.getWindowSize())))
                    .apply((CoGroupFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o);

        } else if ("JOIN".equalsIgnoreCase(connectType) || o instanceof FlatJoinFunction) {
            DataStream<Tuple2<String, byte[]>> tuple2DataStream1 = stream.get(0);
            DataStream<Tuple2<String, byte[]>> tuple2DataStream2 = stream.get(1);
            return tuple2DataStream1
                    .join(tuple2DataStream2)
                    .where(s -> s.f0)
                    .equalTo(s -> s.f0)
                    .window(TumblingProcessingTimeWindows.of(Time.milliseconds(config.getWindowSize())))
                    .apply((FlatJoinFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o);

        } else if ("INTERVALJOINO".equalsIgnoreCase(connectType) || o instanceof ProcessJoinFunction) {
            long windowSize = config.getWindowSize();
            DataStream<Tuple2<String, byte[]>> tuple2DataStream1 = stream.get(0);
            DataStream<Tuple2<String, byte[]>> tuple2DataStream2 = stream.get(1);
            return tuple2DataStream1.keyBy(s -> s.f0).intervalJoin(tuple2DataStream2.keyBy(s -> s.f0))
                    .inProcessingTime()
                    .between(Time.milliseconds(windowSize), Time.milliseconds(windowSize))
                    .process((ProcessJoinFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o).name(config.getName())
                    .setParallelism(config.getParallelism());
        } else {
            return transformation(o, stream.stream()
                    .reduce(DataStream::union).get(), config);
        }

    }


    public static DataStream<Tuple2<String, byte[]>> generateSource(Class<? extends Object> aClass, VertexConfig config) throws Exception {
        StreamExecutionEnvironment env = UserProfileContext.getOrCreateContext().getEnvironment();
        Object o = aClass.newInstance();
        return env.addSource((SourceFunction<Tuple2<String, byte[]>>) o);
    }

}
