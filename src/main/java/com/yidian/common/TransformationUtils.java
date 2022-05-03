package com.yidian.common;


import com.yidian.data.TupleFactory;
import com.yidian.data.VertexConfig;
import com.yidian.serde.ByteSerializationSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class  TransformationUtils {


  public static DataStream<Tuple2<String, byte[]>> transformation(Object o, DataStream<Tuple2<String, byte[]>> stream, VertexConfig config) throws Exception {
    if (StringUtils.isNoneBlank(config.getSideOutTag())) {
      OutputTag<Tuple2<String, byte[]>> tuple2OutputTag = new OutputTag<Tuple2<String, byte[]>>(config.getSideOutTag()) {
      };
      stream = ((SingleOutputStreamOperator<Tuple2<String, byte[]>>) stream).getSideOutput(tuple2OutputTag);
    }
    if (o instanceof KeyedProcessFunction) {
      @SuppressWarnings("unchecked")
      KeyedProcessFunction<String, Tuple2<String, byte[]>, Tuple2<String, byte[]>> keyedProcessFunction =
          (KeyedProcessFunction<String, Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o;
      return stream.keyBy(s -> s.f0).process(keyedProcessFunction)
          .name(config.getName())
          .setParallelism(config.getParallelism());
    } else if (o instanceof ProcessWindowFunction) {
      @SuppressWarnings("unchecked")
      ProcessWindowFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, String, TimeWindow> windowFunction =
          (ProcessWindowFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, String, TimeWindow>) o;
      WindowAssigner<Object, TimeWindow> windowAssigner = getWindowAssigner(config);
      WindowedStream<Tuple2<String, byte[]>, String, TimeWindow> window = stream.keyBy(s -> s.f0)
          .window(windowAssigner);
      return stream.keyBy(s -> s.f0)
          .window(windowAssigner)

          .process(windowFunction)
          .name(config.getName())
          .setParallelism(config.getParallelism());
    } else if (o instanceof ProcessFunction) {
      @SuppressWarnings("unchecked")
      ProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>> processFunction =
          (ProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o;
      return stream.process(processFunction)
          .name(config.getName())
          .setParallelism(config.getParallelism());
    } else if (o instanceof RichFlatMapFunction) {
      if (config.isKeyed()) {
        stream = stream.keyBy(f -> f.f0);
      }
      @SuppressWarnings("unchecked")
      RichFlatMapFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>> flatMapFunction =
          (RichFlatMapFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o;
      return stream.flatMap(flatMapFunction)
          .name(config.getName())
          .setParallelism(config.getParallelism());
    } else if (o instanceof RichMapFunction) {
      if (config.isKeyed()) {
        stream = stream.keyBy(f -> f.f0);
      }
      @SuppressWarnings("unchecked")
      RichMapFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>> mapFunction =
          (RichMapFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o;
      return stream.map(mapFunction)
          .name(config.getName())
          .setParallelism(config.getParallelism());
    } else if (o instanceof SinkFunction) {
      @SuppressWarnings("unchecked")
      SinkFunction<Tuple2<String, byte[]>> sinkFunction =
          (SinkFunction<Tuple2<String, byte[]>>) o;
      stream.addSink(sinkFunction)
          .name(config.getName())
          .setParallelism(config.getParallelism());
      return null;
    }
    throw new RuntimeException();
  }

  public static DataStream<Tuple2<String, byte[]>> transformation(Class<? extends Object> aClass, VertexConfig config, List<DataStream<Tuple2<String, byte[]>>> stream) throws Exception {
    Object o = aClass.getDeclaredConstructor().newInstance();

    String connectType = config.getConnectType();
    boolean keyed = config.isKeyed();
    if (stream.size() == 1) {
      return transformation(o, stream.get(0), config);
    } else if ("CONNECT".equalsIgnoreCase(connectType) || o instanceof CoProcessFunction) {
      DataStream<Tuple2<String, byte[]>> tuple2DataStream1 = stream.get(0);
      DataStream<Tuple2<String, byte[]>> tuple2DataStream2 = stream.get(1);
      @SuppressWarnings("unchecked")
      CoProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>> coProcessFunction =
          (CoProcessFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o;
      return tuple2DataStream1
          .connect(tuple2DataStream2)
          .process(coProcessFunction)
          .name(config.getName())
          .setParallelism(config.getParallelism());

    } else if (("CONNECT".equalsIgnoreCase(connectType) && keyed) || o instanceof KeyedCoProcessFunction) {
      DataStream<Tuple2<String, byte[]>> tuple2DataStream1 = stream.get(0);
      DataStream<Tuple2<String, byte[]>> tuple2DataStream2 = stream.get(1);
      @SuppressWarnings("unchecked")
      KeyedCoProcessFunction<String, Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>> keyedCoProcessFunction =
          (KeyedCoProcessFunction<String, Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o;
      return tuple2DataStream1.keyBy(s -> s.f0)
          .connect(tuple2DataStream2.keyBy(s -> s.f0))
          .process(keyedCoProcessFunction)
          .name(config.getName())
          .setParallelism(config.getParallelism());

    } else if ("COGROUP".equalsIgnoreCase(connectType) || o instanceof CoGroupFunction) {
      DataStream<Tuple2<String, byte[]>> tuple2DataStream1 = stream.get(0);
      DataStream<Tuple2<String, byte[]>> tuple2DataStream2 = stream.get(1);
      @SuppressWarnings("unchecked")
      CoGroupFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>> coGroupFunction =
          (CoGroupFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o;
      return tuple2DataStream1
          .coGroup(tuple2DataStream2)
          .where(s -> s.f0)
          .equalTo(s -> s.f0)
          .window(TumblingProcessingTimeWindows.of(Time.milliseconds(config.getWindowSize())))
          .apply(coGroupFunction);

    } else if ("JOIN".equalsIgnoreCase(connectType) || o instanceof FlatJoinFunction) {
      DataStream<Tuple2<String, byte[]>> tuple2DataStream1 = stream.get(0);
      DataStream<Tuple2<String, byte[]>> tuple2DataStream2 = stream.get(1);
      @SuppressWarnings("unchecked")
      FlatJoinFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>> flatJoinFunction =
          (FlatJoinFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o;
      WindowAssigner<Object, TimeWindow> of = TumblingProcessingTimeWindows.of(Time.milliseconds(config.getWindowSize()));
      return tuple2DataStream1
          .join(tuple2DataStream2)
          .where(s -> s.f0)
          .equalTo(s -> s.f0)
          .window(TumblingProcessingTimeWindows.of(Time.milliseconds(config.getWindowSize())))
          .apply(flatJoinFunction);
    } else if ("INTERVALJOINO".equalsIgnoreCase(connectType) || o instanceof ProcessJoinFunction) {
      long windowSize = config.getWindowSize();
      DataStream<Tuple2<String, byte[]>> tuple2DataStream1 = stream.get(0);
      DataStream<Tuple2<String, byte[]>> tuple2DataStream2 = stream.get(1);
      @SuppressWarnings("unchecked")
      ProcessJoinFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>> processJoinFunction =
          (ProcessJoinFunction<Tuple2<String, byte[]>, Tuple2<String, byte[]>, Tuple2<String, byte[]>>) o;
      return tuple2DataStream1
          .keyBy(s -> s.f0)
          .intervalJoin(tuple2DataStream2.keyBy(s -> s.f0))
          .between(Time.milliseconds(windowSize), Time.milliseconds(windowSize))
          .process(processJoinFunction)
          .name(config.getName())
          .setParallelism(config.getParallelism());
    } else {
      return transformation(o, stream.stream()
          .reduce(DataStream::union).get(), config);
    }

  }

  public static WindowAssigner<Object, TimeWindow> getWindowAssigner(VertexConfig config) {

    String windowType = config.getWindowType();
    long windowSize = config.getWindowSize();

    Time windowSizeTime = Time.milliseconds(windowSize);
    long windowSlidSize = config.getWindowSlidSize();
    if (windowSlidSize <= 0) {
      windowSlidSize = windowSize;
    }
    Time windowSlidSizeTime = Time.milliseconds(windowSlidSize);
    if ("Tumbling".equalsIgnoreCase(windowType)) {
      return TumblingProcessingTimeWindows.of(windowSizeTime);
    }

    if ("Session".equalsIgnoreCase(windowType)) {
      return ProcessingTimeSessionWindows.withGap(windowSizeTime);
    }


    if ("SlidingEvent".equalsIgnoreCase(windowType)) {
      return SlidingEventTimeWindows.of(windowSizeTime, windowSlidSizeTime);
    }

    if ("TumblingEvent".equalsIgnoreCase(windowType)) {
      return TumblingEventTimeWindows.of(windowSizeTime);
    }

    if ("SessionEvent".equalsIgnoreCase(windowType)) {
      return EventTimeSessionWindows.withGap(windowSizeTime);
    }

    return SlidingProcessingTimeWindows.of(windowSizeTime, windowSlidSizeTime);

  }


  public static DataStream<Tuple2<String, byte[]>> generateSource(Class<? extends Object> aClass, VertexConfig config) throws Exception {
    StreamExecutionEnvironment env = UserProfileContext.getOrCreateContext().getEnvironment();
    String sourceType = config.getSourceType();
    if ("kafka".equalsIgnoreCase(sourceType)) {
      Properties properties = config.getProperties();
      String topics = properties.getProperty("topics");
      List<String> topicList = Arrays.asList(topics.split(","));
      properties.remove("topics");
      properties.remove("id");
      FlinkKafkaConsumer<byte[]> flinkKafkaConsumer =
          new FlinkKafkaConsumer<>(topicList, ByteSerializationSchema.create(), properties);
      return  env.addSource(flinkKafkaConsumer)
          .name(config.getName())
          .setParallelism(config.getParallelism())
          .map(TupleFactory::createTuple);
    }
    Object o = aClass.getDeclaredConstructor().newInstance();
    @SuppressWarnings("unchecked")
    SourceFunction<Tuple2<String, byte[]>> sourceFunction = (SourceFunction<Tuple2<String, byte[]>>) o;
    return env.addSource(sourceFunction)
        .setParallelism(config.getParallelism())
        .name(config.getName());
  }


}
