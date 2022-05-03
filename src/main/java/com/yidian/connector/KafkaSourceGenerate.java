package com.yidian.connector;

import com.yidian.data.VertexConfig;
import com.yidian.serde.ByteSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.Properties;


public class KafkaSourceGenerate {
public static FlinkKafkaConsumer<byte[]> create(VertexConfig config){
  Properties properties = config.getProperties();
  String topics = properties.getProperty("topic");
  String[] split = topics.split(",");
  return new FlinkKafkaConsumer<>(Arrays.asList(split),ByteSerializationSchema.create(),properties);
}
}
