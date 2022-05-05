package com.yidian.serde;

import com.yidian.data.TupleFactory;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;

/**
 * @author: Sun Wei
 * @date: 2020/3/14 12:07
 * @description:
 */
@Slf4j
@NoArgsConstructor(staticName = "create")
public class ByteSerializationSchema extends AbstractDeserializationSchema<Tuple2<String,byte[]>> implements SerializationSchema<Tuple2<String,byte[]>> {
  private static final long serialVersionUID = 8474518391552876523L;
  @Override
  public byte[] serialize(Tuple2<String,byte[]> value) {
    return value.f1;
  }

  @Override
  public Tuple2<String,byte[]> deserialize(byte[] bytes) throws IOException {
    return TupleFactory.createTuple(bytes);
  }
}
