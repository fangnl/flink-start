package com.yidian.serde;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.IOException;

/**
 * @author: Sun Wei
 * @date: 2020/3/14 12:07
 * @description:
 */
@Slf4j
@NoArgsConstructor(staticName = "create")
public class ByteSerializationSchema extends AbstractDeserializationSchema<byte[]> implements SerializationSchema<byte[]> {
  private static final long serialVersionUID = 8474518391552876523L;
  @Override
  public byte[] serialize(byte[] bytes) {
    return bytes;
  }

  @Override
  public byte[] deserialize(byte[] bytes) throws IOException {
    return bytes;
  }
}
