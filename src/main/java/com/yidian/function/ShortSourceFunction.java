package com.yidian.function;


import com.yidian.annatations.Operator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.security.SecureRandom;
import java.util.List;

@Operator(name = "source",id = "0" ,para = 3,childId = {"1","2"})
public class ShortSourceFunction extends RichParallelSourceFunction<Tuple2<String,byte []>> {
  private boolean cancel = true;
  private static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static SecureRandom rnd = new SecureRandom();
  private static List<String> list = Lists.newArrayList("1", "2", "3");


  @Override
  public void run(SourceContext<Tuple2<String,byte []>> sourceContext) throws Exception {
    while (true) {
        sourceContext.collect(Tuple2.of(randomString(),randomString().getBytes()));
         Thread.sleep(4000);
    }
  }


  public static String randomString() {
    StringBuilder sb = new StringBuilder(10);
    for (int i = 0; i < 10; i++)
      sb.append(AB.charAt(rnd.nextInt(AB.length())));
    return sb.toString();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public void close() throws Exception {
    super.close();
  }

  @Override
  public void cancel() {
    cancel = false;
  }
}
