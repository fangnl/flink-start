package com.yidian.function;


import com.yidian.annatations.Operator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


@Operator(name = "log.source", operatorType = "file" ,id="4" ,para = 2)
public class FileSink extends RichSinkFunction<Tuple2<String,byte []>> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }


    @Override
    public void invoke(Tuple2<String,byte[]> value, Context context) throws Exception {
        System.out.println(value);
    }
}
