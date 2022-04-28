package com.yidian.dag.test;

import com.yidian.dag.Dag;
import com.yidian.dag.Vertex;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class TestDag {
  static StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
  static HashMap<String, DataStream<String>> map=new HashMap<>();
  public static void main(String[] args) throws Exception {
    Dag<String> dag = new Dag<>();
    dag.addEdge("1", "2");
    dag.addEdge("0", "3");
    dag.addEdge("2", "3");
    dag.addEdge("3", "5");
    dag.addEdge("3", "6");
    dag.addEdge("3", "7");
    dag.addEdge("7", "8");
    dag.addEdge("8", "9");


    dag.addEdge("5", "9");
    dag.addEdge("6", "9");

    dag.addEdge("1", "4");
    dag.addEdge("4", "3");
    dag.addEdge("3", "10");
    //最后一个节点
    List<Vertex<String>> allLeaf = dag.getAllLeaf();
    allLeaf.forEach(TestDag::topologyAnalyze);
    System.out.println(env.getExecutionPlan());
    env.execute();
  }

  public static DataStreamSource<String> generateSource(Vertex<String> vertex) {
    return env.addSource(new SocketTextStreamFunction("localhost",8080,",",2)).setParallelism(1);
  }

  public static DataStream<String> handlerFunction(Vertex<String> vertex, DataStream<String> stream) {
    SingleOutputStreamOperator<String> process = stream.shuffle().process(new ProcessFunction<String, String>() {
      @Override
      public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        out.collect(value);
      }
    });
    return process.setParallelism(1).disableChaining().shuffle();

  }

  public static void addSinks(Vertex<String> vertex, DataStream<String> reduce) {
    reduce.print().setParallelism(1);

  }

  public static void topologyAnalyze(Vertex<String> vertex) {
    if (map.containsKey(vertex.getLabel())) {
      return;
    }
    if (vertex.isRoot()) {
      map.put(vertex.getLabel(), generateSource(vertex));
      return;
    }
    List<Vertex<String>> parents = vertex.getParents();
    for (Vertex<String> parent : parents) {
      topologyAnalyze(parent);
    }

    Optional<DataStream<String>> reduce =
        parents.stream()
            .map(Vertex::getLabel)
            .map(map::get)
            .reduce(DataStream::union);


    if (!vertex.isRoot() && !vertex.isLeaf()) {
      map.put(vertex.getLabel(), handlerFunction(vertex, reduce.get()));
    }
    if (vertex.isLeaf()) {
      addSinks(vertex, reduce.get());
    }
  }
}
