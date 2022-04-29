package com.yidian.common;

import com.alibaba.fastjson.JSON;
import com.typesafe.config.Config;

import com.yidian.annatations.Operator;
import com.yidian.annatations.Window;
import com.yidian.dag.Dag;
import com.yidian.dag.Vertex;
import com.yidian.data.VertexConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.stream.Collectors;

public class UserProfileContext {
    private final StreamExecutionEnvironment executionEnvironment;
    Dag<VertexConfig> dag = new Dag<>();
    public  HashMap<VertexConfig, DataStream<Tuple2<String, byte[]>>> streamMap;
    public  static Map<String, Tuple2<VertexConfig, Class<? extends Object>>> configMap;


    private UserProfileContext() {
        Configuration configuration = new Configuration();
        configuration.setBoolean("rest.flamegraph.enabled", true);
        executionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        configMap = new HashMap<>();
        streamMap = new HashMap<>();
    }

    public static UserProfileContext getOrCreateContext() {
        return Holder.context;
    }

    private static class Holder {
        private static final UserProfileContext context = new UserProfileContext();
    }


    public StreamExecutionEnvironment getEnvironment() {
        return executionEnvironment;
    }


    public void generateVertexConfig(Annotation annotation, VertexConfig vertexConfig) {
        if (annotation instanceof Operator) {
            Operator operator = (Operator) annotation;
            String operatorName = operator.name();
            int para = operator.para();
            String[] childIds = operator.childId();
            boolean keyed = operator.keyed();
            String id = operator.id();
            String operatorType = operator.operatorType();
            String tag = operator.sideOutTag();

            vertexConfig.setSideOutTag(tag);
            vertexConfig.setId(id);
            vertexConfig.setOperatorType(operatorType);
            vertexConfig.setKeyed(keyed);
            vertexConfig.setChildId(childIds);
            vertexConfig.setParallelism(para);
            vertexConfig.setName(operatorName);
        } else if (annotation instanceof Window) {
            Window window = (Window) annotation;
            String type = window.type();
            vertexConfig.setWindowEnable(true);
            vertexConfig.setWindowSize(window.time());
            vertexConfig.setWindowType(type);
        }
    }


    public void run(String basePath) throws Exception {
        Reflections reflections = new Reflections(basePath, new TypeAnnotationsScanner().filterResultsBy(s -> true));
        Set<Class<?>> typesAnnotatedWith = reflections.getTypesAnnotatedWith(Operator.class, true);
        for (Class<?> functionClass : typesAnnotatedWith) {
            VertexConfig vertexConfig = VertexConfig.create();
            Annotation[] annotations = functionClass.getAnnotations();
            HashSet<Annotation> annotationSet = Sets.newHashSet(annotations);
            annotationSet.forEach(a -> generateVertexConfig(a, vertexConfig));
            configMap.put(vertexConfig.getId(), new Tuple2<>(vertexConfig, functionClass));
        }
        dAGScheduler();
    }



    public void run(Class<? extends Object> baseClass) throws Exception {
        run(baseClass.getPackage().getName());
    }

    public void run(Config config) throws Exception {
        List<? extends Config> operators = config.getConfigList("operators");
        for (Config operatorConfig : operators) {
            String json = JSON.toJSONString(operatorConfig.getAnyRef("operator"));
            VertexConfig vertexConfig = JSON.parseObject(json, VertexConfig.class);
            String functionClass = vertexConfig.getFunctionClass();
            configMap.put(vertexConfig.getId(), new Tuple2<>(vertexConfig, Class.forName(functionClass)));

        }

        dAGScheduler();
    }



    public void dAGScheduler() throws Exception {
        for (Tuple2<VertexConfig, Class<?>> value : configMap.values()) {
            String[] childIds = value.f0.getChildId();
            for (String childId : childIds) {
                Tuple2<VertexConfig, Class<?>> tuple2 = configMap.get(childId);
                dag.addEdge(value.f0, tuple2.f0);
            }
        }

        for (Vertex<VertexConfig> vertex : dag.getAllLeaf()) {
            topologyAnalyze(vertex);
        }
    }

    /**
     * 把所有 leafVertex传入
     *
     * @param vertex
     */
    public void topologyAnalyze(Vertex<VertexConfig> vertex) throws Exception {
        if (streamMap.containsKey(vertex.getLabel())) {
            return;
        }
        Class<?> f1 = configMap.get(vertex.getLabel().getId()).f1;
        if (vertex.isRoot()) {
            DataStream<Tuple2<String, byte[]>> dataStream = TransformationUtils.generateSource(f1, vertex.getLabel());
            streamMap.put(vertex.getLabel(), dataStream);
            return;
        }
        List<Vertex<VertexConfig>> parents = vertex.getParents();
        for (Vertex<VertexConfig> parent : parents) {
            topologyAnalyze(parent);
        }


        List<DataStream<Tuple2<String, byte[]>>> parentsList =
                parents.stream()
                        .map(Vertex::getLabel)
                        .map(streamMap::get).collect(Collectors.toList());


        DataStream<Tuple2<String, byte[]>> transformation = null;
        try {
            transformation = TransformationUtils.transformation(f1, vertex.getLabel(), parentsList);

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (!vertex.isRoot() && !vertex.isLeaf()) {
            streamMap.put(vertex.getLabel(), transformation);
        }
    }

    public void setGlobalConfig() {
        Map<String, String> globalConfig = configMap.values().stream()
                .map(t -> t.f0).
                collect(Collectors.toMap(VertexConfig::getId, JSON::toJSONString));
        ParameterTool parameterTool = ParameterTool.fromMap(globalConfig);
        getEnvironment().getConfig().setGlobalJobParameters(parameterTool);
    }




    public void execute() throws Exception {
        setGlobalConfig();
        executionEnvironment.execute();
    }


}
