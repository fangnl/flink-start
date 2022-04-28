package com.yidian.data;

import lombok.Data;
import lombok.NoArgsConstructor;


import java.util.Objects;
import java.util.Properties;

@NoArgsConstructor(staticName = "create")
@Data
public class VertexConfig {
    private String id;
    private String[] childId = new String[0];

    private String name;
    private int parallelism;

    private boolean keyed;
    private String operatorType;

    private String connectType;

    private boolean windowEnable;
    private String WindowType;
    private long windowSize;
    private String triggerId;

    private String sourceType;
    private String sinkType;
    private String sideOutTag;

    private String triggerClass;
    private String functionClass;

    private Properties properties;

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VertexConfig)) return false;
        VertexConfig otherVertexConf = (VertexConfig) obj;
        return Objects.equals(otherVertexConf.id, id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }


}
