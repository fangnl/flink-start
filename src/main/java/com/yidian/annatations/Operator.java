package com.yidian.annatations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Properties;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.ANNOTATION_TYPE, ElementType.TYPE, ElementType.METHOD})
public @interface Operator {
    String id();

    String[] childId() default {};

    int para() default 1;

    String name() default "";

    boolean keyed() default false;

    String operatorType() default "PROCESS";

    String sideOutTag() default "";

}
