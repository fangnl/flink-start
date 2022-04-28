package com.yidian;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.yidian.common.UserProfileContext;

import java.io.File;


public class App {
    public static void main(String[] args) throws Exception {
        UserProfileContext context = UserProfileContext.getOrCreateContext();
//        Config config = ConfigFactory.parseFile(new File("/Users/admin/Downloads/other/flink-start/src/main/resources/application.conf"));
//        context.run(config);
        context.run(App.class);
        context.execute();

    }
}
