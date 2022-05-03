package com.yidian;

import com.yidian.common.UserProfileContext;



public class App {
    public static void main(String[] args) throws Exception {
        UserProfileContext context = UserProfileContext.getOrCreateContext();
//        Config config = ConfigFactory.parseFile(new File("/Users/admin/Downloads/other/flink-start/src/main/resources/application.conf"));
//        context.run(config);
        context.setUserConfigPath("/Users/admin/Desktop/log4j.properties");
        context.run(App.class);
        context.execute();

    }
}
