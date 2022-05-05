package com.yidian;

import com.yidian.common.UserProfileContext;



public class App {
    public static void main(String[] args) throws Exception {
        UserProfileContext context = UserProfileContext.getOrCreateContext();
//        Config config = ConfigFactory.parseFile(new File("/Users/admin/Downloads/other/flink-start/src/main/resources/application.conf"));
//        context.run(config);
        if(args.length==1){
            String arg = args[0];
            context.setUserConfigPath(arg);
        }
        context.run(App.class);
        context.execute();

    }
}
