package com.datastory.commons3.es.copyData;

import org.eclipse.jetty.server.Server;


/**
 * com.datastory.commons3.es.copyData.CopyDataServer
 *
 * @author zhaozhen
 * @since 2017/6/22
 */
public class CopyDataServer {

    public static void main(String[] args) throws Exception

    {
        Server server = new Server(8080);
        // 配置服务
        server.setHandler(new CopyDataHandler());
        server.start();
        server.join();

    }
}
