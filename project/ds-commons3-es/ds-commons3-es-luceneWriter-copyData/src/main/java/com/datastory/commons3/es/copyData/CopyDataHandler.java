package com.datastory.commons3.es.copyData;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * com.datastory.commons3.es.copyData.ResourceHandler
 *
 * @author zhaozhen
 * @since 2017/6/22
 */


public class CopyDataHandler extends AbstractHandler {
    private String esHost;
    private int port;
    private String cluster_name;
    private String index;
    private String hdfs_data_root;
    private String dir;

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        System.out.println(target);
        response.setContentType("text/html; charset=utf-8");
        request.setCharacterEncoding("utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
        PrintWriter out = response.getWriter();
        if (target.equals("/docopy")) {
            esHost = request.getParameter("host");
            port = new Integer(request.getParameter("port"));
            cluster_name = request.getParameter("cluster_name");
            index = request.getParameter("index");
            hdfs_data_root = request.getParameter("hdfs");
            dir = request.getParameter("dir");
            try {
                out.print("[COPYING] Copying hdfs data to local");
                HttpWorkNode.doWork(esHost, port, cluster_name, index, hdfs_data_root, dir);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            out.print("<h3>请求失败，请重试！</h3>");
        }

    }
}
