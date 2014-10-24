package com.ht.scada.oildata;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
/**
 * 
 * @author zhao
 */
public class JettyServer {
    private static final Logger log = LoggerFactory.getLogger(JettyServer.class);
    public static void main(String[] args) throws Exception {
//        log.info("启动Web");

        Server server = new Server(8085);

        WebAppContext context = new WebAppContext("webapp", "/");
        //context.setDescriptor("webapp/WEB-INF/web.xml");
        //JettyServer.class.getResource("")

        //context.setResourceBase(JettyServer.class.getResource("/webapp").toExternalForm());

        if (new File("webapp").exists()) { // 先查找当前目录有没有webapp目录
            context.setResourceBase("webapp");
        } else {
            context.setResourceBase("src/main/webapp");
        }

        context.setParentLoaderPriority(true);
        context.getInitParams().put("org.eclipse.jetty.servlet.Default.useFileMappedBuffer", "false");
        server.setHandler(context);


//		FileInputStream fis = new FileInputStream(new File("./etc/jetty.xml"));
//		XmlConfiguration configuration = new XmlConfiguration(fis);
//		Server server = (Server) configuration.configure();
        log.info("启动定时任务程序……");
        server.start();
        server.join();
    }
}
