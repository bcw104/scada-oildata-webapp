/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.service.NetCheckingService;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

/**
 *
 * @author 赵磊 2014-8-14 23:49:22
 */
@Transactional
@Service("netCheckingService")
public class NetCheckingServiceImpl implements NetCheckingService {

    private static final Logger log = LoggerFactory.getLogger(NetCheckingServiceImpl.class);
    @Autowired
    private EndTagService endTagService;
    @Autowired
    private RealtimeDataService realtimeDataService;
    @Inject
    protected Sql2o sql2o;
    
    public Sql2o getSql2o() {
        return sql2o;
    }

    public void setSql2o(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    @Override
    public void netChecking() {
        log.info("开始网络诊断：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        List<Map<String, Object>> list = null;
        try (Connection con = sql2o.open()) {
//            list = con.createQuery("select * from R_NETCHECKING where DEVICETYPE not like 'RTU' and DEVICETYPE not like '传%'")
            list = con.createQuery("select * from R_NETCHECKING")
                    .executeAndFetchTable().asList();
        } catch (Exception e) {
            log.error(e.toString());
        }
        if (list != null) {
            for (Map<String, Object> map : list) {
                try {
                    String code = (String) map.get("relatedcode");
                    String varName = (String) map.get("var_name");
                    String ip = (String) map.get("ipaddress");
                    String type = (String) map.get("devicetype");
                    boolean ok = false;
                    if (type.trim().startsWith("RTU") || type.trim().contains("传感器")) {
                        String s = realtimeDataService.getEndTagVarInfo(code, varName);
                        ok = (s != null && "true".equals(s)) ? true : false;
                    } else if (ip != null && !"".equals(ip.trim())) {
                        ok = isNetOk(ip);
                    } else {
                        continue;
                    }

                    int i = ok ? 1 : 0;

                    String updateSql = "update R_NETCHECKING set status = :STATUS where relatedcode = :CODE and var_name = :NAME";
                    try (Connection con = sql2o.open()) {
                        con.createQuery(updateSql)
                                .addParameter("CODE", code)
                                .addParameter("NAME", varName)
                                .addParameter("STATUS", i)
                                .executeUpdate();
                    } catch (Exception e) {
                        log.error(e.toString());
                    }
                    log.info(code + "——" + varName + "——" + ip + "：" + (ok ? "通" : "不通"));
                } catch (Exception e) {
                }
            }
        }
        log.info("结束网络诊断：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
    }

    private boolean isNetOk(String ip) {
        Runtime runtime = Runtime.getRuntime(); // 获取当前程序的运行进对象
        Process process = null; // 声明处理类对象
        String line = null; // 返回行信息
        InputStream is = null; // 输入流
        InputStreamReader isr = null; // 字节流
        BufferedReader br = null;
        boolean res = false;// 结果
        try {
            process = runtime.exec("ping " + ip); // PING
            is = process.getInputStream(); // 实例化输入流
            isr = new InputStreamReader(is);// 把输入流转换成字节流
            br = new BufferedReader(isr);// 从字节中读取文本
            while ((line = br.readLine()) != null) {
                if (line.contains("TTL")) {
                    res = true;
                    break;
                }
            }
            is.close();
            isr.close();
            br.close();
        } catch (IOException e) {
            System.out.println(e);
            runtime.exit(1);
        }
        return res;
    }
}