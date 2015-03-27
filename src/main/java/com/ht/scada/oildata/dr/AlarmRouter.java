/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.dr;

import com.alibaba.fastjson.JSON;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;

/**
 *
 * @author 赵磊 2015-1-28 17:25:39
 */
public class AlarmRouter {

    private static final Logger log = LoggerFactory.getLogger(AlarmRouter.class);
    @Inject
    protected Sql2o sql2o;
    @Inject
    @Named("sql2o2")
    protected Sql2o sql2o2;
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private Map<String, String> dwdmMap;    //code:233444
    private Map<String, String> bjdmMap;       //i_c:A000
    private Map<String, Map<String, String>> zkMap;  //站库单位代码 z1-3jlz:88888

    @PostConstruct
    public void init() {
        dwdmMap = new HashMap<>();
        bjdmMap = new HashMap<>();
        zkMap = new HashMap<>();
        Connection con = sql2o.open();
        List<String> codeList = con.createQuery("select code from t_end_tag where type = 'YOU_JING'").executeAndFetch(String.class);
        List<Map<String, Object>> bjdmList = con.createQuery("select varname,bjdm from Z_BJDM").executeAndFetchTable().asList();
        List<Map<String, Object>> zkList = con.createQuery("select localcode,remotecode,dwdm from T_ZK_CODE_MAP").executeAndFetchTable().asList();

        if (codeList != null) {
            int i = 0;
            for (String code : codeList) {
                try (Connection con1 = sql2o.open();) {
                    String dwdm = con1.createQuery("select DWDM from ys_dba01@ydk where jh=:CODE and rownum = 1 order by rq desc").addParameter("CODE", code).executeAndFetchFirst(String.class);
                    if (dwdm != null) {
                        dwdmMap.put(code, dwdm);
                    }
                } catch (Exception e) {
                }
            }
        }
        if (bjdmList != null) {
            for (Map<String, Object> map : bjdmList) {
                String varName = (String) map.get("varname");
                String bjdm = (String) map.get("bjdm");
                bjdmMap.put(varName, bjdm);
            }
        }
        if (zkList != null) {
            for (Map<String, Object> map : zkList) {
                String code = (String) map.get("localcode");
                String jh = (String) map.get("remotecode");
                String dwdm = (String) map.get("dwdm");
                Map<String, String> map1 = new HashMap<>();
                map1.put("dwdm", dwdm);
                map1.put("jh", jh);
                zkMap.put(code, map1);
            }
        }
    }

    public void alarmHandle(final String message) {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                String ID, DWDM, BJD, BJDM = null;
                Date BJSJ, JCSJ;
                Float XX1 = null, XX2 = null, XX3 = null, YZ = null;
                System.out.println(message);
                SoeRecord soeRecord = JSON.parseObject(message, SoeRecord.class);
                BJD = soeRecord.getCode();
                ID = soeRecord.getId();
                JCSJ = soeRecord.getResumeTime();
                if (JCSJ != null) {
                    //TODO 更新报警解除时间
                    return;
                }
                BJSJ = soeRecord.getDeviceTime();
                String varName = soeRecord.getName();
                String soeValue = soeRecord.getSoeValue();
                String shresholdValue = soeRecord.getShresholdValue();

                DWDM = dwdmMap.get(BJD);
                if (DWDM != null) {  //油井
                    if (soeValue != null) {
                        String values[] = soeValue.split(";");
                        if (values.length == 1) {
                            XX1 = Float.valueOf(values[0].split(" ")[1].trim());
                        } else if (values.length == 3) {
                            XX1 = Float.valueOf(values[0].split(" ")[1].trim());
                            XX2 = Float.valueOf(values[1].split(" ")[1].trim());
                            XX3 = Float.valueOf(values[2].split(" ")[1].trim());
                        }
                    }
                    if (shresholdValue != null) {
                        String values[] = shresholdValue.split(";");
                        if (values.length == 1) {
                            YZ = Float.valueOf(values[0].split(" ")[1].trim());
                        } else if (values.length == 3) {
                            //暂不处理
                        }
                    }
                } else {//计量站、配水间
                    Map<String, String> map = zkMap.get(BJD);
                    if(map == null) {
                        return;
                    }
                    String jh = map.get("jh");
                    String dwdm = map.get("dwdm");
                    DWDM = dwdm;
                    BJD = jh;
                    if (soeValue != null) {
                        XX1 = Float.valueOf(soeValue);
                    }
                    if (shresholdValue != null) {
                        YZ = Float.valueOf(shresholdValue);
                    }
                }

                String dm = bjdmMap.get(varName);
                if (dm.contains(",")) {
                    String dms[] = dm.split(",");
                    if (XX1 != null && YZ != null) {
                        if (XX1 >= YZ) {
                            BJDM = dms[0];
                        } else {
                            BJDM = dms[1];
                        }
                    }
                } else {
                    BJDM = dm;
                }

                String insertSql = "INSERT INTO qysczh.SYX_BJ_BJXX(ID, DWDM, BJD, BJSJ, BJDM, XX1, XX2, XX3, YZ) "
                        + "VALUES(:ID, :DWDM, :BJD, :BJSJ, :BJDM, :XX1, :XX2, :XX3, :YZ)";
                try (Connection con = sql2o2.open()) {
                    Query query = con.createQuery(insertSql)
                            .addParameter("ID", ID)
                            .addParameter("DWDM", DWDM)
                            .addParameter("BJD", BJD)
                            .addParameter("BJDM", BJDM)
                            .addParameter("BJSJ", BJSJ)
                            .addParameter("XX1", XX1)
                            .addParameter("XX2", XX2)
                            .addParameter("XX3", XX3)
                            .addParameter("YZ", YZ);
                    query.executeUpdate();
                }
            }
        });
    }
}
