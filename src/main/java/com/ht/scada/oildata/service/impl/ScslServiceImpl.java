/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.Scheduler;
import com.ht.scada.oildata.service.ScslService;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import javax.inject.Inject;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;

/**
 *
 * @author 赵磊 2014-8-14 23:49:22
 */
@Transactional
@Service("scslService")
public class ScslServiceImpl implements ScslService {

    private static final Logger log = LoggerFactory.getLogger(ScslServiceImpl.class);
    @Autowired
    private RealtimeDataService realtimeDataService;
    @Inject
    protected Sql2o sql2o;
    public void setSql2o(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    @Override
    public void calcOilWellScsj(Calendar c) {
        log.info("开始计算油井运行时间：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        if (Scheduler.youJingList != null && Scheduler.youJingList.size() > 0) {
            Map<String, String> yxMap = realtimeDataService.getEndTagVarInfo(Scheduler.youCodeList, VarSubTypeEnum.YOU_JING_YUN_XING.toString().toLowerCase());
//            String sql = "Insert into T_Oil_Well_Calc_Data"
//                    + "(ID, CODE, MINITE, IS_ON, LRSJ) "
//                    + "values (:ID, :CODE, :MINITE, :ISON, :LRSJ)";

            String updateSql = "update T_Oil_Well_Calc_Data set IS_ON = :ISON ,LRSJ = :LRSJ "
                    + "where CODE=:CODE and MINITE=:MINITE ";

            int minite = c.get(Calendar.MINUTE);
            if (c.get(Calendar.HOUR_OF_DAY) % 2 != 0) {
                minite += 60;
            }

            try (Connection con = sql2o.beginTransaction()) {
                Query query = con.createQuery(updateSql);

                for (EndTag youJing : Scheduler.youJingList) {
                    String code = youJing.getCode();
                    Integer isOn = yxMap.get(code) == null ? null : (Boolean.valueOf(yxMap.get(code)) ? 1 : 0);

                    query.addParameter("CODE", code)
                            .addParameter("MINITE", String.valueOf(minite))
                            .addParameter("ISON", isOn)
                            .addParameter("LRSJ", new Date())
                            .addToBatch();
                }
                query.executeBatch();
                con.commit();
                log.info("计算完:" + minite);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        log.info("完成计算油井运行时间：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public void calcWaterWellScsj(Calendar c) {
        log.info("开始计算水井运行时间：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        if (Scheduler.shuiJingList != null && Scheduler.shuiJingList.size() > 0) {
//            String sql = "Insert into T_Water_Well_Calc_Data"
//                    + "(ID, CODE, MINITE, IS_ON, LRSJ) "
//                    + "values (:ID, :CODE, :MINITE, :ISON, :LRSJ)";

            String updateSql = "update T_Water_Well_Calc_Data set IS_ON = :ISON ,LRSJ = :LRSJ "
                    + "where CODE=:CODE and MINITE=:MINITE ";

            int minite = c.get(Calendar.MINUTE);
            if (c.get(Calendar.HOUR_OF_DAY) % 2 != 0) {
                minite += 60;
            }

            try (Connection con = sql2o.beginTransaction()) {
                Query query = con.createQuery(updateSql);
                for (EndTag shuiJing : Scheduler.shuiJingList) {
                    //水井状态
                    Integer isOn = null;
                    String extConfigInfo = shuiJing.getExtConfigInfo();		// 获得扩展信息 
                    if (extConfigInfo != null && !"".equals(extConfigInfo.trim())) {
                        String[] framesLine = extConfigInfo.trim().replaceAll("\\r", "").split("\\n");// 替换字符串									
                        for (String varName : framesLine) {
                            if (varName.contains("yx|")) {
                                String varNames[] = varName.trim().split("\\|");
                                String varName1 = varNames[1];
                                String codeName = varNames[2];
                                String varNameStr = varNames[3];
//                                if (varName1.contains("shll")) { //瞬时流量
//                                    String ssllValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
//                                    if (ssllValue != null) {
//                                        isOn = Float.valueOf(ssllValue)>0.1 ? 1 : 0;
//                                    }
//                                    break;
//                                }
                                if (varName1.contains("fmqg")) { //阀门状态
                                    String fmqgValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                    if (fmqgValue != null) {
                                        if ("true".equals(fmqgValue)) {
                                            isOn = 0;
                                        } else {
                                            isOn = 1;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    query.addParameter("CODE", shuiJing.getCode())
                            .addParameter("MINITE", String.valueOf(minite))
                            .addParameter("ISON", isOn)
                            .addParameter("LRSJ", new Date())
                            .addToBatch();
                }
                query.executeBatch();
                con.commit();
                log.info("计算完:" + minite);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        log.info("完成计算水井运行时间：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
    }
}
