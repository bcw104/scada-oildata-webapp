/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.Scheduler;
import com.ht.scada.oildata.service.WaterWellDataCalcService;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.inject.Inject;
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
@Service("waterWellDataCalcService")
public class WaterWellDataCalcServiceImpl implements WaterWellDataCalcService {

    private static final Logger log = LoggerFactory.getLogger(WaterWellDataCalcServiceImpl.class);
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

    /**
     * 班报任务
     *
     * @author 赵磊
     */
    @Override
    public void runBanBaoTask() {
        log.info("水井班报录入开始——现在时刻：" + CommonUtils.date2String(new Date()));
        if (Scheduler.shuiJingList != null && Scheduler.shuiJingList.size() > 0) {
            for (EndTag shuiJing : Scheduler.shuiJingList) {
                String code = shuiJing.getCode();
                String sql = "insert into T_Water_Well_Hourly_Data "
                        + "(ID, CODE, PSJ, SAVE_TIME, DATE_TIME, YXSJ, LJYXSJ, GY, ZRYL, SSLL, LLSD, RPZL, ZSL, LJZSL, CQL, SJD)"
                        + "values (:ID, :CODE, :PSJ, :SAVE_TIME, :DATE_TIME, :YXSJ, :LJYXSJ, :GY, :ZRYL, :SSLL, :LLSD, :RPZL, :ZSL, :LJZSL, :CQL, :SJD)";
                //Oracle
                String PSJ = shuiJing.getParent() == null ? "—" : shuiJing.getParent().getName();
                //计算
                Float CQL = 0f, ZSL = 0f, LJZSL = 0f;
                //实时库数据
                Float GY = null, ZRYL = null, SSLL = null, LJLL = null, LLSD = null;
                //源头库数据
                Float RPZL = null;
                //程序处理
                String SJD;

                //源头库数据
                Map<String, Object> map = findDataFromYdkByCode(code);
                if (map != null) {
                    RPZL = Float.parseFloat(((BigDecimal) map.get("rpzsl")).toString());
                }

                //实时库数据
                try {
                    String extConfigInfo = shuiJing.getExtConfigInfo();		// 获得扩展信息 
                    if (extConfigInfo != null && !"".equals(extConfigInfo.trim())) {
                        String[] framesLine = extConfigInfo.trim().replaceAll("\\r", "").split("\\n");// 替换字符串									
                        for (String varName : framesLine) {
                            //yc|zsyl-注水压力|psj_z1-10-b|zky12_zsyl 
                            if (varName.contains("yx|") || varName.contains("yc|")) {
                                String varNames[] = varName.trim().split("\\|");
                                String varName1 = varNames[1];
                                String codeName = varNames[2];
                                String varNameStr = varNames[3];
                                if (varName1.contains("zsyl-")) { // 注水压力
                                    String zsylValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                    if (zsylValue != null) {
                                        ZRYL = CommonUtils.formatFloat(Float.parseFloat(zsylValue), 2);
                                    }
                                } else if (varName1.contains("ljll")) { // 累计流量
                                    String ljllValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                    if (ljllValue != null) {
                                        LJLL = CommonUtils.formatFloat(Float.parseFloat(ljllValue), 2);
                                    }
                                } else if (varName1.contains("shll")) { // 瞬时流量
                                    String ssllValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                    if (ssllValue != null) {
                                        SSLL = CommonUtils.formatFloat(Float.parseFloat(ssllValue), 2);
                                    }
                                }
                            }
                        }
                    }

                } catch (Exception e) {
                    log.info(code + ":" + e.toString());
                }

//                if (shuiJing.getParent() != null) {  //干压
//                    GY = getRealData(shuiJing.getParent().getCode(), "gxyl");
//                }
                //获取干线压力
                String extConfigInfo = null;
                if (shuiJing.getParent() != null) {
                    extConfigInfo = shuiJing.getParent().getExtConfigInfo();// 获取配水间值 
                } else {//单井水井
                    extConfigInfo = shuiJing.getExtConfigInfo();// 获取配水间值 
                }
                try {
                    if (extConfigInfo != null && !"".equals(extConfigInfo.trim())) {
                        String[] framesLine = extConfigInfo.trim().replaceAll("\\r", "").split("\\n");// 替换字符串									
                        for (String varName : framesLine) {
                            //yc|zsyl-注水压力|psj_z1-10-b|zky12_zsyl 
                            if (varName.contains("yc|")) {
                                String varNames[] = varName.trim().split("\\|");
                                String varName1 = varNames[1];
                                String codeName = varNames[2];
                                String varNameStr = varNames[3];
                                if (varName1.contains("gxyl-")) { // 干线压力
                                    String gxylValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                    if (gxylValue != null) {
                                        GY = CommonUtils.formatFloat(Float.parseFloat(gxylValue), 2);
                                        if(GY<=0.1) {   //处理干压为0影响统计数据
                                            GY = null;
                                        }
                                    }
                                }
                            }
                        }
                    }

                } catch (Exception e) {
                    log.info(code + ":" + e.toString());
                }

                //计算
                //上一班累积注水量
                String rtLjzsl = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJZSL.toString());
                Float banLJZSL = rtLjzsl == null ? 0f : Float.valueOf(rtLjzsl);
                if (LJLL != null) {
                    String zeroNum = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_LINGSHI_ZSLJLL.toString());
                    if (zeroNum != null) {
                        LJZSL = LJLL - Float.valueOf(zeroNum);
                        ZSL = LJZSL - banLJZSL;
                    }
                    //更新班累积耗电量
                    realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJZSL.toString(), String.valueOf(LJZSL));
                } else {    //读不上来累积流量
                    LJZSL = banLJZSL;
                    //更新班累积耗电量
                    realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJZSL.toString(), String.valueOf(LJZSL));
                }
                
                if (LJZSL != null && RPZL != null) {
                    CQL = LJZSL - RPZL;
                }

                //***************************开始  计算运行时间****************
                Float YXSJ = getYxsj(code);
                //上一班累积值
                String rtLjyxsj = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJYXSJ.toString());
                float ljyxsjValue = rtLjyxsj == null ? 0f : Float.valueOf(rtLjyxsj);
                Float LJYXSJ = ljyxsjValue + YXSJ;

                float scsj = 0;
                try {
                    int hour1 = YXSJ == null ? 0 : (YXSJ.intValue() / 60);
                    float minite = YXSJ % 60;
                    scsj = hour1 + minite / 100;
                } catch (Exception e) {
                }

                float ljscsj = 0;
                try {
                    int hour2 = LJYXSJ == null ? 0 : (LJYXSJ.intValue() / 60);
                    float minite2 = LJYXSJ % 60;
                    ljscsj = hour2 + minite2 / 100;
                } catch (Exception e) {
                }

                //更新运行时间
                realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJYXSJ.toString(), String.valueOf(LJYXSJ));
                //***************************结束  计算运行时间****************

                //程序处理
                Calendar c = Calendar.getInstance();
                c.set(Calendar.MINUTE, 0);
                c.set(Calendar.SECOND, 0);
                c.set(Calendar.MILLISECOND, 0);
                int hour = c.get(Calendar.HOUR_OF_DAY);

                if (hour % 2 != 0) {
                    c.set(Calendar.HOUR_OF_DAY, hour + 1);
                } else {
                    c.set(Calendar.HOUR_OF_DAY, hour);
                }
                SJD = String.valueOf(c.get(Calendar.HOUR_OF_DAY)) + ":00";

                //最后一班写入注水累积流量值
                if (SJD.equals("8:00")) {
                    realtimeDataService.putValue(code, RedisKeysEnum.RI_LINGSHI_ZSLJLL.toString(), LJLL == null ? "0" : String.valueOf(LJLL));
                }

                try (Connection con = sql2o.open()) {
                    con.createQuery(sql) //
                            .addParameter("ID", UUID.randomUUID().toString().replace("-", "")) //ID
                            .addParameter("CODE", code) //井号
                            .addParameter("PSJ", PSJ) //配水间
                            .addParameter("SAVE_TIME", new Date())//转储时间
                            .addParameter("DATE_TIME", c.getTime())//数据时间
                            .addParameter("YXSJ", scsj)//运行时间
                            .addParameter("LJYXSJ", ljscsj)//累积运行时间
                            .addParameter("GY", GY)//干压
                            .addParameter("ZRYL", ZRYL)//注入压力
                            .addParameter("SSLL", SSLL)//瞬时流量
                            .addParameter("LLSD", LLSD)//流量设定值
                            .addParameter("RPZL", RPZL)//日配注量
                            .addParameter("ZSL", ZSL)//注水量
                            .addParameter("LJZSL", LJZSL)//日累积注水量
                            .addParameter("CQL", CQL)//超欠量
                            .addParameter("SJD", SJD)//时间段
                            .executeUpdate();
                } catch (Exception e) {
                    log.info("处理水井：" + code + "出现异常！" + e.toString());
                    continue;
                }
            }
            log.info("水井班报录入结束——现在时刻：" + CommonUtils.date2String(new Date()));
        }
    }

    @Override
    public void runRiBaoTask() {
        log.info("水井日报录入开始——现在时刻：" + CommonUtils.date2String(new Date()));
        if (Scheduler.shuiJingList != null && Scheduler.shuiJingList.size() > 0) {
            for (EndTag shuiJing : Scheduler.shuiJingList) {
                String code = shuiJing.getCode();

                String sql = "insert into T_Water_Well_Daily_Data "
                        + "(ID, CODE, PSJ, SAVE_TIME, DATE_TIME, YXSJ, GY, ZRYL, YY, TY, RPZL, LJZSL, CQL)"
                        + "values (:ID, :CODE, :PSJ, :SAVE_TIME, :DATE_TIME, :YXSJ, :GY, :ZRYL, :YY, :TY, :RPZL, :LJZSL, :CQL)";
                //Oracle
                String PSJ = shuiJing.getParent() == null ? "—" : shuiJing.getParent().getName();
                //计算
                Float YXSJ = null, CQL = null, LJZSL = null;
                //实时库数据
                Float GY = null, ZRYL = null, TY = null;
                //源头库数据
                Float RPZL = null;


                //源头库数据
                Map<String, Object> map = findDataFromYdkByCode(code);
                if (map != null) {
                    RPZL = Float.parseFloat(((BigDecimal) map.get("rpzsl")).toString());
                }

                String rtYXSJ = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJYXSJ.toString());
                String rtZSL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJZSL.toString());

                YXSJ = rtYXSJ == null ? null : Float.valueOf(rtYXSJ);
                LJZSL = rtZSL == null ? null : Float.valueOf(rtZSL);

                //计算
                if (LJZSL != null && RPZL != null) {
                    CQL = LJZSL - RPZL;
                }
                //求平均值
                Calendar startTime = Calendar.getInstance();
                Calendar endTime = Calendar.getInstance();
                startTime.set(Calendar.MINUTE, 0);
                startTime.set(Calendar.SECOND, 0);
                startTime.set(Calendar.MILLISECOND, 0);
                startTime.set(Calendar.HOUR_OF_DAY, startTime.get(Calendar.HOUR_OF_DAY) + 1);
                startTime.set(Calendar.DAY_OF_MONTH, startTime.get(Calendar.DAY_OF_MONTH) - 1);
                endTime.set(Calendar.MINUTE, 0);
                endTime.set(Calendar.SECOND, 0);
                endTime.set(Calendar.MILLISECOND, 0);
                endTime.set(Calendar.HOUR_OF_DAY, endTime.get(Calendar.HOUR_OF_DAY) + 1);
                Map<String, Object> dayMap = getAvgDailyData(code, startTime.getTime(), endTime.getTime());
                if (dayMap != null) {
                    GY = dayMap.get("gy") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("gy")).toString());
                    ZRYL = dayMap.get("zryl") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("zryl")).toString());
                }

                Calendar c = Calendar.getInstance();
                c.set(Calendar.MINUTE, 0);
                c.set(Calendar.SECOND, 0);
                c.set(Calendar.MILLISECOND, 0);
                c.set(Calendar.HOUR_OF_DAY, 0);

                //23.55以上认为是24
                if (YXSJ != null && YXSJ >= 1435) {
                    YXSJ = 1440f;
                }

                float scsj = 0;
                try {
                    int hour = YXSJ == null ? 0 : (YXSJ.intValue() / 60);
                    float minite = YXSJ % 60;
                    scsj = hour + minite / 100;
                } catch (Exception e) {
                }

                try (Connection con = sql2o.open()) {
                    con.createQuery(sql) //
                            .addParameter("ID", UUID.randomUUID().toString().replace("-", "")) //
                            .addParameter("CODE", code) //
                            .addParameter("PSJ", PSJ) //配水间
                            .addParameter("SAVE_TIME", new Date())//
                            .addParameter("DATE_TIME", c.getTime())//
                            .addParameter("YXSJ", scsj)//运行时间
                            .addParameter("GY", GY)//干压
                            .addParameter("ZRYL", ZRYL)//注入压力
                            .addParameter("YY", ZRYL)//油压
                            .addParameter("TY", TY)//套压
                            .addParameter("RPZL", RPZL)//日配注量
                            .addParameter("LJZSL", LJZSL)//日累积注水量
                            .addParameter("CQL", CQL)//超欠量
                            .executeUpdate();//
                } catch (Exception e) {
                    log.info("处理水井：" + code + "出现异常！" + e.toString());
                    continue;
                }

                String jzrSql = "Insert into QYSCZH.SZS_SRD_SJ "
                        + "(JH, RQ, SCSJ, GXYL, YY, RZSL, GXSJ, PZL, GXR) "
                        + "values (:JH, :RQ, :SCSJ, :GXYL, :YY, :RZSL, :GXSJ, :PZL, :GXR)";


                try (Connection con = sql2o.open()) {
                    con.createQuery(jzrSql)
                            .addParameter("JH", code) //井号
                            .addParameter("RQ", c.getTime())//日期
                            .addParameter("SCSJ", scsj) //生产时间
                            .addParameter("GXYL", GY) //干线压力
                            .addParameter("YY", ZRYL) //油压
                            .addParameter("RZSL", LJZSL) //日注水量
                            .addParameter("GXSJ", new Date())//更新时间
                            .addParameter("PZL", RPZL)//日配注量
                            .addParameter("GXR", "管理员")//更新人
                            .executeUpdate();
                } catch (Exception e) {
                    log.info(code + "发生异常！");
                    e.printStackTrace();
                    continue;
                }

                //清除班累积运算值
                realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJYXSJ.toString(), "0");
                realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJZSL.toString(), "0");
            }
            log.info("水井日报录入结束——现在时刻：" + CommonUtils.date2String(new Date()));
        }
    }

    @Override
    public void runPsfzTask(Calendar c) {
        log.info("配水阀组录入开始——现在时刻：" + CommonUtils.date2String(new Date()));
        if (Scheduler.shuiJingList != null && Scheduler.shuiJingList.size() > 0) {
            for (EndTag shuiJing : Scheduler.shuiJingList) {
                String code = shuiJing.getCode();

                //计算
                Float YL = null, WD = null, SSLL = null, LJLL = null, PZL = null, GXYL = null;
                //实时库数据
                Integer FMZT = null, KD = null, SFBL = 0;


                //源头库数据
                Map<String, Object> map = findDataFromYdkByCode(code);
                if (map != null) {
                    PZL = Float.parseFloat(((BigDecimal) map.get("rpzsl")).toString()) / 24;
                }

                //实时库数据
                try {
                    String extConfigInfo = shuiJing.getExtConfigInfo();		// 获得扩展信息 
                    if (extConfigInfo != null && !"".equals(extConfigInfo.trim())) {
                        String[] framesLine = extConfigInfo.trim().replaceAll("\\r", "").split("\\n");// 替换字符串									
                        for (String varName : framesLine) {
                            //yc|zsyl-注水压力|psj_z1-10-b|zky12_zsyl 
                            if (varName.contains("yx|") || varName.contains("yc|")) {
                                String varNames[] = varName.trim().split("\\|");
                                String varName1 = varNames[1];
                                String codeName = varNames[2];
                                String varNameStr = varNames[3];
                                if (varName1.contains("zsyl-")) { // 注水压力
                                    String zsylValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                    if (zsylValue != null) {
                                        YL = CommonUtils.formatFloat(Float.parseFloat(zsylValue), 2);
                                    }
                                } else if (varName1.contains("ljll")) { // 累计流量
                                    String ljllValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                    if (ljllValue != null) {
                                        LJLL = CommonUtils.formatFloat(Float.parseFloat(ljllValue), 2);
                                    }
                                } else if (varName1.contains("shll")) { // 瞬时流量
                                    String ssllValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                    if (ssllValue != null) {
                                        SSLL = CommonUtils.formatFloat(Float.parseFloat(ssllValue), 2);
                                    }
                                } else if (varName1.contains("fmqg")) { //阀门状态
                                    String fmqgValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                    if (fmqgValue != null) {
                                        if ("true".equals(fmqgValue)) {
                                            FMZT = 0;
                                        } else {
                                            FMZT = 1;
                                        }
                                    }
                                } else if (varName1.contains("fmkd")) { //阀门开度
                                    String fmkdValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                    if (fmkdValue != null) {
                                        KD = (int) (Float.parseFloat(fmkdValue));
                                    }
                                }
                            }
                        }
                    }

                } catch (Exception e) {
                    log.info(code + ":" + e.toString());
                }

                //获取干线压力
                String extConfigInfo = null;
                if (shuiJing.getParent() != null) {
                    extConfigInfo = shuiJing.getParent().getExtConfigInfo();// 获取配水间值 
                } else {//单井水井
                    extConfigInfo = shuiJing.getExtConfigInfo();// 获取配水间值 
                }
                try {
                    if (extConfigInfo != null && !"".equals(extConfigInfo.trim())) {
                        String[] framesLine = extConfigInfo.trim().replaceAll("\\r", "").split("\\n");// 替换字符串									
                        for (String varName : framesLine) {
                            //yc|zsyl-注水压力|psj_z1-10-b|zky12_zsyl 
                            if (varName.contains("yc|")) {
                                String varNames[] = varName.trim().split("\\|");
                                String varName1 = varNames[1];
                                String codeName = varNames[2];
                                String varNameStr = varNames[3];
                                if (varName1.contains("gxyl-")) { // 干线压力
                                    String gxylValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                    if (gxylValue != null) {
                                        GXYL = CommonUtils.formatFloat(Float.parseFloat(gxylValue), 2);
                                    }
                                }
                            }
                        }
                    }

                } catch (Exception e) {
                    log.info(code + ":" + e.toString());
                }


                String jzrSql = "Insert into QYSCZH.SJS_SS_PSFZ "
                        + "(SBMC, YL, WD, SSLL, LJLL, FMZT, KD, PZL, GXYL, CJSJ, ZCSJ, SFBL) "
                        + "values (:SBMC, :YL, :WD, :SSLL, :LJLL, :FMZT, :KD, :PZL, :GXYL, :CJSJ, :ZCSJ, :SFBL)";

                try (Connection con = sql2o.open()) {
                    con.createQuery(jzrSql)
                            .addParameter("SBMC", code) //井号
                            .addParameter("YL", YL)//压力
                            .addParameter("WD", WD) //温度
                            .addParameter("SSLL", SSLL) //瞬时流量
                            .addParameter("LJLL", LJLL) //累积流量
                            .addParameter("FMZT", FMZT) //阀门状态
                            .addParameter("KD", KD)//开度
                            .addParameter("PZL", PZL)//配注量（小时）
                            .addParameter("GXYL", GXYL)//干线压力
                            .addParameter("CJSJ", c.getTime())//采集时间
                            .addParameter("ZCSJ", new Date())//转储时间
                            .addParameter("SFBL", SFBL)//是否补录
                            .executeUpdate();
                } catch (Exception e) {
                    log.info(code + "发生异常！");
                    e.printStackTrace();
                    continue;
                }
            }
            log.info("配水阀组录入结束——现在时刻：" + CommonUtils.date2String(new Date()));
        }
    }

    /**
     * 从源头库查询数据 泵径、含水、泵深、气油比、地面原油密度、地层水密度、动液面、天然气相对密度
     *
     * @param code
     * @return
     */
    private Map<String, Object> findDataFromYdkByCode(String code) {
        String sql = "SELECT * FROM (SELECT rpzsl from ys_dba02@ydk where jh=:CODE ORDER BY RQ DESC ) WHERE rownum <= 1";

        List<Map<String, Object>> list;
        try (Connection con = sql2o.open()) {
            list = con.createQuery(sql)
                    .addParameter("CODE", code)
                    .executeAndFetchTable().asList();
        }
        if (list != null && !list.isEmpty()) {
            return list.get(0);
        }
        return null;
    }

    private Float getRealData(String code, String varName) {
        String value = realtimeDataService.getEndTagVarInfo(code, varName);
        if (value != null && !value.isEmpty()) {
            return CommonUtils.string2Float(value, 4); // 保留四位小数
        } else {
            return null;
        }
    }

    /**
     * 获取运行时间
     *
     * @param code
     * @return
     */
    private float getYxsj(String code) {
        float yxsj = 0f;
        String sql = "SELECT count(IS_ON) as is_on "
                + " from T_WATER_WELL_CALC_DATA t where code=:CODE and IS_ON = 0 ";

        List<Map<String, Object>> list;
        try (Connection con = sql2o.open()) {
            list = con.createQuery(sql)
                    .addParameter("CODE", code)
                    .executeAndFetchTable().asList();
        }
        if (list != null && !list.isEmpty()) {
            yxsj = list.get(0).get("is_on") == null ? 0f : Float.parseFloat(((BigDecimal) list.get(0).get("is_on")).toString());
        }
        return 120 - yxsj;
    }

    /**
     * 从T_WELL_HOURLY_DATA中计算日数据
     *
     * @param code
     * @param startTime
     * @param endTime
     * @return
     */
    private Map<String, Object> getAvgDailyData(String code, Date startTime, Date endTime) {
        String sql = "SELECT avg(ZRYL) as ZRYL, "
                + "avg(GY) as GY "
                + " from T_WATER_WELL_HOURLY_DATA t where code=:CODE and DATE_TIME>=:startTime and DATE_TIME<=:endTime order by DATE_TIME DESC";

        List<Map<String, Object>> list;
        try (Connection con = sql2o.open()) {
            list = con.createQuery(sql)
                    .addParameter("CODE", code)
                    .addParameter("startTime", startTime)
                    .addParameter("endTime", endTime)
                    .executeAndFetchTable().asList();
        }
        if (list != null && !list.isEmpty()) {
            return list.get(0);
        }
        return null;
    }

    @Override
    public void testMathod() {
    }
}