/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.data.entity.SoeRecord;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.service.WaterWellDataCalcService;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
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

    /**
     * 班报任务
     *
     * @author 赵磊
     */
    @Override
    public void runBanBaoTask() {
        System.out.println("水井班报录入开始——现在时刻：" + CommonUtils.date2String(new Date()));
        List<EndTag> shuiJingList = endTagService.getByType(EndTagTypeEnum.ZHU_SHUI_JING.toString());
        if (shuiJingList != null && shuiJingList.size() > 0) {
            for (EndTag shuiJing : shuiJingList) {
                String code = shuiJing.getCode();
                String sql = "insert into T_Water_Well_Hourly_Data "
                        + "(ID, CODE, PSJ, SAVE_TIME, DATE_TIME, YXSJ, LJYXSJ, GY, ZRYL, SSLL, LLSD, RPZL, ZSL, LJZSL, CQL, SJD)"
                        + "values (:ID, :CODE, :PSJ, :SAVE_TIME, :DATE_TIME, :YXSJ, :LJYXSJ, :GY, :ZRYL, :SSLL, :LLSD, :RPZL, :ZSL, :LJZSL, :CQL, :SJD)";
                //Oracle
                String PSJ = shuiJing.getParent() == null ? "—" : shuiJing.getParent().getName();
                //计算
                Float YXSJ = null, LJYXSJ = null, CQL = null, ZSL = null, LJZSL = null;
                //实时库数据
                Float GY = null, ZRYL = null, SSLL = null, LLSD = null;
                //源头库数据
                Float RPZL = null;
                //程序处理
                String SJD;


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

                //源头库数据
                Map<String, Object> map = findDataFromYdkByCode(code);
                if (map != null) {
                    RPZL = Float.parseFloat(((BigDecimal) map.get("rpzsl")).toString());
                }

                //实时库数据
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
//                                    ljll = CommonUtils.formatFloat(Float.parseFloat(ljllValue), 2);
                                }
                            } else if (varName1.contains("shll")) { // 瞬时流量
                                String ssllValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                if (ssllValue != null) {
                                    SSLL = CommonUtils.formatFloat(Float.parseFloat(ssllValue), 2);
                                }
                            } else if (varName1.contains("fmqg")) { //阀门全关
                                String fmqgValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                if (fmqgValue != null) {
//                                    yxsj = Float.parseFloat(yxsjValue);
                                }
                            }
//                            else if (varName1.contains("zsllsdz")) { //流量设定值
//                                String llsdValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
//                                if (llsdValue != null) {
//                                    LLSD = Float.parseFloat(llsdValue);
//                                }
//                            }
                        }
                    }
                }
                if (shuiJing.getParent() != null) {  //干压
                    GY = getRealData(shuiJing.getParent().getCode(), "gxyl");
                }

                //计算
                if(LJZSL != null && RPZL != null) {
                    CQL = LJZSL - RPZL;
                }
                YXSJ = 120f;
                

                Calendar startTime = Calendar.getInstance();
                Calendar endTime = Calendar.getInstance();
                startTime.set(Calendar.MINUTE, 0);
                startTime.set(Calendar.SECOND, 0);
                startTime.set(Calendar.MILLISECOND, 0);
                startTime.set(Calendar.HOUR_OF_DAY, startTime.get(Calendar.HOUR_OF_DAY) - 3);
                endTime.set(Calendar.MINUTE, 0);
                endTime.set(Calendar.SECOND, 0);
                endTime.set(Calendar.MILLISECOND, 0);
                endTime.set(Calendar.HOUR_OF_DAY, startTime.get(Calendar.HOUR_OF_DAY) - 1);


                try (Connection con = sql2o.open()) {
                    con.createQuery(sql) //
                            .addParameter("ID", UUID.randomUUID().toString().replace("-", "")) //
                            .addParameter("CODE", code) //
                            .addParameter("PSJ", PSJ) //配水间
                            .addParameter("SAVE_TIME", new Date())//
                            .addParameter("DATE_TIME", c.getTime())//
                            .addParameter("YXSJ", YXSJ)//运行时间
                            .addParameter("LJYXSJ", LJYXSJ)//累积运行时间
                            .addParameter("GY", GY)//干压
                            .addParameter("ZRYL", ZRYL)//注入压力
                            .addParameter("SSLL", SSLL)//瞬时流量
                            .addParameter("LLSD", LLSD)//流量设定值
                            .addParameter("RPZL", RPZL)//日配注量
                            .addParameter("ZSL", ZSL)//注水量
                            .addParameter("LJZSL", LJZSL)//日累积注水量
                            .addParameter("CQL", CQL)//超欠量
                            .addParameter("SJD", SJD)//时间段
                            //.addParameter("BZ", "")//备注
                            .executeUpdate();//
                } catch (Exception e) {
                    System.out.println("处理水井：" + code + "出现异常！" + e.toString());
                }
            }

            System.out.println("水井班报录入结束——现在时刻：" + CommonUtils.date2String(new Date()));
        }
    }

    @Override
    public void runRiBaoTask() {
        System.out.println("水井日报录入开始——现在时刻：" + CommonUtils.date2String(new Date()));
        List<EndTag> shuiJingList = endTagService.getByType(EndTagTypeEnum.ZHU_SHUI_JING.toString());
        if (shuiJingList != null && shuiJingList.size() > 0) {
            for (EndTag shuiJing : shuiJingList) {
                String code = shuiJing.getCode();

                String sql = "insert into T_Water_Well_Daily_Data "
                        + "(ID, CODE, PSJ, SAVE_TIME, DATE_TIME, YXSJ, GY, ZRYL, YY, TY, RPZL, LJZSL, CQL)"
                        + "values (:ID, :CODE, :PSJ, :SAVE_TIME, :DATE_TIME, :YXSJ, :GY, :ZRYL, :YY, :TY, :RPZL, :LJZSL, :CQL)";
                //Oracle
                String PSJ = shuiJing.getParent() == null ? "—" : shuiJing.getParent().getName();
                //计算
                Float YXSJ = null, CQL = null, ZSL = null, LJZSL = null;
                //实时库数据
                Float GY = null, ZRYL = null, YY = null, TY = null;
                //源头库数据
                Float RPZL = null;


                //源头库数据
                Map<String, Object> map = findDataFromYdkByCode(code);
                if (map != null) {
                    RPZL = Float.parseFloat(((BigDecimal) map.get("rpzsl")).toString());
                }


                //计算
                if(LJZSL != null && RPZL != null) {
                    CQL = LJZSL - RPZL;
                }
                YXSJ = 1440f;

                Calendar startTime = Calendar.getInstance();
                Calendar endTime = Calendar.getInstance();
                startTime.set(Calendar.MINUTE, 0);
                startTime.set(Calendar.SECOND, 0);
                startTime.set(Calendar.MILLISECOND, 0);
                startTime.set(Calendar.HOUR_OF_DAY, startTime.get(Calendar.HOUR_OF_DAY) - 3);
                endTime.set(Calendar.MINUTE, 0);
                endTime.set(Calendar.SECOND, 0);
                endTime.set(Calendar.MILLISECOND, 0);
                endTime.set(Calendar.HOUR_OF_DAY, startTime.get(Calendar.HOUR_OF_DAY) - 1);


                Calendar c = Calendar.getInstance();
                c.set(Calendar.MINUTE, 0);
                c.set(Calendar.SECOND, 0);
                c.set(Calendar.MILLISECOND, 0);
                c.set(Calendar.HOUR_OF_DAY, 0);

                try (Connection con = sql2o.open()) {
                    con.createQuery(sql) //
                            .addParameter("ID", UUID.randomUUID().toString().replace("-", "")) //
                            .addParameter("CODE", code) //
                            .addParameter("PSJ", PSJ) //配水间
                            .addParameter("SAVE_TIME", new Date())//
                            .addParameter("DATE_TIME", c.getTime())//
                            .addParameter("YXSJ", YXSJ)//运行时间
                            .addParameter("GY", GY)//干压
                            .addParameter("ZRYL", ZRYL)//注入压力
                            .addParameter("YY", YY)//瞬时流量
                            .addParameter("TY", TY)//流量设定值
                            .addParameter("RPZL", RPZL)//日配注量
                            .addParameter("LJZSL", LJZSL)//日累积注水量
                            .addParameter("CQL", CQL)//超欠量
                            .executeUpdate();//
                } catch (Exception e) {
                    System.out.println("处理水井：" + code + "出现异常！" + e.toString());
                }
            }
            System.out.println("水井日报录入结束——现在时刻：" + CommonUtils.date2String(new Date()));
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

    /**
     * 从T_WELL_HOURLY_DATA中计算日数据
     *
     * @param code
     * @param startTime
     * @param endTime
     * @return
     */
    private Map<String, Object> getDailyData(String code, Date startTime, Date endTime) {
        String sql = "SELECT avg(CHONG_CHENG) as CHONG_CHENG, "
                + "avg(CHONG_CI) as CHONG_CI, "
                + "avg(MAX_ZAIHE) as ZDZH, "
                + "avg(MIN_ZAIHE) as ZXZH, "
                + "avg(PHL) as PHL, "
                + "avg(PHL1) as PHL1, "
                //                +"sum(hdl) as HDL, "
                //                +"sum(cyl) as CYL, "
                //                +"sum(yl) as YL, "
                //                +"sum(yxsj) as RLJYXSJ, "
                + "avg(HY) as HY, "
                + "avg(TY) as TY, "
                + "avg(WD) as WD, "
                + "avg(PJDL) as PJDL, "
                + "avg(PJDY) as PJDY, "
                + "avg(SXDL) as SXDL, "
                + "avg(XXDL) as XXDL, "
                + "avg(SXNH) as SXNH, "
                + "avg(XXNH) as XXNH, "
                + "avg(PL) as PL "
                + " from T_WELL_HOURLY_DATA t where code=:CODE and DATE_TIME>=:startTime and DATE_TIME<=:endTime order by DATE_TIME DESC";

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

    /**
     * 获取累计值
     *
     * @param code
     * @param startTime
     * @param endTime
     * @return
     */
    private Map<String, Object> getLatestDailyData(String code, Date startTime, Date endTime) {
        String sql = "SELECT ljhdl,ljcyl,ljyl,ljyxsj from T_WELL_HOURLY_DATA t where code=:CODE and DATE_TIME>=:startTime and DATE_TIME<=:endTime order by DATE_TIME DESC";

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

    private float getRealData(String code, String varName) {
        String value = realtimeDataService.getEndTagVarInfo(code, varName);
        if (value != null && !value.isEmpty()) {
            return CommonUtils.string2Float(value, 4); // 保留四位小数
        } else {
            return 0;
        }
    }

    /**
     * 获得时间段内运行时间
     *
     * @param code
     * @return
     */
    private float getYxsjByCode(String code, Date startTime, Date endTime) {
        String sql = "select * from T_SOE_RECORD where code=:CODE and DEVICE_TIME>=:startTime and DEVICE_TIME<=:endTime and ALARM_TYPE='油井启停报警' order by DEVICE_TIME desc";
        float time = 120f;


        try (Connection con = sql2o.open()) {
            List<SoeRecord> list = con.createQuery(sql)
                    .setAutoDeriveColumnNames(true)
                    .addParameter("CODE", code)
                    .addParameter("startTime", startTime)
                    .addParameter("endTime", endTime)
                    .executeAndFetch(SoeRecord.class);
            long lastStopTime = startTime.getTime();
            if (list
                    != null && !list.isEmpty()) {
                for (SoeRecord s : list) {
                    if (s.getTagName().contains("停")) {
                        lastStopTime = s.getDeviceTime().getTime();
                    } else if (s.getTagName().contains("起")) {
                        if (lastStopTime == -1) {
                            continue;
                        }
                        time -= ((float) (s.getDeviceTime().getTime() - lastStopTime)) / (1000 * 60);
                        lastStopTime = -1;
                    }
                }
                if (lastStopTime != -1 && lastStopTime != startTime.getTime()) {//一直未起井
                    time -= ((float) (endTime.getTime() - lastStopTime)) / (1000 * 60);
                }
            } else {//若一直停井则需另判断
                return time;
            }
        }
        return time;
    }

    public static void main(String args[]) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
//        while (cal.get(Calendar.HOUR_OF_DAY) != 8) {
//            System.out.println("时间：" + cal.get(Calendar.HOUR_OF_DAY));
//            if (cal.get(Calendar.HOUR_OF_DAY) % 2 != 0) {
//                cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
//                continue;
//            }
//            System.out.println(cal.get(Calendar.HOUR_OF_DAY));
//            cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
//        }

        Calendar startTime = Calendar.getInstance();
        Calendar endTime = Calendar.getInstance();
        startTime.set(Calendar.MINUTE, 0);
        startTime.set(Calendar.SECOND, 0);
        startTime.set(Calendar.MILLISECOND, 0);
        endTime.set(Calendar.MINUTE, 0);
        endTime.set(Calendar.SECOND, 0);
        endTime.set(Calendar.MILLISECOND, 0);
        startTime.set(Calendar.HOUR_OF_DAY, 1);
        System.out.println(sdf.format(startTime.getTime()));
        startTime.set(Calendar.HOUR_OF_DAY, startTime.get(Calendar.HOUR_OF_DAY) - 2);
        System.out.println(sdf.format(startTime.getTime()));

    }

    @Override
    public void testMathod() {
        log.info("开始测试……");

        log.info("结束测试……");
    }
}