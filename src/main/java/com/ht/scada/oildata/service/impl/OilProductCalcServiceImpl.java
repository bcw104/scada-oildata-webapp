/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.entity.WellHourlyData;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.oildata.CommonUtils;
import com.ht.scada.common.tag.util.EndTagSubTypeEnum;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.calc.GTDataComputerProcess;
import com.ht.scada.oildata.calc.GTReturnKeyEnum;
import com.ht.scada.oildata.model.GTSC;
import com.ht.scada.oildata.model.WellInfoWrapper;
import com.ht.scada.oildata.service.OilProductCalcService;
import com.ht.scada.oildata.service.WellInfoService;
import com.ht.scada.oildata.util.String2FloatArrayUtil;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
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
import org.sql2o.data.Row;

/**
 *
 * @author 赵磊 2014-8-14 23:49:22
 */
@Transactional
@Service("oilProductCalcService")
public class OilProductCalcServiceImpl implements OilProductCalcService {

    private static final Logger log = LoggerFactory.getLogger(OilProductCalcServiceImpl.class);
    @Autowired
    private EndTagService endTagService;
    @Autowired
    private RealtimeDataService realtimeDataService;
    @Autowired
    private WellInfoService wellInfoService;
    @Inject
    protected Sql2o sql2o;
    private Map<String, String> dateMap = new HashMap<>();
    private Map<String, String> myDateMap = new HashMap<>();

    public Sql2o getSql2o() {
        return sql2o;
    }

    public void setSql2o(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    @Override
    public void oilProductCalcTask() {
        System.out.println("开启功图分析任务：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));

        List<EndTag> youJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if (youJingList != null && youJingList.size() > 0) {
            for (EndTag youJing : youJingList) {
                try {
                    if (!youJing.getSubType().equals(EndTagSubTypeEnum.YOU_LIANG_SHI.toString()) && !youJing.getSubType().equals(EndTagSubTypeEnum.GAO_YUAN_JI.toString())) {
                        continue;
                    }
                    String code = youJing.getCode();
                    // 1.判断功图时间是否更新
                    String newDateTime = realtimeDataService.getEndTagVarYcArray(code, RedisKeysEnum.GT_DATETIME.toString());
                    if (myDateMap.get(code) != null && myDateMap.get(code).equals(newDateTime)) {
                        continue;
                    }
                    myDateMap.put(code, newDateTime);
                    // 2.判断是否有功图
                    if (getRealtimeData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) > 0) { // 有功图才写进行持久化
                        float[] weiyi = String2FloatArrayUtil.string2FloatArrayUtil(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.WEI_YI_ARRAY.toString().toLowerCase()), ",");
                        float[] zaihe = String2FloatArrayUtil.string2FloatArrayUtil(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.ZAI_HE_ARRAY.toString().toLowerCase()), ",");
                        float chongCi = Float.valueOf(realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase()));

                        String powerStr = realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.GONG_LV_ARRAY.toString().toLowerCase());
                        float[] power = null;
                        if (powerStr != null) {
                            power = String2FloatArrayUtil.string2FloatArrayUtil(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.GONG_LV_ARRAY.toString().toLowerCase()), ",");
                        }

                        String dlStr = realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.DIAN_LIU_ARRAY.toString().toLowerCase());
                        float[] dl = null;
                        if (dlStr != null) {
                            dl = String2FloatArrayUtil.string2FloatArrayUtil(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.DIAN_LIU_ARRAY.toString().toLowerCase()), ",");
                        }

                        WellInfoWrapper info = wellInfoService.findWellInfoByCode(code);
                        float bj = info == null || info.getBeng_jing() == null ? 56f : Float.valueOf(info.getBeng_jing());
                        float hs = info == null || info.getHan_shui() == null ? 0.9f : Float.valueOf(info.getHan_shui());
                        float md = info == null || info.getYymd() == null ? 1 : Float.valueOf(info.getYymd());

                        GTDataComputerProcess gtData = new GTDataComputerProcess();
                        Map<GTReturnKeyEnum, Object> resultMap = null;
                        try {
                            resultMap = gtData.calcSGTData(weiyi, zaihe, power, dl, chongCi, bj, md, hs / 100);
                        } catch (Exception e) {
                            System.out.println("功图分析出现异常：" + e.toString());
                            e.printStackTrace();
                            continue;
                        }
                        Date date = com.ht.scada.common.tag.util.CommonUtils.string2Date(newDateTime);

                        String shCYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.LIQUID_PRODUCT) * 24);
                        String ljCYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.LIQUID_PRODUCT) * 24 * CommonUtils.timeProportion(date));
                        String ygCYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.LIQUID_PRODUCT) * 24);

                        String shYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.OIL_PRODUCT) * 24);
                        String ljYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.OIL_PRODUCT) * 24 * CommonUtils.timeProportion(date));
                        String ygYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.OIL_PRODUCT) * 24);

                        String nhShang = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.NENG_HAO_SHANG));
                        String nhXia = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.NENG_HAO_XIA));
                        String phl = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.PING_HENG_DU));

                        String shNH = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.NENG_HAO_RI));
                        String ljNH = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.NENG_HAO_RI) * CommonUtils.timeProportion(date));
                        String ygNH = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.NENG_HAO_RI));

                        String phddl = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.PING_HENG_DU_DL));
                        String sxdl = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.DL_SHANG));
                        String xxdl = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.DL_XIA));

                        GTSC gtsc = findOneGTFXRecordByCode(code);

                        realtimeDataService.putValue(code, RedisKeysEnum.RI_SS_CYL.toString(), shCYL);
                        realtimeDataService.putValue(code, RedisKeysEnum.RI_LEIJI_CYL.toString(), ljCYL);
                        realtimeDataService.putValue(code, RedisKeysEnum.RI_YUGU_CYL.toString(), ygCYL);
                        realtimeDataService.putValue(code, RedisKeysEnum.ZR_CYL.toString(), shCYL); //昨日产液量

                        realtimeDataService.putValue(code, RedisKeysEnum.RI_SS_YL.toString(), shYL);
                        realtimeDataService.putValue(code, RedisKeysEnum.RI_LEIJI_YL.toString(), ljYL);
                        realtimeDataService.putValue(code, RedisKeysEnum.RI_YUGU_YL.toString(), ygYL);
                        realtimeDataService.putValue(code, RedisKeysEnum.ZR_YL.toString(), shYL); //昨日产油量

                        /**
                         * **************** 威尔泰克功图 ******************
                         */
                        float wetkCyl = gtsc.getRCYL1();
                        float wetkYl = gtsc.getRCYL();

                        String querySql = "select HDL, CYL, YL, YXSJ from T_Well_Hourly_Data where code=:CODE and DATE_TIME=:DATE_TIME";

                        Float LJCYL = 0f;
                        Float LJYL = 0f;
                        Float HDL = 0f;

                        Calendar cal = Calendar.getInstance();
                        cal.set(Calendar.MINUTE, 0);
                        cal.set(Calendar.SECOND, 0);
                        cal.set(Calendar.MILLISECOND, 0);
                        int i = 0;
                        while (cal.get(Calendar.HOUR_OF_DAY) != 8) {
                            if (cal.get(Calendar.HOUR_OF_DAY) % 2 != 0) {   //偶数点
                                cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
                                continue;
                            }
                            try (Connection con = sql2o.open()) {
                                List<WellHourlyData> list = con.createQuery(querySql)
                                        .setAutoDeriveColumnNames(true)
                                        .addParameter("CODE", code)
                                        .addParameter("DATE_TIME", cal.getTime())
                                        .executeAndFetch(WellHourlyData.class);
                                if (list != null && !list.isEmpty()) {
                                    WellHourlyData data = list.get(0);
                                    LJCYL += data.getCyl() == null ? 0f : data.getCyl();
                                    LJYL += data.getYl() == null ? 0f : data.getYl();
                                    HDL -= data.getHdl() == null ? 0f : data.getHdl();
                                    i++;
                                }
                            }
                            cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
                        }

                        Date eDate = cal.getTime();
                        Date sDate = new Date(cal.getTime().getTime() - 24 * 60 * 60000);
                        float[] cyl = getCylAndYlByJHAndTime(code, sDate, eDate);
//                        log.info("时间段：" + LocalDateTime.fromDateFields(sDate) + " - " + LocalDateTime.fromDateFields(eDate));
                        log.info("{} 产液量：{}  产油量：{}", code, cyl[0], cyl[1]);

                        realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_CYL.toString(), String.valueOf(wetkCyl));
                        realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_LEIJI_CYL.toString(), String.valueOf(LJCYL));
                        realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_YUGU_CYL.toString(), String.valueOf(LJCYL * 2));
                        realtimeDataService.putValue(code, RedisKeysEnum.WETK_ZR_CYL.toString(), String.valueOf(cyl[0])); //昨日产液量

                        realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_YL.toString(), String.valueOf(wetkYl));
                        realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_LEIJI_YL.toString(), String.valueOf(LJYL));
                        realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_YUGU_YL.toString(), String.valueOf(LJYL * 2));
                        realtimeDataService.putValue(code, RedisKeysEnum.WETK_ZR_YL.toString(), String.valueOf(cyl[1])); //昨日产油量

                        String bgt_weiyi = String2FloatArrayUtil.string2OrientationStringArrayUtil(gtsc.getBGT(), String2FloatArrayUtil.ORIENTATION_X, ";");
                        String bgt_zaihe = String2FloatArrayUtil.string2OrientationStringArrayUtil(gtsc.getBGT(), String2FloatArrayUtil.ORIENTATION_Y, ";");

//                        log.info("泵功图位移：{}", bgt_weiyi);
//                        log.info("泵功图载荷：{}", bgt_zaihe);
                        if (bgt_weiyi != null) {
                            realtimeDataService.putValue(code, RedisKeysEnum.WETK_WY.toString(), bgt_weiyi); //位移 
                        }
                        if (bgt_zaihe != null) {
                            realtimeDataService.putValue(code, RedisKeysEnum.WETK_BGT.toString(), bgt_zaihe); //泵功图
                        }
                        /**
                         * **************** 威尔泰克功图 ******************
                         */
                        realtimeDataService.putValue(code, RedisKeysEnum.ZR_HDL.toString(), shNH);  //昨日耗电量
                        realtimeDataService.putValue(code, RedisKeysEnum.RI_LEIJI_HDL.toString(), ljNH);
                        realtimeDataService.putValue(code, RedisKeysEnum.RI_YUGU_HDL.toString(), ygNH);

                        realtimeDataService.putValue(code, RedisKeysEnum.SHANG_NH.toString(), nhShang);
                        realtimeDataService.putValue(code, RedisKeysEnum.XIA_NH.toString(), nhXia);
                        realtimeDataService.putValue(code, RedisKeysEnum.PING_HENG_LV.toString(), phl);

                        realtimeDataService.putValue(code, RedisKeysEnum.HAN_SHUI_LV.toString(), String.valueOf(hs / 100));
                        realtimeDataService.putValue(code, RedisKeysEnum.DONG_YE_MIAIN.toString(), "0");

                        realtimeDataService.putValue(code, RedisKeysEnum.PING_HENG_LV_DL.toString(), phddl);
                        realtimeDataService.putValue(code, RedisKeysEnum.DL_SHANG.toString(), sxdl);
                        realtimeDataService.putValue(code, RedisKeysEnum.DL_XIA.toString(), xxdl);
                        //TODU:写历史数据
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("完成功图分析任务：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
    }

     private GTSC findOneGTFXRecordByCode(String code) {
        //String sql = "select q.RCYL1 rcyl1,q.RCYL rcyl,s.JH jh,q.CJSJ cjsj,s.WY wy,q.BGT bgt FROM QYSCZH.SCY_SGT_GTCJ s inner join QYSCZH.SCY_SGT_GTFX q on s.JH=:CODE AND q.SCJSBZ = 1 AND q.JH=s.JH ORDER BY q.CJSJ DESC ";//
        String sql = "SELECT" +//
                "  q.RCYL1 rcyl1, " +//
                "  q.RCYL  rcyl, " +//
                "  s.JH    jh, " +//
                "  q.CJSJ  cjsj, " +//
                "  s.WY    wy, " +//
                "  q.BGT   bgt " +//
                " FROM QYSCZH.SCY_SGT_GTCJ s LEFT JOIN QYSCZH.SCY_SGT_GTFX q ON q.JH = s.JH AND s.cjsj = q.cjsj AND s.id = q.gtid "
                + " WHERE q.SCJSBZ=1 and s.JH = :CODE and q.cjsj is not null ORDER BY q.CJSJ DESC ";
        try (Connection con = sql2o.open()) {  //
            org.sql2o.Query query = con.createQuery(sql).addParameter("CODE", code);
            return query.executeAndFetchFirst(GTSC.class);
        }
    }

    private List<GTSC> findGTSCRecordByJHAndTime(String JH, Date startTime, Date endTime) {
        List<GTSC> rtnList = new ArrayList<>();
        String sql = "SELECT GTCJ.JH jh,GTCJ.CJSJ cjsj,GTFX.RCYL rcyl,GTFX.RCYL1 rcyl1,GTCJ.WY wy,GTCJ.ZH zh, " + //
                "GTCJ.ZDZH zdzh,GTCJ.ZXZH zxzh,GTFX.JSBZ jsbz,GTCJ.CC cc,GTCJ.CC1 cc1 FROM QYSCZH.SCY_SGT_GTFX GTFX " +//
                "LEFT JOIN QYSCZH.SCY_SGT_GTCJ GTCJ ON GTFX.GTID=GTCJ.ID WHERE GTFX.SCJSBZ=1 AND GTFX.JH=:JH AND GTCJ.CJSJ>=:startTime AND GTCJ.CJSJ<:endTime ORDER BY cjsj DESC";
        try (Connection con = sql2o.open()) {  //
            org.sql2o.Query query = con.createQuery(sql).addParameter("JH", JH).addParameter("startTime", startTime).addParameter("endTime", endTime);
            List<Row> dataList = query.executeAndFetchTable().rows();
            for (Row row : dataList) {
                GTSC gtsc = new GTSC();
                gtsc.setJH(row.getString("jh"));
                gtsc.setCJSJ(row.getString("cjsj"));
                if (row.getFloat("rcyl") == null) {
                    gtsc.setRCYL(0f);
                } else {
                    gtsc.setRCYL(row.getFloat("rcyl"));
                }
                if (row.getFloat("rcyl1") == null) {
                    gtsc.setRCYL1(0f);
                } else {
                    gtsc.setRCYL1(row.getFloat("rcyl1"));
                }
                gtsc.setWY(row.getString("wy"));
                gtsc.setZH(row.getString("zh"));
                gtsc.setZDZH(row.getFloat("zdzh"));
                gtsc.setZXZH(row.getFloat("zxzh"));
                gtsc.setJSBZ(row.getFloat("jsbz"));
                gtsc.setCC(row.getFloat("cc"));
                gtsc.setCC1(row.getFloat("cc1"));
                rtnList.add(gtsc);
            }
        }
        return rtnList;
    }

    private float[] getCylAndYlByJHAndTime(String JH, Date startTime, Date endTime) {
        float[] result = new float[2];
        result[0] = 0f;
        result[1] = 0f;
        String sql = "SELECT avg(GTFX.RCYL) rcyl,avg(GTFX.RCYL1) rcyl1 " + //
                "FROM QYSCZH.SCY_SGT_GTFX GTFX " +//
                "WHERE GTFX.SCJSBZ=1 AND GTFX.JH=:JH AND GTFX.CJSJ>=:startTime AND GTFX.CJSJ<:endTime";
        try (Connection con = sql2o.open()) {  //
            org.sql2o.Query query = con.createQuery(sql).addParameter("JH", JH).addParameter("startTime", startTime).addParameter("endTime", endTime);
            List<Row> list = query.executeAndFetchTable().rows();

            if (list != null && !list.isEmpty()) {
                result[0] = list.get(0).getFloat("rcyl1");    //日产液量
                result[1] = list.get(0).getFloat("rcyl");     //日产油量
            }
        }
        return result;
    }
    
    /**
     * 根据井号和变量名获取实时数据
     *
     * @param code
     * @param varName
     * @return
     */
    private float getRealtimeData(String code, String varName) {
        String value = realtimeDataService.getEndTagVarInfo(code, varName);
        if (value != null && !value.isEmpty()) {
            return com.ht.scada.common.tag.util.CommonUtils.string2Float(value, 4); // 保留四位小数
        } else {
            return 0;
        }
    }
    
}