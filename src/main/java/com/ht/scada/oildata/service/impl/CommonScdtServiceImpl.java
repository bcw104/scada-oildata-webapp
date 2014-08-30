/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.entity.WellHourlyData;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.entity.SoeRecord;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.service.CommonScdtService;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
@Service("commonScdtService")
public class CommonScdtServiceImpl implements CommonScdtService {

    private static final Logger log = LoggerFactory.getLogger(CommonScdtServiceImpl.class);
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
        System.out.println("班报录入开始——现在时刻：" + CommonUtils.date2String(new Date()));
        List<EndTag> youJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if (youJingList != null && youJingList.size() > 0) {
            for (EndTag youJing : youJingList) {
                try {
                    String code = youJing.getCode();
                    String sql = "insert into T_Well_Hourly_Data "
                            + "(ID, CODE, BENG_JING, HAN_SHUI, DYM, YYMD,TRQXDMD, SMD,QYB,BENG_SHEN,BZSZ,BZXZ,SAVE_TIME,DATE_TIME,CHONG_CHENG,"
                            + "CHONG_CI,MIN_ZAIHE,MAX_ZAIHE,WEIYI,ZAIHE,PHL,HDL,CYL,YL,YXSJ,LJHDL,LJCYL,LJYL,LJYXSJ,HY,TY,WD,PJDL,PJDY,SXDL,XXDL,PL,SXNH,XXNH,SJD,PHL1) "
                            + "values (:ID, :CODE, :BENG_JING, :HAN_SHUI, :DYM,:YYMD, :TRQXDMD, :SMD,:QYB,:BENG_SHEN,:BZSZ,:BZXZ,:SAVE_TIME,:DATE_TIME,:CHONG_CHENG,"
                            + ":CHONG_CI,:MIN_ZAIHE,:MAX_ZAIHE,:WEIYI,:ZAIHE,:PHL,:HDL,:CYL,:YL,:YXSJ,:LJHDL,:LJCYL,:LJYL,:LJYXSJ,:HY,:TY,:WD,:PJDL,:PJDY,:SXDL,:XXDL,:PL,:SXNH,:XXNH,:SJD,:PHL1)";
                    //源头库中数据
                    Map<String, Object> map = findDataFromYdkByCode(code);
                    Float BJ = null, HS = null, BS = null, QYB = null, DMYYMD = null, DCSMD = null, DYM = null, TRQXDMD = null;
                    if (map != null) {
                        BJ = Float.parseFloat(((BigDecimal) map.get("bj")).toString());
                        HS = Float.parseFloat(((BigDecimal) map.get("hs")).toString());
                        BS = map.get("bs") == null ? null : Float.parseFloat(((BigDecimal) map.get("bs")).toString());
                        QYB = map.get("qyb") == null ? null : Float.parseFloat(((BigDecimal) map.get("qyb")).toString());
                        DMYYMD = map.get("dmyymd") == null ? null : Float.parseFloat(((BigDecimal) map.get("dmyymd")).toString());
                        DCSMD = map.get("dcsmd") == null ? null : Float.parseFloat(((BigDecimal) map.get("dcsmd")).toString());
                        DYM = map.get("dym") != null ? Float.parseFloat(((BigDecimal) map.get("dym")).toString()) : null;
                        TRQXDMD = map.get("trqxdmd") == null ? null : Float.parseFloat(((BigDecimal) map.get("trqxdmd")).toString());
                    }
                    //实时库中数据
                    Float CHONG_CHONG = null, CHONG_CI = null, ZDZH = null, ZXZH = null;
                    String ZAIHE = null, WEIYI = null;
                    if (getRealData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) > 0) {
                        CHONG_CHONG = getRealData(code, VarSubTypeEnum.CHONG_CHENG.toString().toLowerCase());
                        CHONG_CI = getRealData(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase());
                        ZDZH = getRealData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase());
                        ZXZH = getRealData(code, VarSubTypeEnum.ZUI_XIAO_ZAI_HE.toString().toLowerCase());
                        ZAIHE = realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.ZAI_HE_ARRAY.toString().toLowerCase());
                        WEIYI = realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.WEI_YI_ARRAY.toString().toLowerCase());
                    }
                    //TODU:用上一班的两个小时内功图求平均值
                    Float CYL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.WETK_RI_SS_CYL.toString()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.WETK_RI_SS_CYL.toString())) / 12;
                    Float YL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.WETK_RI_SS_YL.toString()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.WETK_RI_SS_YL.toString())) / 12;
                    Float PHL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.PING_HENG_LV.toString()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.PING_HENG_LV.toString()));
//              Float HDL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_SS_HDL.toString()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_SS_HDL.toString())) / 12;

                    Float LJHDL = 0f;
                    //累积用电量
                    String currentNum = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.DL_ZX_Z.toString().toLowerCase());
                    try {
                        if (currentNum != null) {
                            String zeroNum = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_LINGSHI_DBDS.toString());
                            if (zeroNum != null) {
                                LJHDL = Float.valueOf(currentNum) - Float.valueOf(zeroNum);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    Float LJCYL = 0f;
                    Float LJYL = 0f;
                    Float LJYXSJ = 0f;
                    Float HDL = LJHDL;

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
                    Float YXSJ = getYxsjByCode(code, startTime.getTime(), endTime.getTime());

                    String querySql = "select HDL, CYL, YL, YXSJ from T_Well_Hourly_Data where code=:CODE and DATE_TIME=:DATE_TIME";

                    Calendar cal = Calendar.getInstance();
                    cal.set(Calendar.MINUTE, 0);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);
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
                                LJYXSJ += data.getYxsj() == null ? 0f : data.getYxsj();
                                HDL -= data.getHdl() == null ? 0f : data.getHdl();
                            }
                        }
                        cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
                    }
                    LJCYL += CYL == null ? 0f : CYL;
                    LJYL += YL == null ? 0f : YL;
                    LJYXSJ += YXSJ == null ? 0f : YXSJ;

                    Float HY = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.HUI_YA.toString().toLowerCase()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.HUI_YA.toString().toLowerCase()));
                    Float TY = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.TAO_YA.toString().toLowerCase()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.TAO_YA.toString().toLowerCase()));
                    Float WD = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.JING_KOU_WEN_DU.toString().toLowerCase()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.JING_KOU_WEN_DU.toString().toLowerCase()));
                    Float PJDL = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.I_B.toString().toLowerCase()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.I_B.toString().toLowerCase()));
                    Float PJDY = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.U_B.toString().toLowerCase()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.U_B.toString().toLowerCase()));
                    Float PL = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.GV_ZB.toString().toLowerCase()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.GV_ZB.toString().toLowerCase()));
                    Float SXDL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DL_SHANG.toString()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DL_SHANG.toString()));
                    Float XXDL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DL_XIA.toString()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DL_XIA.toString()));
                    Float SXNH = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.SHANG_NH.toString()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.SHANG_NH.toString()));
                    Float XXNH = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.XIA_NH.toString()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.XIA_NH.toString()));

                    Float PHL1 = null;
                    if (SXDL != null && XXDL != null && Math.abs(SXDL) > 0) {
                        PHL1 = Math.abs(XXDL) / Math.abs(SXDL);
                    }

                    Calendar c = Calendar.getInstance();
                    c.set(Calendar.MINUTE, 0);
                    c.set(Calendar.SECOND, 0);
                    c.set(Calendar.MILLISECOND, 0);
                    int hour = c.get(Calendar.HOUR_OF_DAY);
                    
                    if(c.get(Calendar.HOUR_OF_DAY) % 2 != 0) {
                        c.set(Calendar.HOUR_OF_DAY, hour);
                    } else {
                        c.set(Calendar.HOUR_OF_DAY, hour + 1);
                    }
                    String SJD = String.valueOf(c.get(Calendar.HOUR_OF_DAY)) + ":00";

                    try (Connection con = sql2o.open()) {
                        con.createQuery(sql) //
                                .addParameter("ID", UUID.randomUUID().toString().replace("-", "")) //
                                .addParameter("CODE", code) //
                                .addParameter("BENG_JING", BJ)//泵径
                                .addParameter("HAN_SHUI", HS) //含水
                                .addParameter("DYM", DYM) //动液面
                                .addParameter("YYMD", DMYYMD) //原油密度
                                .addParameter("TRQXDMD", TRQXDMD) //天然气相对密度
                                .addParameter("SMD", DCSMD)//水密度
                                .addParameter("QYB", QYB)//生产汽油比
                                .addParameter("BENG_SHEN", BS)//泵深
                                .addParameter("BZSZ", -1)//标准上载
                                .addParameter("BZXZ", -1)//标准下载
                                .addParameter("SAVE_TIME", new Date())//
                                .addParameter("DATE_TIME", c.getTime())//
                                .addParameter("CHONG_CHENG", CHONG_CHONG)//冲程
                                .addParameter("CHONG_CI", CHONG_CI)//冲次
                                .addParameter("MIN_ZAIHE", ZXZH)//最小载荷
                                .addParameter("MAX_ZAIHE", ZDZH)//最大载荷
                                .addParameter("WEIYI", WEIYI)//位移
                                .addParameter("ZAIHE", ZAIHE)//载荷
                                .addParameter("PHL", PHL)//平衡率
                                .addParameter("PHL1", PHL1)//平衡率
                                .addParameter("HDL", HDL)//耗电量
                                .addParameter("CYL", CYL)//产液量
                                .addParameter("YL", YL)//油量
                                .addParameter("LJHDL", LJHDL)//累积耗电量
                                .addParameter("LJCYL", LJCYL)//累积产液量
                                .addParameter("LJYL", LJYL)//累积液量
                                .addParameter("YXSJ", YXSJ)//运行时间
                                .addParameter("LJYXSJ", LJYXSJ)//累积运行时间
                                .addParameter("HY", HY)//回压
                                .addParameter("TY", TY)//套压
                                .addParameter("WD", WD)//温度
                                .addParameter("PJDL", PJDL)//平均电流
                                .addParameter("PJDY", PJDY)//平均电压
                                .addParameter("SXDL", SXDL)//上行电流
                                .addParameter("XXDL", XXDL)//下行电流
                                .addParameter("SXNH", SXNH)//上行能耗
                                .addParameter("XXNH", XXNH)//下行能耗
                                .addParameter("PL", PL)//频率
                                .addParameter("SJD", SJD)//时间段
                                //.addParameter("BZ", "")//备注
                                .executeUpdate();//
                    } catch (Exception e) {
                        System.out.println("处理井：" + code + "出现异常！" + e.toString());
                    }
                } catch (Exception e) {
                }

            }
        }
        System.out.println("班报录入结束——现在时刻：" + CommonUtils.date2String(new Date()));
    }

    @Override
    public void runRiBaoTask() {
        System.out.println("日报录入开始——现在时刻：" + CommonUtils.date2String(new Date()));
        List<EndTag> youJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if (youJingList != null && youJingList.size() > 0) {
            for (EndTag youJing : youJingList) {
                try {
                    String code = youJing.getCode();

                    String sql = "Insert into T_Well_Daily_Data "
                            + "(ID, CODE, BENG_JING, HAN_SHUI, DYM, YYMD,TRQXDMD, SMD,QYB,BENG_SHEN,BZSZ,BZXZ,SAVE_TIME,DATE_TIME,CHONG_CHENG,"
                            + "CHONG_CI,MIN_ZAIHE,MAX_ZAIHE,WEIYI,ZAIHE,PHL,PHL1,HDL,CYL,YL,RLJYXSJ,HY,TY,WD,PJDL,PJDY,SXDL,XXDL,SXNH,XXNH,PL,BX,CQL) "
                            + "values (:ID, :CODE, :BENG_JING, :HAN_SHUI, :DYM, :YYMD,:TRQXDMD, :SMD,:QYB,:BENG_SHEN,:BZSZ,:BZXZ,:SAVE_TIME,:DATE_TIME,:CHONG_CHENG,"
                            + ":CHONG_CI,:MIN_ZAIHE,:MAX_ZAIHE,:WEIYI,:ZAIHE,:PHL,:PHL1,:HDL,:CYL,:YL,:RLJYXSJ,:HY,:TY,:WD,:PJDL,:PJDY,:SXDL,:XXDL,:SXNH,:XXNH,:PL,:BX,:CQL)";

                    //源头库中数据
                    Map<String, Object> map = findDataFromYdkByCodeWithBX(code);
                    Float BJ = null, HS = null, BS = null, QYB = null, DMYYMD = null, DCSMD = null, DYM = null, TRQXDMD = null, BX = null, CQL = null;
                    if (map != null) {
                        BJ = Float.parseFloat(((BigDecimal) map.get("bj")).toString());
                        HS = Float.parseFloat(((BigDecimal) map.get("hs")).toString());
                        BS = map.get("bs") == null ? null : Float.parseFloat(((BigDecimal) map.get("bs")).toString());
                        QYB = map.get("qyb") == null ? null : Float.parseFloat(((BigDecimal) map.get("qyb")).toString());
                        DMYYMD = map.get("dmyymd") == null ? null : Float.parseFloat(((BigDecimal) map.get("dmyymd")).toString());
                        DCSMD = map.get("dcsmd") == null ? null : Float.parseFloat(((BigDecimal) map.get("dcsmd")).toString());
                        DYM = map.get("dym") != null ? Float.parseFloat(((BigDecimal) map.get("dym")).toString()) : null;
                        TRQXDMD = map.get("trqxdmd") == null ? null : Float.parseFloat(((BigDecimal) map.get("trqxdmd")).toString());
                        BX = map.get("sjbx") == null ? null : Float.parseFloat(((BigDecimal) map.get("sjbx")).toString());
                        CQL = map.get("rcql") == null ? null : Float.parseFloat(((BigDecimal) map.get("rcql")).toString());
                    }
                    //DYM写入实时库
                    realtimeDataService.putValue(code, RedisKeysEnum.DONG_YE_MIAIN.toString(), DYM == null ? "测不出" : String.valueOf(DYM));

                    String ZAIHE = null, WEIYI = null;

                    Float CHONG_CHENG = null, CHONG_CI = null, ZDZH = null, ZXZH = null,
                            PHL = null, PHL1 = null, HDL = null, CYL = null, YL = null, RLJYXSJ = null, HY = null,
                            TY = null, WD = null, PJDL = null, PJDY = null, SXDL = null, XXDL = null, SXNH = null, XXNH = null, PL = null;

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
                    endTime.set(Calendar.HOUR_OF_DAY, startTime.get(Calendar.HOUR_OF_DAY) + 1);
                    Map<String, Object> dayMap = getDailyData(code, startTime.getTime(), endTime.getTime());
                    Map<String, Object> dayLatestMap = getLatestDailyData(code, startTime.getTime(), endTime.getTime());
                    if (dayMap != null) {
                        CHONG_CHENG = dayMap.get("chong_cheng") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("chong_cheng")).toString());
                        CHONG_CI = dayMap.get("chong_ci") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("chong_ci")).toString());
                        ZDZH = dayMap.get("zdzh") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("zdzh")).toString());
                        ZXZH = dayMap.get("zxzh") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("zxzh")).toString());
                        PHL = dayMap.get("phl") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("phl")).toString());
                        PHL1 = dayMap.get("phl1") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("phl1")).toString());

                        HY = dayMap.get("hy") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("hy")).toString());
                        TY = dayMap.get("ty") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("ty")).toString());
                        WD = dayMap.get("wd") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("wd")).toString());
                        PJDL = dayMap.get("pjdl") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("pjdl")).toString());
                        PJDY = dayMap.get("pjdy") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("pjdy")).toString());
                        SXDL = dayMap.get("sxdl") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("sxdl")).toString());
                        XXDL = dayMap.get("xxdl") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("xxdl")).toString());
                        SXNH = dayMap.get("sxnh") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("sxnh")).toString());
                        XXNH = dayMap.get("xxnh") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("xxnh")).toString());
                        PL = dayMap.get("pl") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("pl")).toString());
                        if (dayLatestMap != null) {
                            HDL = dayLatestMap.get("ljhdl") != null ? Float.parseFloat(((BigDecimal) dayLatestMap.get("ljhdl")).toString()) : null;
                            CYL = dayLatestMap.get("ljcyl") == null ? null : Float.parseFloat(((BigDecimal) dayLatestMap.get("ljcyl")).toString());
                            YL = dayLatestMap.get("ljyl") == null ? null : Float.parseFloat(((BigDecimal) dayLatestMap.get("ljyl")).toString());
                            RLJYXSJ = dayLatestMap.get("ljyxsj") == null ? null : Float.parseFloat(((BigDecimal) dayLatestMap.get("ljyxsj")).toString());
                        }
                    }

                    Calendar c = Calendar.getInstance();
                    c.set(Calendar.MINUTE, 0);
                    c.set(Calendar.SECOND, 0);
                    c.set(Calendar.MILLISECOND, 0);
                    c.set(Calendar.HOUR_OF_DAY, 0);

                    try (Connection con = sql2o.open()) {  			//
                        con.createQuery(sql) //
                                .addParameter("ID", UUID.randomUUID().toString().replace("-", "")) //
                                .addParameter("CODE", code) //
                                .addParameter("BENG_JING", BJ)//
                                .addParameter("HAN_SHUI", HS) //
                                .addParameter("DYM", DYM) //动液面
                                .addParameter("YYMD", DMYYMD) //原油密度
                                .addParameter("TRQXDMD", TRQXDMD) //天然气相对密度
                                .addParameter("SMD", DCSMD)//水密度
                                .addParameter("QYB", QYB)//生产汽油比
                                .addParameter("BENG_SHEN", BS)//泵深
                                .addParameter("BX", BX)//泵效
                                .addParameter("CQL", CQL)//产气量
                                .addParameter("BZSZ", -1)//标准上载
                                .addParameter("BZXZ", -1)//标准下载
                                .addParameter("SAVE_TIME", new Date())//
                                .addParameter("DATE_TIME", c.getTime())//
                                .addParameter("CHONG_CHENG", CHONG_CHENG)//冲程
                                .addParameter("CHONG_CI", CHONG_CI)//冲次
                                .addParameter("MIN_ZAIHE", ZXZH)//最小载荷
                                .addParameter("MAX_ZAIHE", ZDZH)//最大载荷
                                .addParameter("WEIYI", WEIYI)//位移
                                .addParameter("ZAIHE", ZAIHE)//载荷
                                .addParameter("PHL", PHL)//平衡率
                                .addParameter("PHL1", PHL1)//电流平衡率
                                .addParameter("HDL", HDL)//耗电量
                                .addParameter("CYL", CYL)//产液量
                                .addParameter("YL", YL)//油量
                                .addParameter("RLJYXSJ", RLJYXSJ)//运行时间
                                .addParameter("HY", HY)//回压
                                .addParameter("TY", TY)//套压
                                .addParameter("WD", WD)//温度
                                .addParameter("PJDL", PJDL)//平均电流
                                .addParameter("PJDY", PJDY)//平均电压
                                .addParameter("SXDL", SXDL)//上行电流
                                .addParameter("XXDL", XXDL)//下行电流
                                .addParameter("SXNH", SXNH)//上行能耗
                                .addParameter("XXNH", XXNH)//下行能耗
                                .addParameter("PL", PL)//频率
                                //.addParameter("BZ", "")//备注

                                .executeUpdate();//
                    } catch (Exception e) {
                        System.out.println("e:" + e.getMessage());//
                    }


                    Float LJHDL = 0F;
                    //电表读数
                    String currentNum = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.DL_ZX_Z.toString().toLowerCase());
                    try {
                        if (currentNum != null) {
                            String zeroNum = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_LINGSHI_DBDS.toString());
                            if (zeroNum != null) {
                                LJHDL = Float.valueOf(currentNum) - Float.valueOf(zeroNum);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    //写入电表读数
                    realtimeDataService.putValue(code, RedisKeysEnum.RI_LINGSHI_DBDS.toString(), currentNum == null ? "0" : currentNum);

                    String jzrSql = "Insert into QYSCZH.SCY_SRD_YJ "
                            + "(JH, RQ, SCSJ, CC, CC1,TY,HY,RCYL1,DY,SXDL,XXDL,HDL,JKWD,GXSJ,GXR,DBDS,RCYL,HS) "
                            + "values (:JH, :RQ, :SCSJ, :CC, :CC1,:TY,:HY,:RCYL1,:DY,:SXDL,:XXDL,:HDL,:JKWD,:GXSJ,:GXR,:DBDS,:RCYL,:HS)";

                    try (Connection con = sql2o.open()) {  			//
                        con.createQuery(jzrSql) //
                                //                            .addParameter("ID", UUID.randomUUID().toString().replace("-", "")) //
                                .addParameter("JH", code) //井号
                                .addParameter("RQ", c.getTime())//日期
                                .addParameter("SCSJ", RLJYXSJ) //生产时间
                                .addParameter("CC", CHONG_CHENG) //冲程
                                .addParameter("CC1", CHONG_CI) //冲次
                                .addParameter("TY", TY) //套压
                                .addParameter("HY", HY)//回压
                                .addParameter("RCYL1", CYL)//日产液量
                                .addParameter("DY", PJDY)//电压
                                .addParameter("SXDL", SXDL)//上行电流
                                .addParameter("XXDL", XXDL)//下行电流
                                .addParameter("HDL", LJHDL)//耗电量
                                .addParameter("JKWD", WD)//井口温度
                                .addParameter("GXSJ", new Date())//更新时间
                                .addParameter("GXR", "管理员")//更新人
                                .addParameter("DBDS", currentNum)//电表读数
                                .addParameter("RCYL", YL)//日产油量
                                .addParameter("HS", HS)//含水
                                //.addParameter("BZ", "")//备注

                                .executeUpdate();//
                    } catch (Exception e) {
                        System.out.println("e:" + e.getMessage());//
                    }
                } catch (Exception e) {
                }

            }
        }
        System.out.println("日报录入结束——现在时刻：" + CommonUtils.date2String(new Date()));
    }

    /**
     * 从源头库查询数据
     *
     * @param code
     * @return
     */
    private Map<String, Object> findDataFromYdkByCode(String code) {
        String sql = "SELECT * FROM (SELECT s.BJ , s.HS , s.BS , s.QYB, q.DMYYMD , q.DCSMD , y.DYM, t.TRQXDMD "
                + "FROM ys_dba01@ydk s INNER JOIN ys_dab04@ydk q ON s.JH = :CODE AND s.DYDM = q.DYDM LEFT JOIN YS_DCA03@ydk y "
                + " ON s.JH = y.JH LEFT JOIN ys_dab05@ydk t on s.DYDM = t.DYDM WHERE s.BJ IS NOT NULL AND s.HS IS NOT NULL "
                + "ORDER BY s.RQ DESC ) WHERE rownum <= 1";

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
     * 从源头库查询数据(带泵效、产气量)
     *
     * @param code
     * @return
     */
    private Map<String, Object> findDataFromYdkByCodeWithBX(String code) {
        String sql = "SELECT * FROM (SELECT s.BJ , s.HS , s.BS , s.QYB, s.RCQL, q.DMYYMD , q.DCSMD , y.DYM, t.TRQXDMD, z.SJBX "
                + "FROM ys_dba01@ydk s INNER JOIN ys_dab04@ydk q ON s.JH = :CODE AND s.DYDM = q.DYDM LEFT JOIN YS_DCA03@ydk y "
                + " ON s.JH = y.JH LEFT JOIN ys_dab05@ydk t on s.DYDM = t.DYDM LEFT JOIN ys_dca021@ydk z on s.JH=z.JH WHERE s.BJ IS NOT NULL AND s.HS IS NOT NULL "
                + "ORDER BY s.RQ DESC ) WHERE rownum <= 1";

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

//     Float CHONG_CHENG = null, CHONG_CI = null, ZDZH = null, ZXZH = null,
//                        PHL = null, PHL1 = null, HDL = null, CYL = null, YL = null, RLJYXSJ = null, HY = null,
//                        TY = null, WD = null, PJDL = null, PJDY = null, SXDL = null, XXDL = null, SXNH = null, XXNH = null, PL = null;
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
            if (list != null && !list.isEmpty()) {
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
        String code = "GD1-13X818";

        Float LJHDL = 0f;
        //累积用电量
        String currentNum = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.DL_ZX_Z.toString().toLowerCase());
        if (currentNum != null) {
            String zeroNum = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_LINGSHI_DBDS.toString());
            LJHDL = Float.valueOf(currentNum) - Float.valueOf(zeroNum);
        }

        Float LJCYL = 0f;
        Float LJYL = 0f;
        Float LJYXSJ = 0f;
        Float HDL = LJHDL;

        Float CYL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_SS_CYL.toString()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_SS_CYL.toString())) / 12;
        Float YL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_SS_YL.toString()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_SS_YL.toString())) / 12;


        String querySql = "select HDL, CYL, YL, YXSJ from T_Well_Hourly_Data where code=:CODE and DATE_TIME=:DATE_TIME";


        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        while (cal.get(Calendar.HOUR_OF_DAY) != 8) {
            if (cal.get(Calendar.HOUR_OF_DAY) % 2 != 0) {   //偶数点
                cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
                continue;
            }
            try (Connection con = sql2o.open()) {
                log.info("计算日期：" + LocalDateTime.fromCalendarFields(cal).toString("yyyy-MM-dd HH:mm:ss"));
                List<WellHourlyData> list = con.createQuery(querySql)
                        .setAutoDeriveColumnNames(true)
                        .addParameter("CODE", code)
                        .addParameter("DATE_TIME", cal.getTime())
                        .executeAndFetch(WellHourlyData.class);
                if (list != null && !list.isEmpty()) {
                    WellHourlyData data = list.get(0);
                    LJCYL += data.getCyl() == null ? 0f : data.getCyl();
                    log.info("产液量：" + data.getCyl());
                    log.info("累积产液量：" + LJCYL);
                    LJYL += data.getYl() == null ? 0f : data.getYl();
                    log.info("油量：" + data.getYl());
                    log.info("累积油量：" + LJYL);
                    LJYXSJ += data.getYxsj() == null ? 0f : data.getYxsj();
                    HDL -= data.getHdl() == null ? 0f : data.getHdl();
                }
            }
            cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
        }
        LJCYL += CYL == null ? 0f : CYL;
        LJYL += YL == null ? 0f : YL;
        log.info("--------累积产液量：" + LJCYL);
        log.info("--------累积油量：" + LJYL);

//        LJYXSJ += YXSJ == null ? 0f : YXSJ;

        log.info("结束测试……");
    }
}