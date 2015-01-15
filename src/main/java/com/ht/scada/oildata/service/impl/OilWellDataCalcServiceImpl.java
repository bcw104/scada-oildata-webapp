/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.EndTagSubTypeEnum;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.entity.SoeRecord;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.Scheduler;
import static com.ht.scada.oildata.Scheduler.shuiJingList;
import static com.ht.scada.oildata.Scheduler.youCodeList;
import static com.ht.scada.oildata.Scheduler.youJingList;
import com.ht.scada.oildata.service.OilWellDataCalcService;
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
import org.sql2o.data.Row;

/**
 *
 * @author 赵磊 2014-8-14 23:49:22
 */
@Transactional
@Service("oilWellDataCalcService")
public class OilWellDataCalcServiceImpl implements OilWellDataCalcService {

    private static final Logger log = LoggerFactory.getLogger(OilWellDataCalcServiceImpl.class);
    @Autowired
    private RealtimeDataService realtimeDataService;
    @Inject
    protected Sql2o sql2o;
    @Autowired
    private EndTagService endTagService;
    public List<EndTag> youJingList;

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
        log.info("班报录入开始——现在时刻：" + CommonUtils.date2String(new Date()));
        if (Scheduler.youJingList != null && Scheduler.youJingList.size() > 0) {
            for (EndTag youJing : Scheduler.youJingList) {
                try {
                    String code = youJing.getCode();
                    //班报时间段
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
                    int hourOfDay = c.get(Calendar.HOUR_OF_DAY);
                    String SJD = String.valueOf(hourOfDay) + ":00";

                    //第一班先清除累加值
                    try {
                        if (SJD.equals("10:00")) {
                            String BAN_LJYXSJ = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJYXSJ.toString());
                            if (BAN_LJYXSJ != null && (Float.valueOf(BAN_LJYXSJ)) > 0) {
                                realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJYXSJ.toString(), "0");
                            }

                            String BAN_LJCYL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJCYL.toString());
                            if (BAN_LJCYL != null && (Float.valueOf(BAN_LJCYL)) > 0) {
                                realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJCYL.toString(), "0");
                            }

                            String BAN_LJYL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJYL.toString());
                            if (BAN_LJYL != null && (Float.valueOf(BAN_LJYL)) > 0) {
                                realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJYL.toString(), "0");
                            }

                            String BAN_LJHDL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJHDL.toString());
                            if (BAN_LJHDL != null && (Float.valueOf(BAN_LJHDL)) > 0) {
                                realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJHDL.toString(), "0");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJYXSJ.toString(), "0");
                        realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJCYL.toString(), "0");
                        realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJYL.toString(), "0");
                        realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJHDL.toString(), "0");
                    }

                    String sql = "insert into T_Well_Hourly_Data "
                            + "(ID, CODE, BENG_JING, HAN_SHUI, DYM, YYMD,TRQXDMD, SMD,QYB,BENG_SHEN,BZSZ,BZXZ,SAVE_TIME,DATE_TIME,CHONG_CHENG,"
                            + "CHONG_CI,MIN_ZAIHE,MAX_ZAIHE,WEIYI,ZAIHE,PHL,HDL,CYL,YL,YXSJ,LJHDL,LJCYL,LJYL,LJYXSJ,HY,TY,WD,PJDL,PJDY,SXDL,XXDL,PL,SXNH,XXNH,SXGL,XXGL,SJD,PHL1) "
                            + "values (:ID, :CODE, :BENG_JING, :HAN_SHUI, :DYM,:YYMD, :TRQXDMD, :SMD,:QYB,:BENG_SHEN,:BZSZ,:BZXZ,:SAVE_TIME,:DATE_TIME,:CHONG_CHENG,"
                            + ":CHONG_CI,:MIN_ZAIHE,:MAX_ZAIHE,:WEIYI,:ZAIHE,:PHL,:HDL,:CYL,:YL,:YXSJ,:LJHDL,:LJCYL,:LJYL,:LJYXSJ,:HY,:TY,:WD,:PJDL,:PJDY,:SXDL,:XXDL,:PL,:SXNH,:XXNH,:SXGL,:XXGL,:SJD,:PHL1)";
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
                    //实时库中功图数据
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
                    //实时库中其他数据
                    String rtHy = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.HUI_YA.toString().toLowerCase());
                    String rtTy = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.TAO_YA.toString().toLowerCase());
                    String rtWd = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.JING_KOU_WEN_DU.toString().toLowerCase());
                    String rtPjdl = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.I_B.toString().toLowerCase());
                    String rtPjdy = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.U_B.toString().toLowerCase());
                    String rtPl = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.GV_ZB.toString().toLowerCase());
                    String rtSXDL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DL_SHANG.toString());
                    String rtXXDL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DL_XIA.toString());
                    String rtSXNH = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.SHANG_NH.toString());
                    String rtXXNH = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.XIA_NH.toString());
                    String rtSXGL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GL_SHANG.toString());
                    String rtXXGL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GL_XIA.toString());
                    Float HY = rtHy == null ? null : Float.valueOf(rtHy);
                    Float TY = rtTy == null ? null : Float.valueOf(rtTy);
                    Float WD = rtWd == null ? null : Float.valueOf(rtWd);
                    Float PJDL = rtPjdl == null ? null : Float.valueOf(rtPjdl);
                    Float PJDY = rtPjdy == null ? null : Float.valueOf(rtPjdy);
                    Float PL = rtPl == null ? null : Float.valueOf(rtPl);
                    Float SXDL = rtSXDL == null ? null : Float.valueOf(rtSXDL);
                    Float XXDL = rtXXDL == null ? null : Float.valueOf(rtXXDL);
                    Float SXNH = rtSXNH == null ? null : Float.valueOf(rtSXNH);
                    Float XXNH = rtXXNH == null ? null : Float.valueOf(rtXXNH);
                    Float SXGL = rtSXGL == null ? null : Float.valueOf(rtSXGL);
                    Float XXGL = rtXXGL == null ? null : Float.valueOf(rtXXGL);

                    //*************************************开始  计算产量*************************
                    //TODU:用上一班的两个小时内功图求平均值
                    Float CYL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.WETK_RI_SS_CYL.toString()) == null ? 0f : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.WETK_RI_SS_CYL.toString())) / 12;
                    Float YL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.WETK_RI_SS_YL.toString()) == null ? 0f : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.WETK_RI_SS_YL.toString())) / 12;

                    //上一班的累计值
                    float ljcylValue = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJCYL.toString()) == null ? 0f : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJCYL.toString()));
                    float ljylValue = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJYL.toString()) == null ? 0f : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJYL.toString()));

                    Float LJCYL = ljcylValue + CYL;
                    Float LJYL = ljylValue + YL;

                    //更新累积产液量、液量
                    realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJCYL.toString(), String.valueOf(LJCYL));
                    realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJYL.toString(), String.valueOf(LJYL));

                    //*************************************结束  计算产量*************************

                    //***************************开始  计算耗电量*******************
                    Float LJHDL = 0f;   //累积耗电量
                    Float HDL = 0f;     //本班耗电量
                    //当前电表读数
                    String currentNum = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.DL_ZX_Z.toString().toLowerCase());
                    //上一班累积耗电量
                    String rtLjhdl = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJHDL.toString());
                    Float banLJHDL = rtLjhdl == null ? 0f : Float.valueOf(rtLjhdl);
                    try {
                        if (currentNum != null) {
                            //零时（8点）电表读数
                            String zeroNum = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_LINGSHI_DBDS.toString());
                            if (zeroNum != null) {
                                LJHDL = Float.valueOf(currentNum) - Float.valueOf(zeroNum);
                                HDL = LJHDL - banLJHDL;
                            }
                            //更新班累积耗电量
                            realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJHDL.toString(), String.valueOf(LJHDL));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    //***************************结束  计算耗电量*******************

                    //***********************开始   计算螺杆泵液量*****************
                    String lljsyNum = null;
                    if (youJing.getSubType().equals(EndTagSubTypeEnum.LUO_GAN_BENG.toString())) {
                        //流量积算仪读数
                        lljsyNum = realtimeDataService.getEndTagVarInfo(code, "lljsy1_ljll");
                        //上一班累积流量
                        String ljLljsy = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJCYL.toString());
                        Float banLljsy = ljLljsy == null ? 0f : Float.valueOf(ljLljsy);
                        if (lljsyNum != null) {
                            //零时8点积算仪累积流量
                            String zeroJsyNum = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_LINGSHI_LLJSYLL.toString());
                            LJCYL = Float.valueOf(lljsyNum) - Float.valueOf(zeroJsyNum == null ? "0" : zeroJsyNum);
                            CYL = LJCYL - banLljsy;
                            LJYL = LJCYL * (1 - (HS == null ? 0 : HS / 100f));
                            YL = CYL * (1 - (HS == null ? 0 : HS / 100f));
                            //更新流量自控仪累积产液量
                            realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJCYL.toString(), String.valueOf(LJCYL));
                            realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJYL.toString(), String.valueOf(LJYL));
                        }
                    }
                    //***********************结束   计算螺杆泵液量*****************


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
                        e.printStackTrace();
                    }

                    float ljscsj = 0;
                    try {
                        int hour2 = LJYXSJ == null ? 0 : (LJYXSJ.intValue() / 60);
                        float minite2 = LJYXSJ % 60;
                        ljscsj = hour2 + minite2 / 100;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    //更新运行时间
                    realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJYXSJ.toString(), String.valueOf(LJYXSJ));
                    //***************************结束  计算运行时间****************

                    //*************************开始  计算平衡率*****************************************
                    Float PHL = null;   //功率平衡率
                    Float PHL1 = null;  //电流平衡率
                    if (SXGL != null && XXGL != null) {
                        if (SXGL == 0) {
                            PHL = Float.MAX_VALUE;
                        } else {
                            PHL = XXGL / SXGL;
                        }
                    }
                    if (SXDL != null && XXDL != null && Math.abs(SXDL) > 0) {
                        PHL1 = Math.abs(XXDL) / Math.abs(SXDL);
                    }
                    //*************************结束  计算平衡率*****************************************

                    //更新累积及预估
                    realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_LEIJI_CYL.toString(), String.valueOf(LJCYL));
                    realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_LEIJI_YL.toString(), String.valueOf(LJYL));
                    realtimeDataService.putValue(code, RedisKeysEnum.RI_LEIJI_HDL.toString(), String.valueOf(LJHDL));
                    int time = 2;
                    if (hourOfDay <= 22 && hourOfDay > 8) {
                        time = hourOfDay - 8;
                    } else {
                        time = 16 + hourOfDay;
                    }
                    realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_YUGU_CYL.toString(), String.valueOf(LJCYL * 24 / time));
                    realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_YUGU_YL.toString(), String.valueOf(LJYL * 24 / time));
                    realtimeDataService.putValue(code, RedisKeysEnum.RI_YUGU_HDL.toString(), String.valueOf(LJHDL * 24 / time));

                    //最后一班写入电表读数
                    if (SJD.equals("8:00")) {
                        realtimeDataService.putValue(code, RedisKeysEnum.RI_LINGSHI_DBDS.toString(), currentNum == null ? "0" : currentNum);
                        //螺杆泵流量积算仪数据
                        if (youJing.getSubType().equals(EndTagSubTypeEnum.LUO_GAN_BENG.toString())) {
                            realtimeDataService.putValue(code, RedisKeysEnum.RI_LINGSHI_LLJSYLL.toString(), lljsyNum == null ? "0" : lljsyNum);
                        }
                    }

                    String CTJ = realtimeDataService.getEndTagVarInfo(youJing.getCode(), RedisKeysEnum.CTJ.toString());
                    if (CTJ != null && !"".equals(CTJ.trim())) {
                        CHONG_CHONG = CHONG_CI = ZXZH = ZDZH = PHL = HDL = CYL = YL = YXSJ = LJHDL = LJCYL = LJYL = LJYXSJ = HY = TY = WD = PJDL = PJDY = SXDL = XXDL = PL = SXNH = XXNH = SXGL = XXGL = PHL1 = null;
                        WEIYI = ZAIHE = null;
                        scsj = 0;
                    }

                    //计算油量
                    Float jsyl = null;
                    if (CYL != null && HS != null) {
                        jsyl = CYL * (1 - HS / 100);
                    }

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
                                .addParameter("YL", jsyl)//油量
                                .addParameter("LJHDL", LJHDL)//累积耗电量
                                .addParameter("LJCYL", LJCYL)//累积产液量
                                .addParameter("LJYL", LJYL)//累积液量
                                .addParameter("YXSJ", scsj)//运行时间
                                .addParameter("LJYXSJ", ljscsj)//累积运行时间
                                .addParameter("HY", HY)//回压
                                .addParameter("TY", TY)//套压
                                .addParameter("WD", WD)//温度
                                .addParameter("PJDL", PJDL)//平均电流
                                .addParameter("PJDY", PJDY)//平均电压
                                .addParameter("SXDL", SXDL)//上行电流
                                .addParameter("XXDL", XXDL)//下行电流
                                .addParameter("SXNH", SXNH)//上行能耗
                                .addParameter("XXNH", XXNH)//下行能耗
                                .addParameter("SXGL", SXGL)//上行功率
                                .addParameter("XXGL", XXGL)//下行功率
                                .addParameter("PL", PL)//频率
                                .addParameter("SJD", SJD)//时间段
                                .executeUpdate();
                    } catch (Exception e) {
                        log.info("处理井：" + code + "出现异常！" + e.toString());
                        continue;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }
        }
        log.info("班报录入结束——现在时刻：" + CommonUtils.date2String(new Date()));
    }

    @Override
    public void runRiBaoTask() {
        log.info("日报录入开始——现在时刻：" + CommonUtils.date2String(new Date()));

        youJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        List<EndTag> syjList = endTagService.getByType(EndTagTypeEnum.SHUI_YUAN_JING.toString());
        if (youJingList != null && youJingList.size() > 0) {
            for (EndTag youJing : youJingList) {
                try {
                    String code = youJing.getCode();

                    String sql = "Insert into T_Well_Daily_Data "
                            + "(ID, CODE, BENG_JING, HAN_SHUI, DYM, YYMD,TRQXDMD, SMD,QYB,BENG_SHEN,BZSZ,BZXZ,SAVE_TIME,DATE_TIME,CHONG_CHENG,"
                            + "CHONG_CI,MIN_ZAIHE,MAX_ZAIHE,WEIYI,ZAIHE,PHL,PHL1,HDL,CYL,YL,RLJYXSJ,HY,TY,WD,PJDL,PJDY,SXDL,XXDL,SXNH,XXNH,SXGL,XXGL,PL,BX,CQL,XS) "
                            + "values (:ID, :CODE, :BENG_JING, :HAN_SHUI, :DYM, :YYMD,:TRQXDMD, :SMD,:QYB,:BENG_SHEN,:BZSZ,:BZXZ,:SAVE_TIME,:DATE_TIME,:CHONG_CHENG,"
                            + ":CHONG_CI,:MIN_ZAIHE,:MAX_ZAIHE,:WEIYI,:ZAIHE,:PHL,:PHL1,:HDL,:CYL,:YL,:RLJYXSJ,:HY,:TY,:WD,:PJDL,:PJDY,:SXDL,:XXDL,:SXNH,:XXNH,:SXGL,:XXGL,:PL,:BX,:CQL,:XS)";

                    //源头库中数据
                    Map<String, Object> map = findDataFromYdkByCodeForDaylyTask(code);
                    Float BJ = null, HS = 0f, BS = null, QYB = null, DMYYMD = null, DCSMD = null, DYM = null, TRQXDMD = null, BX = null, CQL = null;
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
                    //动液面、含水率写入实时库
                    realtimeDataService.putValue(code, RedisKeysEnum.DONG_YE_MIAIN.toString(), DYM == null ? "测不出" : String.valueOf(DYM));
                    realtimeDataService.putValue(code, RedisKeysEnum.HAN_SHUI_LV.toString(), HS == null ? "0" : String.valueOf(HS / 100));
                    realtimeDataService.putValue(code, RedisKeysEnum.BENG_JING.toString(), BJ == null ? "0" : String.valueOf(BJ));
                    realtimeDataService.putValue(code, RedisKeysEnum.BENG_SHEN.toString(), BS == null ? "0" : String.valueOf(BS));

                    //暂未存储
                    String ZAIHE = null, WEIYI = null;
                    //从班表里计算平均值
                    Float CHONG_CHENG = null, CHONG_CI = null, ZDZH = null, ZXZH = null, HY = null, TY = null, WD = null, PL = null,
                            PJDL = null, PJDY = null, SXDL = null, XXDL = null, SXNH = null, XXNH = null, SXGL = null, XXGL = null;
                    //从日报表里计算的平均值
                    Float YSL = null;
                    //平衡度计算值
                    Float PHL = null, PHL1 = null;
                    //从实时库中获取已经累积的值
                    Float HDL = null, CYL = 0f, YL = null, RLJYXSJ = null;

                    Calendar startTime = Calendar.getInstance();
                    Calendar endTime = Calendar.getInstance();
                    startTime.set(Calendar.MINUTE, 0);
                    startTime.set(Calendar.SECOND, 0);
                    startTime.set(Calendar.MILLISECOND, 0);
                    startTime.set(Calendar.DAY_OF_MONTH, startTime.get(Calendar.DAY_OF_MONTH) - 1);
                    endTime.set(Calendar.MINUTE, 0);
                    endTime.set(Calendar.SECOND, 0);
                    endTime.set(Calendar.MILLISECOND, 0);
                    Map<String, Object> dayMap = getAvgDailyData(code, startTime.getTime(), endTime.getTime());
                    if (dayMap != null) {
                        HY = dayMap.get("hy") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("hy")).toString());
                        TY = dayMap.get("ty") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("ty")).toString());
                        WD = dayMap.get("wd") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("wd")).toString());
                        PJDL = dayMap.get("pjdl") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("pjdl")).toString());
                        PJDY = dayMap.get("pjdy") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("pjdy")).toString());
                        SXDL = dayMap.get("sxdl") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("sxdl")).toString());
                        XXDL = dayMap.get("xxdl") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("xxdl")).toString());
                        SXNH = dayMap.get("sxnh") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("sxnh")).toString());
                        XXNH = dayMap.get("xxnh") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("xxnh")).toString());
                        SXGL = dayMap.get("sxgl") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("sxgl")).toString());
                        XXGL = dayMap.get("xxgl") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("xxgl")).toString());
                        PL = dayMap.get("pl") == null ? null : Float.parseFloat(((BigDecimal) dayMap.get("pl")).toString());
                        //计算平衡度
//                        if (SXGL != null && XXGL != null) {
//                            if (SXGL == 0) {
//                                PHL = Float.MAX_VALUE;
//                            } else {
//                                PHL = XXGL / SXGL;
//                            }
//                        }
//                        if (SXDL != null && XXDL != null && Math.abs(SXDL) > 0) {
//                            PHL1 = Math.abs(XXDL) / Math.abs(SXDL);
//                        }
                        //螺杆泵产液量
//                        CYL = dayMap.get("cyl") == null ? 0f : Float.parseFloat(((BigDecimal) dayMap.get("cyl")).toString());
                    }

                    if (youJing.getSubType().equals(EndTagSubTypeEnum.YOU_LIANG_SHI.toString()) || youJing.getSubType().equals(EndTagSubTypeEnum.GAO_YUAN_JI.toString())) {
                        Map<String, Object> gtfxMap = getAvgGTFXData(code, startTime.getTime(), endTime.getTime());
                        if (gtfxMap != null) {
                            CHONG_CHENG = gtfxMap.get("chong_cheng") == null ? null : Float.parseFloat(((BigDecimal) gtfxMap.get("chong_cheng")).toString());
                            CHONG_CI = gtfxMap.get("chong_ci") == null ? null : Float.parseFloat(((BigDecimal) gtfxMap.get("chong_ci")).toString());
                            ZDZH = gtfxMap.get("zdzh") == null ? null : Float.parseFloat(((BigDecimal) gtfxMap.get("zdzh")).toString());
                            ZXZH = gtfxMap.get("zxzh") == null ? null : Float.parseFloat(((BigDecimal) gtfxMap.get("zxzh")).toString());
                            CYL = gtfxMap.get("rcyl1") == null ? 0f : Float.parseFloat(((BigDecimal) gtfxMap.get("rcyl1")).toString());
                        }
                    } else {//螺杆泵液量采用班累积
                        String rtCYL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJCYL.toString());
                        CYL = rtCYL == null ? 0f : Float.valueOf(rtCYL);
                    }


                    //暂时从实时库中取值
                    try {
                        if (getRealData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) > 0) {
//                            CHONG_CHENG = getRealData(code, VarSubTypeEnum.CHONG_CHENG.toString().toLowerCase());
//                            CHONG_CI = getRealData(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase());
//                            ZDZH = getRealData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase());
//                            ZXZH = getRealData(code, VarSubTypeEnum.ZUI_XIAO_ZAI_HE.toString().toLowerCase());
                            BX = getRealData(code, RedisKeysEnum.BENG_XIAO.toString());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    Calendar startTime1 = Calendar.getInstance();
                    Calendar endTime1 = Calendar.getInstance();
                    endTime1.set(Calendar.MINUTE, 0);
                    endTime1.set(Calendar.SECOND, 0);
                    endTime1.set(Calendar.MILLISECOND, 0);
                    endTime1.set(Calendar.HOUR_OF_DAY, 0);
                    endTime1.set(Calendar.DAY_OF_MONTH, 0);
                    try {
                        Map<String, Object> monthMap = getAvgMonthlyData(code, endTime1.getTime(), startTime1.getTime());
                        if (monthMap != null) {
                            YSL = monthMap.get("rljyxsj") == null ? 0 : Float.parseFloat(((BigDecimal) monthMap.get("rljyxsj")).toString()) / 24f;
                            realtimeDataService.putValue(code, RedisKeysEnum.YSL.toString(), String.valueOf(YSL));    //月时率
                        }
                    } catch (Exception e) {
                        log.error(youJing.getCode() + "月时率计算错误！");
                    }

                    String rtYXSJ = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJYXSJ.toString());
//                    String rtCYL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJCYL.toString());
//                    String rtYL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJYL.toString());
                    String rtHDL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJHDL.toString());

                    RLJYXSJ = rtYXSJ == null ? null : Float.valueOf(rtYXSJ);
//                    CYL = rtCYL == null ? null : Float.valueOf(rtCYL);
//                    YL = rtYL == null ? null : Float.valueOf(rtYL);
                    HDL = rtHDL == null ? 0f : Float.valueOf(rtHDL);

                    
                    Calendar c = Calendar.getInstance();
                    c.set(Calendar.MINUTE, 0);
                    c.set(Calendar.SECOND, 0);
                    c.set(Calendar.MILLISECOND, 0);
                    c.set(Calendar.HOUR_OF_DAY, 0);

                    //23.55以上认为是24
                    if (RLJYXSJ != null && RLJYXSJ >= 1435) {
                        RLJYXSJ = 1440f;
                    }

                    float scsj = 0;
                    try {
                        int hour = RLJYXSJ == null ? 0 : (RLJYXSJ.intValue() / 60);
                        float minite = RLJYXSJ % 60;
                        scsj = hour + minite / 100;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    //处理长停井
                    String CTJ = realtimeDataService.getEndTagVarInfo(youJing.getCode(), RedisKeysEnum.CTJ.toString());
                    if (CTJ != null && !"".equals(CTJ.trim())) {
                        CHONG_CHENG = CHONG_CI = ZXZH = ZDZH = PHL = HDL = HY = TY = WD = PJDL = PJDY = SXDL = XXDL = PL = SXNH = XXNH = SXGL = XXGL = PHL1 = null;
                        CYL = YL = 0F;
                        WEIYI = ZAIHE = null;
                        scsj = 0;
                    }

                    Float bdxs = (youJing.getBdxs() == null || youJing.getBdxs() <= 0) ? 1f : youJing.getBdxs();  //标定系数
                    
                    realtimeDataService.putValue(code, RedisKeysEnum.WETK_ZR_CYL.toString(), String.valueOf(CYL * bdxs)); //昨日产液量
                    realtimeDataService.putValue(code, RedisKeysEnum.WETK_ZR_YL.toString(), String.valueOf(CYL * bdxs * (1 - HS / 100)));    //昨日产油量

                    try (Connection con = sql2o.open()) {
                        con.createQuery(sql)
                                .addParameter("ID", UUID.randomUUID().toString().replace("-", ""))
                                .addParameter("CODE", code) //井号
                                .addParameter("BENG_JING", BJ)//泵径
                                .addParameter("HAN_SHUI", HS) //含水
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
                                .addParameter("SAVE_TIME", new Date())//保存时间
                                .addParameter("DATE_TIME", c.getTime())//数据时间
                                .addParameter("CHONG_CHENG", CHONG_CHENG)//冲程
                                .addParameter("CHONG_CI", CHONG_CI)//冲次
                                .addParameter("MIN_ZAIHE", ZXZH)//最小载荷
                                .addParameter("MAX_ZAIHE", ZDZH)//最大载荷
                                .addParameter("WEIYI", WEIYI)//位移
                                .addParameter("ZAIHE", ZAIHE)//载荷
                                .addParameter("PHL", PHL)//平衡率
                                .addParameter("PHL1", PHL1)//电流平衡率
                                .addParameter("HDL", HDL)//耗电量
                                .addParameter("CYL", CYL * bdxs)//产液量
                                .addParameter("YL", CYL * bdxs * (1 - HS / 100))//油量
                                .addParameter("RLJYXSJ", scsj)//运行时间
                                .addParameter("HY", HY)//回压
                                .addParameter("TY", TY)//套压
                                .addParameter("WD", WD)//温度
                                .addParameter("PJDL", PJDL)//平均电流
                                .addParameter("PJDY", PJDY)//平均电压
                                .addParameter("SXDL", SXDL)//上行电流
                                .addParameter("XXDL", XXDL)//下行电流
                                .addParameter("SXNH", SXNH)//上行能耗
                                .addParameter("XXNH", XXNH)//下行能耗
                                .addParameter("SXGL", SXGL)//上行功率
                                .addParameter("XXGL", XXGL)//下行功率
                                .addParameter("PL", PL)//频率
                                .addParameter("XS", bdxs)//系数
                                .executeUpdate();
                    } catch (Exception e) {
                        e.printStackTrace();
                        continue;
                    }

                    //零时电表读数(班报中已更新)
                    String zeroNum = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_LINGSHI_DBDS.toString());

                    Float RCSL1 = null;

                    //************************开始 处理掺水***********************************
                    try {
                        if (syjList != null && !syjList.isEmpty()) {
                            for (EndTag csJing : syjList) {
                                if (csJing.getCode().replace("-CS", "").trim().equals(code)) {//有掺水
                                    try {
                                        String extConfigInfo = csJing.getExtConfigInfo();		// 获得扩展信息 
                                        if (extConfigInfo != null && !"".equals(extConfigInfo.trim())) {
                                            String[] framesLine = extConfigInfo.trim().replaceAll("\\r", "").split("\\n");// 替换字符串									
                                            for (String varName : framesLine) {
                                                //yc|zsyl-注水压力|psj_z1-10-b|zky12_zsyl 
                                                if (varName.contains("yc|")) {
                                                    String varNames[] = varName.trim().split("\\|");
                                                    String varName1 = varNames[1];
                                                    String codeName = varNames[2];
                                                    String varNameStr = varNames[3];
                                                    if (varName1.contains("ljll-")) { // 累积流量
                                                        String ljllValue = realtimeDataService.getEndTagVarInfo(codeName, varNameStr);
                                                        if (ljllValue != null) {
                                                            String zeroCslNum = realtimeDataService.getEndTagVarInfo(code, "RCSL1");
                                                            RCSL1 = ((zeroCslNum == null) || (CommonUtils.formatFloat(Float.parseFloat(zeroCslNum), 2) == 0f)) ? 0 : (CommonUtils.formatFloat(Float.parseFloat(ljllValue), 2) - CommonUtils.formatFloat(Float.parseFloat(zeroCslNum), 2));
                                                            realtimeDataService.putValue(code, "RCSL1", ljllValue);
                                                        }
                                                        log.info(code + " 掺水处理完毕！");
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    } catch (Exception e) {
                                        log.error(code + ":" + e.toString());
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("掺水处理异常：" + e.toString());
                    }

                    //************************结束 处理掺水***********************************

                    String jzrSql = "Insert into QYSCZH.SCY_SRD_YJ "
                            + "(JH, RQ, SCSJ, CC, CC1,TY,HY,RCYL1,DY,SXDL,XXDL,HDL,JKWD,GXSJ,GXR,DBDS,RCYL,HS,RCSL1) "
                            + "values (:JH, :RQ, :SCSJ, :CC, :CC1,:TY,:HY,:RCYL1,:DY,:SXDL,:XXDL,:HDL,:JKWD,:GXSJ,:GXR,:DBDS,:RCYL,:HS,:RCSL1)";

                    try (Connection con = sql2o.open()) { //
                        con.createQuery(jzrSql) //
                                .addParameter("JH", code) //井号
                                .addParameter("RQ", c.getTime())//日期
                                .addParameter("SCSJ", scsj) //生产时间
                                .addParameter("CC", CHONG_CHENG) //冲程
                                .addParameter("CC1", CHONG_CI) //冲次
                                .addParameter("TY", TY) //套压
                                .addParameter("HY", HY)//回压
                                .addParameter("RCYL1", CYL * bdxs)//日产液量
                                .addParameter("DY", PJDY)//电压
                                .addParameter("SXDL", SXDL)//上行电流
                                .addParameter("XXDL", XXDL)//下行电流
                                .addParameter("HDL", HDL)//耗电量
                                .addParameter("JKWD", WD)//井口温度
                                .addParameter("GXSJ", new Date())//更新时间
                                .addParameter("GXR", "管理员")//更新人
                                .addParameter("DBDS", zeroNum == null ? null : Float.valueOf(zeroNum))//电表读数
                                .addParameter("RCYL", CYL * bdxs * (1 - HS / 100))//日产油量
                                .addParameter("HS", HS)//含水
                                .addParameter("RCSL1", RCSL1)//日掺水量
                                .executeUpdate();
                    } catch (Exception e) {
                        log.info(code + "发生异常！");
                        e.printStackTrace();
                        continue;
                    }

                    realtimeDataService.putValue(code, RedisKeysEnum.ZR_HDL.toString(), String.valueOf(HDL));  //昨日耗电量

                    //清除班累积运算值
                    realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJYXSJ.toString(), "0");
                    realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJCYL.toString(), "0");
                    realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJYL.toString(), "0");
                    realtimeDataService.putValue(code, RedisKeysEnum.BAN_LJHDL.toString(), "0");
                    realtimeDataService.putValue(code, RedisKeysEnum.TJCS.toString(), "0");

                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
                log.info(youJing.getCode() + "日报计算结束！");
            }
        }
        log.info("日报录入结束——现在时刻：" + CommonUtils.date2String(new Date()));
    }

    /**
     * 从源头库查询数据 泵径、含水、泵深、气油比、地面原油密度、地层水密度、动液面、天然气相对密度
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
     * 从源头库查询数据(带泵效、产气量) 泵径、含水、泵深、气油比、日产气量、地面原油密度、地层水密度、动液面、天然气相对密度、泵效
     *
     * @param code
     * @return
     */
    private Map<String, Object> findDataFromYdkByCodeForDaylyTask(String code) {
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

    /**
     * 从T_WELL_HOURLY_DATA中计算日数据
     *
     * @param code
     * @param startTime
     * @param endTime
     * @return
     */
    private Map<String, Object> getAvgDailyData(String code, Date startTime, Date endTime) {
        String sql = "SELECT "
                //                + "avg(CHONG_CHENG) as CHONG_CHENG, "
                //                + "avg(CHONG_CI) as CHONG_CI, "
                //                + "avg(MAX_ZAIHE) as ZDZH, "
                //                + "avg(MIN_ZAIHE) as ZXZH, "
                //                + "avg(PHL) as PHL, "
                //                + "avg(PHL1) as PHL1, "
                + "avg(HY) as HY, "
                + "avg(TY) as TY, "
                + "avg(WD) as WD, "
                + "avg(PJDL) as PJDL, "
                + "avg(PJDY) as PJDY, "
                + "avg(SXDL) as SXDL, "
                + "avg(XXDL) as XXDL, "
                + "avg(SXNH) as SXNH, "
                + "avg(XXNH) as XXNH, "
                + "avg(SXGL) as SXGL, "
                + "avg(XXGL) as XXGL, "
                + "avg(CYL) as CYL, "
                + "avg(PL) as PL "
                + " from T_WELL_HOURLY_DATA t where code=:CODE and DATE_TIME>=:startTime and DATE_TIME<=:endTime";

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

    private Map<String, Object> getAvgGTFXData(String code, Date startTime, Date endTime) {
        String sql = "SELECT avg(CC) as CHONG_CHENG, "
                + "avg(CC1) as CHONG_CI, "
                + "avg(ZDZH) as ZDZH, "
                + "avg(ZXZH) as ZXZH, "
                + "avg(RCYL1) as RCYL1 "
                + " from QYSCZH.SCY_SGT_GTFX t where JH=:CODE and CJSJ>=:startTime and CJSJ<=:endTime and JSBZ=0";

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

    private Map<String, Object> getAvgMonthlyData(String code, Date startTime, Date endTime) {
        String sql = "SELECT avg(RLJYXSJ) as RLJYXSJ "
                + " from T_WELL_DAILY_DATA t where code=:CODE and DATE_TIME>=:startTime and DATE_TIME<=:endTime ";

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
     * 获取运行时间
     *
     * @param code
     * @return
     */
    private float getYxsj(String code) {
        float yxsj = 120f;
        float tzsj = 0f;
        String sql = "SELECT count(IS_ON) as is_on "
                + " from T_OIL_WELL_CALC_DATA where code=:CODE and IS_ON = 0 ";

        List<Map<String, Object>> list;
        try (Connection con = sql2o.open()) {
            list = con.createQuery(sql)
                    .addParameter("CODE", code)
                    .executeAndFetchTable().asList();
        }
        if (list != null && !list.isEmpty()) {
            tzsj = list.get(0).get("is_on") == null ? 0f : Float.parseFloat(((BigDecimal) list.get(0).get("is_on")).toString());
        }
        return yxsj - tzsj;
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

        Calendar startTime = Calendar.getInstance();
        Calendar endTime = Calendar.getInstance();
        startTime.set(Calendar.MINUTE, 0);
        startTime.set(Calendar.SECOND, 0);
        startTime.set(Calendar.MILLISECOND, 0);
        endTime.set(Calendar.MINUTE, 0);
        endTime.set(Calendar.SECOND, 0);
        endTime.set(Calendar.MILLISECOND, 0);
        startTime.set(Calendar.HOUR_OF_DAY, 1);
        log.info(sdf.format(startTime.getTime()));
        startTime.set(Calendar.HOUR_OF_DAY, startTime.get(Calendar.HOUR_OF_DAY) - 2);
        log.info(sdf.format(startTime.getTime()));

    }

    @Override
    public void testMathod() {
        log.info("开始测试……");
//        String code = "GD1-13X818";
//
//        Float LJHDL = 0f;
//        //累积用电量
//        String currentNum = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.DL_ZX_Z.toString().toLowerCase());
//        if (currentNum != null) {
//            String zeroNum = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_LINGSHI_DBDS.toString());
//            LJHDL = Float.valueOf(currentNum) - Float.valueOf(zeroNum);
//        }
//
//        Float LJCYL = 0f;
//        Float LJYL = 0f;
//        Float LJYXSJ = 0f;
//        Float HDL = LJHDL;
//
//        Float CYL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_SS_CYL.toString()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_SS_CYL.toString())) / 12;
//        Float YL = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_SS_YL.toString()) == null ? null : Float.valueOf(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.RI_SS_YL.toString())) / 12;
//
//
//        String querySql = "select HDL, CYL, YL, YXSJ from T_Well_Hourly_Data where code=:CODE and DATE_TIME=:DATE_TIME";
//
//
//        Calendar cal = Calendar.getInstance();
//        cal.set(Calendar.MINUTE, 0);
//        cal.set(Calendar.SECOND, 0);
//        cal.set(Calendar.MILLISECOND, 0);
//        while (cal.get(Calendar.HOUR_OF_DAY) != 8) {
//            if (cal.get(Calendar.HOUR_OF_DAY) % 2 != 0) {   //偶数点
//                cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
//                continue;
//            }
//            try (Connection con = sql2o.open()) {
//                log.info("计算日期：" + LocalDateTime.fromCalendarFields(cal).toString("yyyy-MM-dd HH:mm:ss"));
//                List<WellHourlyData> list = con.createQuery(querySql)
//                        .setAutoDeriveColumnNames(true)
//                        .addParameter("CODE", code)
//                        .addParameter("DATE_TIME", cal.getTime())
//                        .executeAndFetch(WellHourlyData.class);
//                if (list != null && !list.isEmpty()) {
//                    WellHourlyData data = list.get(0);
//                    LJCYL += data.getCyl() == null ? 0f : data.getCyl();
//                    log.info("产液量：" + data.getCyl());
//                    log.info("累积产液量：" + LJCYL);
//                    LJYL += data.getYl() == null ? 0f : data.getYl();
//                    log.info("油量：" + data.getYl());
//                    log.info("累积油量：" + LJYL);
//                    LJYXSJ += data.getYxsj() == null ? 0f : data.getYxsj();
//                    HDL -= data.getHdl() == null ? 0f : data.getHdl();
//                }
//            }
//            cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) - 1);
//        }
//        LJCYL += CYL == null ? 0f : CYL;
//        LJYL += YL == null ? 0f : YL;
//        log.info("--------累积产液量：" + LJCYL);
//        log.info("--------累积油量：" + LJYL);

//        LJYXSJ += YXSJ == null ? 0f : YXSJ;

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
        String SJD = String.valueOf(c.get(Calendar.HOUR_OF_DAY)) + ":00";

        log.info("结束测试……");
    }

    /**
     * 处理小数点精度
     *
     * @param f
     * @param num
     * @return
     */
    private Float handleValue(Float f, int num) {
        if (f == null) {
            return null;
        }
        return new BigDecimal(f).setScale(num,
                BigDecimal.ROUND_HALF_UP).floatValue();
    }

    /**
     * 获取时间段内的产液量和产油量
     *
     * @param JH
     * @param startTime
     * @param endTime
     * @return
     */
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
            if (list != null && !list.isEmpty() && list.get(0) != null) {
                result[0] = list.get(0).getFloat("rcyl1");    //日产液量
                result[1] = list.get(0).getFloat("rcyl");     //日产油量
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return result;
    }
}