/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.Config;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.CommonUtils;
import com.ht.scada.oildata.calc.GTCalc;
import com.ht.scada.oildata.calc.GTDataComputerProcess;
import com.ht.scada.oildata.calc.GTReturnKeyEnum;
import com.ht.scada.oildata.model.GTSC;
import com.ht.scada.oildata.util.String2FloatArrayUtil;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import javax.inject.Inject;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

/**
 *
 * @author 赵磊 2014-12-14 23:49:22
 */
public class SgtCalcService {

    private static final Logger log = LoggerFactory.getLogger(SgtCalcService.class);
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    @Inject
    protected Sql2o sql2o;
    @Autowired
    private RealtimeDataService realtimeDataService;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static boolean isListen = Config.INSTANCE.getConfig().getBoolean("is.listen", false);

    public Sql2o getSql2o() {
        return sql2o;
    }

    public void setSql2o(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    public RealtimeDataService getRealtimeDataService() {
        return realtimeDataService;
    }

    public void setRealtimeDataService(RealtimeDataService realtimeDataService) {
        this.realtimeDataService = realtimeDataService;
    }

    /**
     * 接收推送的数据
     *
     * @param message
     */
    public void sgtCalc(final String message) {
        executorService.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    log.info("收到功图信息：" + message + "  " + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
                    if (!isListen) {
                        log.info("忽略！");
                        return;
                    }
                    String mes[] = message.split(",");
                    String code = mes[0];
                    String time = mes[1];
                    Date date = sdf.parse(time);
                    String sgtTime = realtimeDataService.getEndTagVarYcArray(code, RedisKeysEnum.GT_DATETIME.toString());
                    //判断功图时间是否一致
                    if (!sgtTime.equals(time)) {
                        log.info("时间不一致：" + sgtTime);
                        log.info("时间不一致：" + time);
                        log.info("时间不一致：" + code);
                        return;
                    }
                    if (!isSgtOk(code)) {
                        log.info("错误功图：" + code);
                        realtimeDataService.putValue(code, RedisKeysEnum.BENG_XIAO.toString(), "0");
                        realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_CYL.toString(), "0");
                        realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_YL.toString(), "0");
                        realtimeDataService.putValue(code, RedisKeysEnum.GTMJ.toString(), "0");
                        realtimeDataService.putValue(code, RedisKeysEnum.GLGL.toString(), "0");
                        return;
                    }

                    float[] weiyi = String2FloatArrayUtil.string2FloatArrayUtil(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.WEI_YI_ARRAY.toString().toLowerCase()), ",");
                    float[] zaihe = String2FloatArrayUtil.string2FloatArrayUtil(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.ZAI_HE_ARRAY.toString().toLowerCase()), ",");
                    float chongCi = Float.valueOf(realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase()));
                    log.info("开始计算：" + code + " " + time + "  " + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
                    handleData(code, date, weiyi, zaihe, chongCi);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 20, TimeUnit.SECONDS);
    }

    public void handleData(String code, Date date, float[] weiyi, float[] zaihe, float chongCi) {
        //判断功图是否正常
        Float CYL = null, YL = null, HS = null, PHL = null, DYM = null, SXGL = null, XXGL = null, SRGL = null, GGGL = null, SLGL = null, XTXL = null,
                GTMJ = null, BX = null, SXDL = null, XXDL = null, PJSZ = null, PJXZ = null, ZDCD = null;
        String ZDXX = null, ZDYJ = null;


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

        Float hs = null, bj = null, bs = 0f;
        String strHs = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.HAN_SHUI_LV.toString());
        String strBj = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BENG_JING.toString());
        String strBs = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BENG_SHEN.toString());
        String strDym = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DONG_YE_MIAIN.toString());
        if (strHs != null) {
            hs = Float.valueOf(strHs);
        }
        if (strBj != null) {
            bj = Float.valueOf(strBj);
        }
        if (strBs != null) {
            bs = Float.valueOf(strBs);
        }
        if (strDym != null && !"测不出".equals(strDym)) {
            DYM = Float.valueOf(strDym);
        }

        HS = hs == null ? 0 : hs;
        float realBj = bj == null ? 56f : bj;


        GTDataComputerProcess gtData = new GTDataComputerProcess();
        Map<GTReturnKeyEnum, Object> resultMap = null;
        try {
            resultMap = gtData.calcSGTData(weiyi, zaihe, power, dl, chongCi, realBj, 1, HS);

            CYL = (Float) resultMap.get(GTReturnKeyEnum.LIQUID_PRODUCT) * 24;
            YL = CYL * (1 - HS);
            SLGL = CYL * bs / 8640;

            String shCYL = String.valueOf(CYL);
            String ljCYL = String.valueOf(CYL * 24 * CommonUtils.timeProportion(date));
            String ygCYL = String.valueOf(CYL);

            String shYL = String.valueOf(YL);
            String ljYL = String.valueOf(YL * CommonUtils.timeProportion(date));
            String ygYL = String.valueOf(YL);

            SXGL = (Float) resultMap.get(GTReturnKeyEnum.GL_SHANG);
            XXGL = (Float) resultMap.get(GTReturnKeyEnum.GL_XIA);
            SRGL = (Float) resultMap.get(GTReturnKeyEnum.GL_AVG);
            PHL = (Float) resultMap.get(GTReturnKeyEnum.PING_HENG_DU);

            if (SRGL != 0) {
                XTXL = SLGL / SRGL / 2;
                realtimeDataService.putValue(code, RedisKeysEnum.XTXL.toString(), String.valueOf(XTXL));
            }

            String glShang = String.valueOf(SXGL);
            String glXia = String.valueOf(XXGL);
            String phlGl = String.valueOf(PHL);

            SXDL = (Float) resultMap.get(GTReturnKeyEnum.DL_SHANG);
            XXDL = (Float) resultMap.get(GTReturnKeyEnum.DL_XIA);

            String sxdl = String.valueOf(SXDL);
            String xxdl = String.valueOf(XXDL);
            String phdDl = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.PING_HENG_DU_DL));

            realtimeDataService.putValue(code, RedisKeysEnum.RI_SS_CYL.toString(), shCYL);
            realtimeDataService.putValue(code, RedisKeysEnum.RI_LEIJI_CYL.toString(), ljCYL);
            realtimeDataService.putValue(code, RedisKeysEnum.RI_YUGU_CYL.toString(), ygCYL);
            realtimeDataService.putValue(code, RedisKeysEnum.ZR_CYL.toString(), shCYL); //昨日产液量

            realtimeDataService.putValue(code, RedisKeysEnum.RI_SS_YL.toString(), shYL);
            realtimeDataService.putValue(code, RedisKeysEnum.RI_LEIJI_YL.toString(), ljYL);
            realtimeDataService.putValue(code, RedisKeysEnum.RI_YUGU_YL.toString(), ygYL);
            realtimeDataService.putValue(code, RedisKeysEnum.ZR_YL.toString(), shYL); //昨日产油量

            realtimeDataService.putValue(code, RedisKeysEnum.GL_SHANG.toString(), glShang);
            realtimeDataService.putValue(code, RedisKeysEnum.GL_XIA.toString(), glXia);
            realtimeDataService.putValue(code, RedisKeysEnum.PING_HENG_LV.toString(), phlGl);

            realtimeDataService.putValue(code, RedisKeysEnum.DL_SHANG.toString(), sxdl);
            realtimeDataService.putValue(code, RedisKeysEnum.DL_XIA.toString(), xxdl);
            realtimeDataService.putValue(code, RedisKeysEnum.PING_HENG_LV_DL.toString(), phdDl);

            ZDXX = (String) resultMap.get(GTReturnKeyEnum.FAULT_DIAGNOSE_INFO);

            realtimeDataService.putValue(code, RedisKeysEnum.FALUT_DIAGNOSE_INFO.toString(), ZDXX);

            //泵效
            Object bx = resultMap.get(GTReturnKeyEnum.BENG_XIAO);
            if (bx != null) {
                BX = (Float) bx;
                realtimeDataService.putValue(code, RedisKeysEnum.BENG_XIAO.toString(), String.valueOf(BX));
            }

            PJSZ = (Float) resultMap.get(GTReturnKeyEnum.AVG_ZAIHE_SHANG);
            PJXZ = (Float) resultMap.get(GTReturnKeyEnum.AVG_ZAIHE_XIA);

        } catch (Exception e) {
            log.info(code + "功图分析出现异常：" + e.toString());
        }
        
        //功图面积、光杆功率
        float calc[] = new GTCalc().getGTCalcResult(weiyi, zaihe, chongCi);
        GTMJ = calc[0];
        GGGL = calc[1];
        String gtmj = String.valueOf(GTMJ);
        String glgl = String.valueOf(GGGL);
        realtimeDataService.putValue(code, RedisKeysEnum.GTMJ.toString(), gtmj);
        realtimeDataService.putValue(code, RedisKeysEnum.GLGL.toString(), glgl);

        //***************START 威尔泰克功图 ******************
        Calendar startTime = Calendar.getInstance();
        startTime.set(Calendar.HOUR_OF_DAY, startTime.get(Calendar.HOUR_OF_DAY) - 2);
        //取两个小时内最新的功图
        GTSC gtsc = findOneGTFXRecordByCode(code, startTime.getTime());
        float wetkCyl = 0;
        float wetkYl = 0;
        if (gtsc != null) {
            wetkCyl = gtsc.getRCYL1();
//            wetkYl = gtsc.getRCYL();
            wetkYl = wetkCyl * (1-HS);
            //更新威尔泰克功图算产
            Date cjsj = null;
            try {
                cjsj = sdf.parse(gtsc.getCJSJ());
            } catch (ParseException ex) {
                java.util.logging.Logger.getLogger(SgtCalcService.class.getName()).log(Level.SEVERE, null, ex);
            }
            updateSgtHistoryByCode(code, cjsj, wetkCyl, wetkYl);
            realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_CYL.toString(), String.valueOf(wetkCyl));
            realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_YL.toString(), String.valueOf(wetkYl));
        } else {//两个小时内无数据
            realtimeDataService.putValue(code, RedisKeysEnum.BENG_XIAO.toString(), "0");
            realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_CYL.toString(), "0");
            realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_YL.toString(), "0");
        }
        //***************END 威尔泰克功图 ******************

        //写入到历史数据表
        updateHisSgtData(wetkCyl, wetkYl, HS, PHL, DYM, SXGL, XXGL, SRGL, GGGL, SLGL, XTXL, GTMJ, BX, SXDL, XXDL, PJSZ, PJXZ, ZDXX, ZDCD, ZDYJ, code, date);
    }

    private boolean isSgtOk(String code) {
        if (getRealtimeData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) > 0
                && getRealtimeData(code, VarSubTypeEnum.CHONG_CHENG.toString().toLowerCase()) > 0
                && getRealtimeData(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase()) > 0) {
            return true;
        } else {
            return false;
        }
    }

    private float getRealtimeData(String code, String varName) {
        String value = realtimeDataService.getEndTagVarInfo(code, varName);
        if (value != null && !value.isEmpty()) {
            double f = Double.parseDouble(value);
            BigDecimal bg = new BigDecimal(f);
            float f1 = bg.setScale(3, BigDecimal.ROUND_HALF_UP).floatValue();
            return f1;
        } else {
            return 0;
        }
    }

    private void updateHisSgtData(Float CYL, Float YL, Float HS, Float PHL, Float DYM, Float SXGL, Float XXGL, Float SRGL, Float GGGL, Float SLGL, Float XTXL,
            Float GTMJ, Float BX, Float SXDL, Float XXDL, Float PJSZ, Float PJXZ, String ZDXX, Float ZDCD, String ZDYJ, String code, Date date) {
        String sql = "update T_SGT_HISTORY set CYL=:CYL,YL=:YL,HS=:HS,PHL=:PHL,DYM=:DYM,SXGL=:SXGL,XXGL=:XXGL,SRGL=:SRGL, "
                + " GGGL=:GGGL,SLGL=:SLGL,XTXL=:XTXL,GTMJ=:GTMJ,BX=:BX,SXDL=:SXDL,XXDL=:XXDL,PJSZ=:PJSZ,PJXZ=:PJXZ,ZDXX=:ZDXX,ZDCD=:ZDCD,ZDYJ=:ZDYJ,JSSJ=:JSSJ "
                + " where code=:CODE and DATETIME=:DATE";

        try (Connection con = sql2o.open()) {
            con.createQuery(sql)
                    .addParameter("CYL", CYL)
                    .addParameter("YL", YL)
                    .addParameter("HS", HS)
                    .addParameter("PHL", PHL)
                    .addParameter("DYM", DYM)
                    .addParameter("SXGL", SXGL)
                    .addParameter("XXGL", XXGL)
                    .addParameter("SRGL", SRGL)
                    .addParameter("GGGL", GGGL)
                    .addParameter("SLGL", SLGL)
                    .addParameter("XTXL", XTXL)
                    .addParameter("GTMJ", GTMJ)
                    .addParameter("BX", BX)
                    .addParameter("SXDL", SXDL)
                    .addParameter("XXDL", XXDL)
                    .addParameter("PJSZ", PJSZ)
                    .addParameter("PJXZ", PJXZ)
                    .addParameter("ZDXX", ZDXX)
                    .addParameter("ZDCD", ZDCD)
                    .addParameter("ZDYJ", ZDYJ)
                    .addParameter("JSSJ", new Date())
                    .addParameter("CODE", code)
                    .addParameter("DATE", date)
                    .executeUpdate();
            log.info("更新完计算信息：" + code + " " + sdf.format(date));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private GTSC findOneGTFXRecordByCode(String code, Date date) {
        String sql = "SELECT"
                + "  q.RCYL1 rcyl1, "
                + "  q.RCYL  rcyl, "
                + "  q.JH    jh, "
                + "  q.CJSJ  cjsj "
                + " FROM QYSCZH.SCY_SGT_GTFX q  "
                + " WHERE q.SCJSBZ=1 and q.JH = :CODE and q.cjsj is not null and q.CJSJ>=:DATE ORDER BY q.CJSJ DESC ";
        try (Connection con = sql2o.open()) {
            org.sql2o.Query query = con.createQuery(sql).addParameter("CODE", code).addParameter("DATE", date);
            return query.executeAndFetchFirst(GTSC.class);
        }
    }
    
    /**
     * 根据威尔泰克产液量值更新
     * @param code
     * @param date
     * @param CYL
     * @param YL 
     */
    private void updateSgtHistoryByCode(String code, Date date, Float CYL, Float YL) {
        if(date == null) {
            return;
        }
        String sql = "update T_SGT_HISTORY set CYL = :CYL ,YL = :YL "
                    + "where CODE=:CODE and DATETIME=:DATETIME ";
        try (Connection con = sql2o.open()) {
            con.createQuery(sql)
                    .addParameter("CODE", code)
                    .addParameter("DATETIME", date)
                    .addParameter("CYL", CYL)
                    .addParameter("YL", YL)
                    .executeUpdate();
        }
    }
}