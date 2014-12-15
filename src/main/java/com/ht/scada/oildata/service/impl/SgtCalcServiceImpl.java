/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.CommonUtils;
import com.ht.scada.oildata.calc.GTCalc;
import com.ht.scada.oildata.calc.GTDataComputerProcess;
import com.ht.scada.oildata.calc.GTReturnKeyEnum;
import com.ht.scada.oildata.model.GTSC;
import com.ht.scada.oildata.model.WellInfoWrapper;
import com.ht.scada.oildata.service.WellInfoService;
import com.ht.scada.oildata.util.String2FloatArrayUtil;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
public class SgtCalcServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(SgtCalcServiceImpl.class);
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    @Inject
    protected Sql2o sql2o;
    @Autowired
    private RealtimeDataService realtimeDataService;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Sql2o getSql2o() {
        return sql2o;
    }

    public void setSql2o(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    public void sgtCalc(final String message) {
        System.out.println(message);
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    log.info("收到功图信息：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
                    String mes[] = message.split(",");
                    String code = mes[0];
                    long time = Long.parseLong(mes[1]);
                    Date date = new Date(time);

                    String sgtTime = realtimeDataService.getEndTagVarYcArray(code, RedisKeysEnum.GT_DATETIME.toString());
                    //判断功图时间是否一致
                    if (!sgtTime.equals(sdf.format(date))) {
                        return;
                    }
                    //判断功图是否正常
                    if (isSgtOk(code)) {
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

                        Float hs = null, bj = null, bs = null, dym = null;
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
                        if (strDym != null) {
                            dym = Float.valueOf(strDym);
                        }

                        float realHs = hs == null ? 0 : hs;
                        float realBj = bj == null ? 56f : bj;


                        GTDataComputerProcess gtData = new GTDataComputerProcess();
                        Map<GTReturnKeyEnum, Object> resultMap = null;
                        try {
                            resultMap = gtData.calcSGTData(weiyi, zaihe, power, dl, chongCi, realBj, 1, realHs);

                            String shCYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.LIQUID_PRODUCT) * 24);
                            String ljCYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.LIQUID_PRODUCT) * 24 * CommonUtils.timeProportion(date));
                            String ygCYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.LIQUID_PRODUCT) * 24);

//                            String shYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.OIL_PRODUCT) * 24);
//                            String ljYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.OIL_PRODUCT) * 24 * CommonUtils.timeProportion(date));
//                            String ygYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.OIL_PRODUCT) * 24);

                            String shYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.LIQUID_PRODUCT) * 24 * (1 - realHs));
                            String ljYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.LIQUID_PRODUCT) * 24 * (1 - realHs) * CommonUtils.timeProportion(date));
                            String ygYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.LIQUID_PRODUCT) * 24 * (1 - realHs));

//                            String nhShang = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.NENG_HAO_SHANG));
//                            String nhXia = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.NENG_HAO_XIA));

                            String glShang = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.GL_SHANG));
                            String glXia = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.GL_XIA));
                            String glAvg = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.GL_AVG));
                            String phlGl = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.PING_HENG_DU));

                            String sxdl = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.DL_SHANG));
                            String xxdl = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.DL_XIA));
                            String phdDl = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.PING_HENG_DU_DL));

                            realtimeDataService.putValue(code, RedisKeysEnum.RI_SS_CYL.toString(), shCYL);
                            realtimeDataService.putValue(code, RedisKeysEnum.RI_LEIJI_CYL.toString(), ljCYL);
                            realtimeDataService.putValue(code, RedisKeysEnum.RI_YUGU_CYL.toString(), ygCYL);
                            realtimeDataService.putValue(code, RedisKeysEnum.ZR_CYL.toString(), shCYL); //昨日产液量

                            realtimeDataService.putValue(code, RedisKeysEnum.RI_SS_YL.toString(), shYL);
                            realtimeDataService.putValue(code, RedisKeysEnum.RI_LEIJI_YL.toString(), ljYL);
                            realtimeDataService.putValue(code, RedisKeysEnum.RI_YUGU_YL.toString(), ygYL);
                            realtimeDataService.putValue(code, RedisKeysEnum.ZR_YL.toString(), shYL); //昨日产油量

//                            realtimeDataService.putValue(code, RedisKeysEnum.SHANG_NH.toString(), nhShang);
//                            realtimeDataService.putValue(code, RedisKeysEnum.XIA_NH.toString(), nhXia);

                            realtimeDataService.putValue(code, RedisKeysEnum.GL_SHANG.toString(), glShang);
                            realtimeDataService.putValue(code, RedisKeysEnum.GL_XIA.toString(), glXia);
                            realtimeDataService.putValue(code, RedisKeysEnum.PING_HENG_LV.toString(), phlGl);

                            realtimeDataService.putValue(code, RedisKeysEnum.DL_SHANG.toString(), sxdl);
                            realtimeDataService.putValue(code, RedisKeysEnum.DL_XIA.toString(), xxdl);
                            realtimeDataService.putValue(code, RedisKeysEnum.PING_HENG_LV_DL.toString(), phdDl);

                            //泵效
                            Object bx = resultMap.get(GTReturnKeyEnum.BENG_XIAO);
                            if (bx != null) {
                                realtimeDataService.putValue(code, RedisKeysEnum.BENG_XIAO.toString(), String.valueOf((Float) bx));
                            }

                        } catch (Exception e) {
                            log.info(code + "功图分析出现异常：" + e.toString());
                        }

                        //***************START 威尔泰克功图 ******************
                        Calendar startTime = Calendar.getInstance();
                        startTime.set(Calendar.HOUR_OF_DAY, startTime.get(Calendar.HOUR_OF_DAY) - 2);
                        //取两个小时内最新的功图
                        GTSC gtsc = findOneGTFXRecordByCode(code, startTime.getTime());
                        if (gtsc != null) {
                            float wetkCyl = gtsc.getRCYL1();
                            float wetkYl = gtsc.getRCYL();
                            realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_CYL.toString(), String.valueOf(wetkCyl));
                            realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_YL.toString(), String.valueOf(wetkYl));
                        } else {//两个小时内无数据
                            realtimeDataService.putValue(code, RedisKeysEnum.BENG_XIAO.toString(), "0");
                            realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_CYL.toString(), "0");
                            realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_YL.toString(), "0");
                        }
                        //***************END 威尔泰克功图 ******************

                        //功图面积、光杆功率
                        float calc[] =new GTCalc().getGTCalcResult(weiyi, zaihe, chongCi);
                        String gtmj = String.valueOf(calc[0]);
                        String glgl = String.valueOf(calc[1]);
                        realtimeDataService.putValue(code, RedisKeysEnum.GTMJ.toString(), gtmj);
                        realtimeDataService.putValue(code, RedisKeysEnum.GLGL.toString(), glgl);
                    } else {//无功图
                        realtimeDataService.putValue(code, RedisKeysEnum.BENG_XIAO.toString(), "0");
                        realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_CYL.toString(), "0");
                        realtimeDataService.putValue(code, RedisKeysEnum.WETK_RI_SS_YL.toString(), "0");
                        realtimeDataService.putValue(code, RedisKeysEnum.GTMJ.toString(), "0");
                        realtimeDataService.putValue(code, RedisKeysEnum.GLGL.toString(), "0");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
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
}