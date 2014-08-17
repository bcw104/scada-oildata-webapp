package com.ht.scada.oildata;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.EndTagSubTypeEnum;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.calc.GTDataComputerProcess;
import com.ht.scada.oildata.calc.GTReturnKeyEnum;
import com.ht.scada.oildata.entity.WetkSGT;
import com.ht.scada.oildata.model.WellInfoWrapper;
import com.ht.scada.oildata.service.CommonScdtService;
import com.ht.scada.oildata.service.WellInfoService;
import com.ht.scada.oildata.service.ScdtService;
import com.ht.scada.oildata.service.WetkSGTService;
import com.ht.scada.oildata.util.String2FloatArrayUtil;
import java.math.BigDecimal;
import java.util.Calendar;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.LocalDateTime;

/**
 * 定时任务
 *
 * @author 赵磊
 */
@Component
public class Scheduler {

    @Autowired
    private EndTagService endTagService;
    @Autowired
    private RealtimeDataService realtimeDataService;
    @Autowired
    private WetkSGTService wetkSGTService;
    @Autowired
    private WellInfoService wellInfoService;
    @Autowired
    private ScdtService scdtService;
    @Autowired
    private CommonScdtService commonScdtService;
    private Map<String, String> dateMap = new HashMap<>();
    private Map<String, String> myDateMap = new HashMap<>();

    /**
     * 测试你的方法，启动时运行
     */
    private void testYourMathod() {
        commonScdtService.runRiBaoTask();
//        eightTask();
    }

    /**
     * 凌晨1秒任务
     */
    @Scheduled(cron = "1 0 0 * * ? ")
    private void dailyTask() {
        wellInfoSaveTask(); //井基本数据录入任务
    }

    /**
     * 每隔10分钟定时任务
     */
    @Scheduled(cron = "0 0/10 * * * ? ")
    private void hourlyTask() {
        wetkTask();     //威尔泰克功图
        oilProductCalcTask();   //功图分析
    }

    /**
     * 每天7点半将报表数据写入数据库
     */
    @Scheduled(cron = "0 55 7 * * ? ")
    private void reportTask() {
        commonScdtService.runRiBaoTask();
    }

    /**
     * 9、11、13、15、17、19、21、23、1、3、5、7
     */
    @Scheduled(cron = "0 45 1/2 * * ? ")
    private void banbaoTask() {
        commonScdtService.runBanBaoTask();
    }

    /**
     * 每天8点钟任务
     */
    @Scheduled(cron = "0 50 7 * * ? ")
    private void eightTask() {
        dbdsTask();
    }

    /**
     * 生产动态导入源头库中油井日报数据
     */
    private void scdtYjrbInsert() {
//        scdtService.
    }

    /**
     * 每天8点更新电表读数
     *
     * @author 赵磊
     */
    private void dbdsTask() {
        System.out.println("开始更新电表读数——现在时刻：" + CommonUtils.date2String(new Date()));
        List<EndTag> youJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if (youJingList != null && youJingList.size() > 0) {
            for (EndTag youJing : youJingList) {
                String code = youJing.getCode();
                String num = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.DL_ZX_Z.toString().toLowerCase()) == null ? "0"
                        : realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.DL_ZX_Z.toString().toLowerCase());
                realtimeDataService.putValue(code, RedisKeysEnum.RI_LINGSHI_DBDS.toString(), num);
            }
        }
        System.out.println("结束更新电表读数——现在时刻：" + CommonUtils.date2String(new Date()));
    }

    /**
     * 威尔泰克功图数据
     *
     * @author 陈志强
     */
    private void wetkTask() {
        System.out.println("开启功图计产任务——现在时刻：" + CommonUtils.date2String(new Date()));
        // 功图id(32位随机数)
        String gtId;
        float CC, CC1, ZDZH, ZXZH;

        List<EndTag> youJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if (youJingList != null && youJingList.size() > 0) {
            for (EndTag youJing : youJingList) {
                String code = youJing.getCode();
                // 1.判断功图时间是否更新
                String newDateTime = realtimeDataService.getEndTagVarYcArray(code, RedisKeysEnum.GT_DATETIME.toString());
                if (dateMap.get(code) != null && dateMap.get(code).equals(newDateTime)) {
                    continue;
                }
                dateMap.put(code, newDateTime);
                // 2.判断是否有功图
                if (getDianYCData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) > 0) { // 有功图才写进行持久化
                    WetkSGT wetkSGT = new WetkSGT();
                    gtId = CommonUtils.getCode();
                    CC = getDianYCData(code, VarSubTypeEnum.CHONG_CHENG.toString().toLowerCase());
                    CC1 = getDianYCData(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase());
                    ZDZH = getDianYCData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase());
                    ZXZH = getDianYCData(code, VarSubTypeEnum.ZUI_XIAO_ZAI_HE.toString().toLowerCase());
                    wetkSGT.setID(gtId);
                    wetkSGT.setJH(code);
                    wetkSGT.setCJSJ(CommonUtils.string2Date(newDateTime));
                    wetkSGT.setCC(CC); // 冲程
                    wetkSGT.setCC1(CC1); // 冲次
                    wetkSGT.setSXCC1(getDianYCData(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase())); //todo 上行冲次，暂时与冲次值相同
                    wetkSGT.setXXCC1(getDianYCData(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase())); //todo 下行冲次，暂时与冲次值相同
                    wetkSGT.setWY(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.WEI_YI_ARRAY.toString().toLowerCase()), 3));
                    wetkSGT.setZH(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.ZAI_HE_ARRAY.toString().toLowerCase()), 3));
                    wetkSGT.setGL(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.GONG_LV_ARRAY.toString().toLowerCase()), 3));
                    wetkSGT.setDL(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.DIAN_LIU_ARRAY.toString().toLowerCase()), 3));
                    wetkSGT.setBPQSCGL(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.GONG_LV_YIN_SHU_ARRAY.toString().toLowerCase()), 3));
                    wetkSGT.setZJ(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.DIAN_GONG_TU_ARRAY.toString().toLowerCase()), 3));
                    wetkSGT.setZDZH(ZDZH);// 最大载荷
                    wetkSGT.setZXZH(ZXZH); // 最小载荷
                    wetkSGT.setBZGT(null); // 暂时为空
                    wetkSGT.setGLYS(getDianYCData(code, VarSubTypeEnum.GV_GLYS.toString().toLowerCase()));
                    wetkSGT.setYGGL(getDianYCData(code, VarSubTypeEnum.GV_YG.toString().toLowerCase()));
                    wetkSGT.setWGGL(getDianYCData(code, VarSubTypeEnum.GV_WG.toString().toLowerCase()));

                    wetkSGTService.addOneRecord(wetkSGT); // 持久化
                    wetkSGTService.addOneGTFXRecord(gtId, code, CommonUtils.string2Date(newDateTime), CC, CC1, ZDZH, ZXZH); // 功图分析表持久化数据
                }
            }
        }
        System.out.println("完成功图计产任务——现在时刻：" + CommonUtils.date2String(new Date()));
    }

    /**
     * 功图分析计算
     *
     * @author 赵磊
     */
    private void oilProductCalcTask() {
        System.out.println("开启功图分析任务：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));

        List<EndTag> youJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if (youJingList != null && youJingList.size() > 0) {
            for (EndTag youJing : youJingList) {
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
                if (getDianYCData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) > 0) { // 有功图才写进行持久化
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
//                    System.out.println(""+bj);
//                    System.out.println(""+hs);
//                    System.out.println(""+md);
                    GTDataComputerProcess gtData = new GTDataComputerProcess();
                    Map<GTReturnKeyEnum, Object> resultMap = null;
                    try {
                        resultMap = gtData.calcSGTData(weiyi, zaihe, power, dl, chongCi, bj, md, hs / 100);
                    } catch (Exception e) {
                        System.out.println("功图分析出现异常：" + e.toString());
                        e.printStackTrace();
                        continue;
                    }
                    Date date = CommonUtils.string2Date(newDateTime);

                    String shCYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.LIQUID_PRODUCT) * 24);
                    String ljCYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.LIQUID_PRODUCT) * 24 * timeProportion(date));
                    String ygCYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.LIQUID_PRODUCT) * 24);

                    String shYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.OIL_PRODUCT) * 24);
                    String ljYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.OIL_PRODUCT) * 24 * timeProportion(date));
                    String ygYL = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.OIL_PRODUCT) * 24);

                    String nhShang = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.NENG_HAO_SHANG));
                    String nhXia = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.NENG_HAO_XIA));
                    String phl = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.PING_HENG_DU));

                    String shNH = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.NENG_HAO_RI));
                    String ljNH = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.NENG_HAO_RI) * timeProportion(date));
                    String ygNH = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.NENG_HAO_RI));
                    
                    String phddl = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.PING_HENG_DU_DL));
                    String sxdl = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.DL_SHANG));
                    String xxdl = String.valueOf((Float) resultMap.get(GTReturnKeyEnum.DL_XIA));
                    
                    realtimeDataService.putValue(code, RedisKeysEnum.RI_SS_CYL.toString(), shCYL);
                    realtimeDataService.putValue(code, RedisKeysEnum.RI_LEIJI_CYL.toString(), ljCYL);
                    realtimeDataService.putValue(code, RedisKeysEnum.RI_YUGU_CYL.toString(), ygCYL);

                    realtimeDataService.putValue(code, RedisKeysEnum.RI_SS_YL.toString(), shYL);
                    realtimeDataService.putValue(code, RedisKeysEnum.RI_LEIJI_YL.toString(), ljYL);
                    realtimeDataService.putValue(code, RedisKeysEnum.RI_YUGU_YL.toString(), ygYL);

                    realtimeDataService.putValue(code, RedisKeysEnum.RI_SS_HDL.toString(), shNH);
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
            }
        }
        System.out.println("完成功图分析任务：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
    }

    /**
     * 井基本数据录入任务
     *
     * @author 王蓬
     */
    private void wellInfoSaveTask() {
        System.out.println("开始录入井信息：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));

        List<Map<String, Object>> allEndtagsCode = wellInfoService.findAllEndtagsCode();
        for (int i = 0; i < allEndtagsCode.size(); i++) {
            String endTagCode = (String) allEndtagsCode.get(i).get("code");
            Map<String, Object> basicInforOfthisEndtag = wellInfoService.findBasicCalculateInforsByCode(endTagCode);

            if (basicInforOfthisEndtag != null) {		// 不为空
                float bengJing = Float.parseFloat(((BigDecimal) basicInforOfthisEndtag.get("bj")).toString());
                float hanShui = Float.parseFloat(((BigDecimal) basicInforOfthisEndtag.get("hs")).toString());
                float yymd = Float.parseFloat(((BigDecimal) basicInforOfthisEndtag.get("dmyymd")).toString());
                Date lrqiDate = (Date) basicInforOfthisEndtag.get("rq");
                // 此处调用写入函数
                wellInfoService.addOneTWellInforRecord(
                        CommonUtils.getCode(),
                        endTagCode,
                        bengJing,
                        hanShui,
                        1.0f, // 水密度 默认1.0
                        yymd,
                        lrqiDate);
            }
        }
        System.out.println("完成录入井信息：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
    }

    /*
     * 当前时间较 0点时间占一天比重
     */
    private float timeProportion(Date date) {
        Calendar cal = Calendar.getInstance();		// 当前时间
        cal.setTime(date);
        return (float) (cal.get(Calendar.HOUR_OF_DAY) * 60 + cal.get(Calendar.MINUTE)) / (24 * 60);
    }

    /**
     * 根据井号和变量名获取实时数据
     *
     * @param code
     * @param varName
     * @return
     */
    private float getDianYCData(String code, String varName) {
        String value = realtimeDataService.getEndTagVarInfo(code, varName);
        if (value != null && !value.isEmpty()) {
            return CommonUtils.string2Float(value, 4); // 保留四位小数
        } else {
            return 0;
        }
    }
}
