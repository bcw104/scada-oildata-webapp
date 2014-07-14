package com.ht.scada.oildata;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.entity.WetkSGT;
import com.ht.scada.oildata.service.ReportService;
import com.ht.scada.oildata.service.WellService;
import com.ht.scada.oildata.service.WetkSGTService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 定时任务
 *
 * @author 赵磊
 */
@Component
public class Scheduler {

    //@Autowired
    //@Qualifier("scheduledService1")
    //private ScheduledService scheduledService;
    @Autowired
    private EndTagService endTagService;
    @Autowired
    private ReportService reportService;
    @Autowired
    private RealtimeDataService realtimeDataService;
    @Autowired
    private WellService wellService;
    @Autowired
    private WetkSGTService wetkSGTService;

    private int interval = 20;  //存储间隔

    /**
     *
     */
    @Scheduled(cron = "30 0 0 * * ? ")
    public void hourlyTask() {
        //油井
        //List<EndTag> oilWellList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        //if (oilWellList != null && !oilWellList.isEmpty()) {
        //    for (EndTag endTag : oilWellList) {
        //
        //        OilWellHourlyDataRecord oilWellHourlyDataRecord = scheduledService.getOilWellHourlyDataRecordByCode(endTag.getCode(), interval, new Date());
        //        reportService.insertOilWellHourlyDataRecord(oilWellHourlyDataRecord);
        //        System.out.println(new Date().toString() + "写入时记录" + endTag.getCode() + "成功！");
        //    }
        //}
        //水源井
        //List<EndTag> waterList = endTagService.getByType(EndTagTypeEnum.SHUI_YUAN_JING.toString());
        //if (waterList != null && !waterList.isEmpty()) {
        //    for (EndTag endTag : waterList) {
        //
        //        WaterWellHourlyDataRecord record = scheduledService.getWaterWellHourlyDataRecordByCode(endTag.getCode(), interval, new Date());
        //        reportService.insertWaterWellHourlyDataRecord(record);
        //        System.out.println(new Date().toString() + "写入时记录" + endTag.getCode() + "成功！");
        //    }
        //}
        //天然气井
        //String trqType = EndTagTypeEnum.TIAN_RAN_QI_JING.toString();
        //List<EndTag> gasList = endTagService.getByType(trqType);
        //if (gasList != null && !gasList.isEmpty()) {
        //    for (EndTag endTag : gasList) {
        //
        //        GasWellHourlyDataRecord record = scheduledService.getGasWellHourlyDataRecordByCode(endTag.getCode(), interval, new Date());
        //        reportService.insertGasWellHourlyDataRecord(record);
        //        System.out.println(new Date().toString() + "写入时记录" + endTag.getCode() + "成功！");
        //    }
        //}
        //注水站
        //List<EndTag> zhuShuiList = endTagService.getByType(EndTagTypeEnum.ZHU_SHUI_ZHAN.toString());
        //if (zhuShuiList != null && !zhuShuiList.isEmpty()) {
        //    for (EndTag endTag : zhuShuiList) {
        //
        //        ZhuShuiHourlyDataRecord record = scheduledService.getZhuShuiHourlyDataRecordByCode(endTag.getCode(), interval, new Date());
        //        reportService.insertZhuShuiHourlyDataRecord(record);
        //        System.out.println(new Date().toString() + "写入时记录" + endTag.getCode() + "成功！");
        //    }
        //}
        //注汽站
        //List<EndTag> zhuQiList = endTagService.getByType(EndTagTypeEnum.ZHU_QI_ZHAN.toString());
        //if (zhuQiList != null && !zhuQiList.isEmpty()) {
        //    for (EndTag endTag : zhuQiList) {
        //
        //        ZhuQiHourlyDataRecord record = scheduledService.getZhuQiHourlyDataRecordByCode(endTag.getCode(), interval, new Date());
        //        reportService.insertZhuQiHourlyDataRecord(record);
        //        System.out.println(new Date().toString() + "写入时记录" + endTag.getCode() + "成功！");
        //    }
        //}
        //威尔泰克功图
        Map<String, String> dateMap = new HashMap<String, String>();
        List<EndTag> youJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if (youJingList != null && youJingList.size() > 0) {
            //for (EndTag youJing : youJingList) {
            //    String code = youJing.getCode();
                String code = "GD1-15X819";
                String newDateTime = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GT_DATETIME.toString());
                if(dateMap.get(code) != null && dateMap.get(code).equals(newDateTime)) {
                     return;
                }
                dateMap.put(code, newDateTime);
                WetkSGT wetkSGT = new WetkSGT();
                wetkSGT.setJH(code);
                String dateTime = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GT_DATETIME.toString());
                if (dateTime!=null){
                    wetkSGT.setCJSJ(CommonUtils.string2Date(dateTime));
                }else {
                    wetkSGT.setCJSJ(new Date());
                }
                wetkSGT.setCC(getDianYCData(code,VarSubTypeEnum.CHONG_CHENG.toString().toLowerCase()));
                wetkSGT.setCC1(getDianYCData(code,VarSubTypeEnum.CHONG_CI.toString().toLowerCase()));
                wetkSGT.setSXCC1(0); //todo 上行冲次
                wetkSGT.setXXCC1(0); //todo 下行冲次
                wetkSGT.setWY(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.WEI_YI_ARRAY.toString().toLowerCase()));
                wetkSGT.setZH(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.ZAI_HE_ARRAY.toString().toLowerCase()));
                wetkSGT.setGL(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.GONG_LV_ARRAY.toString().toLowerCase()));
                wetkSGT.setDL(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.DIAN_LIU_ARRAY.toString().toLowerCase()));
                wetkSGT.setBPQSCGL(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.BIAN_PIN_QI_SHU_CHU_PIN_LV.toString().toLowerCase()));
                wetkSGT.setZJ(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.DIAN_GONG_TU_ARRAY.toString().toLowerCase()));
                wetkSGT.setZDZH(getDianYCData(code,VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()));
                wetkSGT.setZXZH(getDianYCData(code,VarSubTypeEnum.ZUI_XIAO_ZAI_HE.toString().toLowerCase()));
                wetkSGT.setBZGT(null); // 暂时为空
                wetkSGT.setGLYS(getDianYCData(code, VarSubTypeEnum.GV_GLYS.toString().toLowerCase()));
                wetkSGT.setYGGL(getDianYCData(code, VarSubTypeEnum.GV_YG.toString().toLowerCase()));
                wetkSGT.setWGGL(getDianYCData(code, VarSubTypeEnum.GV_WG.toString().toLowerCase()));

                wetkSGTService.addOneRecord(wetkSGT); // 持久化
            //}
        }

        System.out.println("现在时刻：" + new Date().toString());
    }


    /**
    * 根据井号和变量名获取实时数据
    * @param code
    * @param varName
    * @return
    */
    private float getDianYCData(String code, String varName) {
        String value = realtimeDataService.getEndTagVarInfo(code, varName);
        if (value != null && !value.isEmpty()) {
            return  CommonUtils.string2Float(value, 4); // 保留四位小数
        } else {
            return 0;
        }
    }
    //
    ////@Scheduled(cron = "5 0 0 * * ? ")
    //public void dailyTask() {
    //    //油井
    //    List<EndTag> oilWellList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
    //    if (oilWellList != null && !oilWellList.isEmpty()) {
    //        for (EndTag endTag : oilWellList) {
    //            OilWellDailyDataRecord oilWellDailyDataRecord = scheduledService.getYesterdayOilWellDailyDataRecordByCode(endTag.getCode());
    //            reportService.insertOilWellDailyDataRecord(oilWellDailyDataRecord);
    //            System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
    //        }
    //    }
    //    //水源井
    //    List<EndTag> waterWellList = endTagService.getByType(EndTagTypeEnum.SHUI_YUAN_JING.toString());
    //    if (waterWellList != null && !waterWellList.isEmpty()) {
    //        for (EndTag endTag : waterWellList) {
    //            WaterWellDailyDataRecord record = scheduledService.getYesterdayWaterWellDailyDataRecordByCode(endTag.getCode());
    //            reportService.insertWaterWellDailyDataRecord(record);
    //            System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
    //        }
    //    }
    //    //天然气井
    //    List<EndTag> gasWellList = endTagService.getByType(EndTagTypeEnum.TIAN_RAN_QI_JING.toString());
    //    if (gasWellList != null && !gasWellList.isEmpty()) {
    //        for (EndTag endTag : gasWellList) {
    //            GasWellDailyDataRecord record = scheduledService.getYesterdayGasWellDailyDataRecordByCode(endTag.getCode());
    //            reportService.insertGasWellDailyDataRecord(record);
    //            System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
    //        }
    //    }
    //    //增压站
    //    List<EndTag> zyzList = endTagService.getByType(EndTagTypeEnum.ZENG_YA_ZHAN.toString());
    //    if (zyzList != null && !zyzList.isEmpty()) {
    //        for (EndTag endTag : zyzList) {
    //            ZengYaZhanDailyDataRecord record = scheduledService.getYesterdayZengYaZhanDailyDataRecordByCode(endTag.getCode());
    //            reportService.insertZengYaZhanDailyDataRecord(record);
    //            System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
    //        }
    //    }
    //    //注水站
    //    List<EndTag> zszList = endTagService.getByType(EndTagTypeEnum.ZHU_SHUI_ZHAN.toString());
    //    if (zszList != null && !zszList.isEmpty()) {
    //        for (EndTag endTag : zszList) {
    //            ZhuShuiDailyDataRecord record = scheduledService.getYesterdayZhuShuiDailyDataRecordByCode(endTag.getCode());
    //            reportService.insertZhuShuiDailyDataRecord(record);
    //            System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
    //        }
    //    }
    //    //注汽站
    //    List<EndTag> zqzList = endTagService.getByType(EndTagTypeEnum.ZHU_QI_ZHAN.toString());
    //    if (zqzList != null && !zqzList.isEmpty()) {
    //        for (EndTag endTag : zqzList) {
    //            ZhuQiDailyDataRecord record = scheduledService.getYesterdayZhuQiDailyDataRecordByCode(endTag.getCode());
    //            reportService.insertZhuQiDailyDataRecord(record);
    //            System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
    //        }
    //    }
    //}
    //
    ///**
    // * 定时给 SCY_SGT_GTCJ 写功图数据
    // */
    //public void dailyTask2() {
    //    //油井
    //    List<EndTag> oilWellList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
    //    if (oilWellList != null && !oilWellList.isEmpty()) {
    //        for (EndTag endTag : oilWellList) {
    //            OilWellDailyDataRecord oilWellDailyDataRecord = scheduledService.getYesterdayOilWellDailyDataRecordByCode(endTag.getCode());
    //            reportService.insertOilWellDailyDataRecord(oilWellDailyDataRecord);
    //            System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
    //        }
    //    }
    //    //水源井
    //    List<EndTag> waterWellList = endTagService.getByType(EndTagTypeEnum.SHUI_YUAN_JING.toString());
    //    if (waterWellList != null && !waterWellList.isEmpty()) {
    //        for (EndTag endTag : waterWellList) {
    //            WaterWellDailyDataRecord record = scheduledService.getYesterdayWaterWellDailyDataRecordByCode(endTag.getCode());
    //            reportService.insertWaterWellDailyDataRecord(record);
    //            System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
    //        }
    //    }
    //    //天然气井
    //    List<EndTag> gasWellList = endTagService.getByType(EndTagTypeEnum.TIAN_RAN_QI_JING.toString());
    //    if (gasWellList != null && !gasWellList.isEmpty()) {
    //        for (EndTag endTag : gasWellList) {
    //            GasWellDailyDataRecord record = scheduledService.getYesterdayGasWellDailyDataRecordByCode(endTag.getCode());
    //            reportService.insertGasWellDailyDataRecord(record);
    //            System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
    //        }
    //    }
    //    //增压站
    //    List<EndTag> zyzList = endTagService.getByType(EndTagTypeEnum.ZENG_YA_ZHAN.toString());
    //    if (zyzList != null && !zyzList.isEmpty()) {
    //        for (EndTag endTag : zyzList) {
    //            ZengYaZhanDailyDataRecord record = scheduledService.getYesterdayZengYaZhanDailyDataRecordByCode(endTag.getCode());
    //            reportService.insertZengYaZhanDailyDataRecord(record);
    //            System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
    //        }
    //    }
    //    //注水站
    //    List<EndTag> zszList = endTagService.getByType(EndTagTypeEnum.ZHU_SHUI_ZHAN.toString());
    //    if (zszList != null && !zszList.isEmpty()) {
    //        for (EndTag endTag : zszList) {
    //            ZhuShuiDailyDataRecord record = scheduledService.getYesterdayZhuShuiDailyDataRecordByCode(endTag.getCode());
    //            reportService.insertZhuShuiDailyDataRecord(record);
    //            System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
    //        }
    //    }
    //    //注汽站
    //    List<EndTag> zqzList = endTagService.getByType(EndTagTypeEnum.ZHU_QI_ZHAN.toString());
    //    if (zqzList != null && !zqzList.isEmpty()) {
    //        for (EndTag endTag : zqzList) {
    //            ZhuQiDailyDataRecord record = scheduledService.getYesterdayZhuQiDailyDataRecordByCode(endTag.getCode());
    //            reportService.insertZhuQiDailyDataRecord(record);
    //            System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
    //        }
    //    }
    //}
    //
    //@Scheduled(cron = "0 0/1 * * * ? ")
    //public void testSGT() {
    //
    //    Map<String, String> map = realtimeDataService.getEndTagVarGroupInfo("test_001", VarGroupEnum.YOU_JING_SGT.toString());
    //    if (map != null) {
    //        TestSGT testSGT = new TestSGT();
    //        testSGT.setChongCheng(Float.valueOf(map.get(VarSubTypeEnum.CHONG_CHENG.toString().toLowerCase())));
    //        testSGT.setChongCi(Float.valueOf(map.get(VarSubTypeEnum.CHONG_CI.toString().toLowerCase())));
    //        testSGT.setChongCiShang(Float.valueOf(map.get(VarSubTypeEnum.SHANG_XING_CHONG_CI.toString().toLowerCase())));
    //        testSGT.setChongCiXia(Float.valueOf(map.get(VarSubTypeEnum.XIA_XING_CHONG_CI.toString().toLowerCase())));
    //        testSGT.setMinZaihe(Float.valueOf(map.get(VarSubTypeEnum.ZUI_XIAO_ZAI_HE.toString().toLowerCase())));
    //        testSGT.setMaxZaihe(Float.valueOf(map.get(VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase())));
    //
    //        String weiyi = realtimeDataService.getEndTagVarYcArray("test_001", VarSubTypeEnum.WEI_YI_ARRAY.toString().toLowerCase());
    //        String zaihe = realtimeDataService.getEndTagVarYcArray("test_001", VarSubTypeEnum.ZAI_HE_ARRAY.toString().toLowerCase());
    //
    //        testSGT.setWeiyi(weiyi);
    //        testSGT.setZaihe(zaihe);
    //        testSGT.setDate(new Date());
    //        float[] weiyiArray = String2FloatArrayUtil.string2FloatArrayUtil(weiyi, ",");
    //        float[] zaiheArray = String2FloatArrayUtil.string2FloatArrayUtil(zaihe, ",");
    //        float weiyiSum = 0, zaiheSum = 0;
    //        for (float f : weiyiArray) {
    //            weiyiSum += f;
    //        }
    //        for (float f : zaiheArray) {
    //            zaiheSum += f;
    //        }
    //        testSGT.setWeiyiSum(weiyiSum);
    //        testSGT.setZaiheSum(zaiheSum);
    //
    //
    //
    //        testSGTDao.save(testSGT);
    //        System.out.println("写入功图测试数据成功！");
    //    }
    //
    //
    //
    //}
    //
    //private void sendFaultData(FaultDiagnoseRecord record) {
    //    String message = JSON.toJSONString(record);
    //   // redisTemplate.convertAndSend("FaultDiagnoseChannel", message);
    //}
}
