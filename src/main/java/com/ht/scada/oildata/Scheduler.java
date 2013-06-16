package com.ht.scada.oildata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.oildata.entity.FaultDiagnoseRecord;
import com.ht.scada.oildata.entity.GasWellDailyDataRecord;
import com.ht.scada.oildata.entity.GasWellHourlyDataRecord;
import com.ht.scada.oildata.entity.OilWellDailyDataRecord;
import com.ht.scada.oildata.entity.OilWellHourlyDataRecord;
import com.ht.scada.oildata.entity.WaterWellDailyDataRecord;
import com.ht.scada.oildata.entity.WaterWellHourlyDataRecord;
import com.ht.scada.oildata.entity.ZengYaZhanDailyDataRecord;
import com.ht.scada.oildata.entity.ZhuQiDailyDataRecord;
import com.ht.scada.oildata.entity.ZhuQiHourlyDataRecord;
import com.ht.scada.oildata.entity.ZhuShuiDailyDataRecord;
import com.ht.scada.oildata.entity.ZhuShuiHourlyDataRecord;
import com.ht.scada.oildata.service.ReportService;
import com.ht.scada.oildata.service.ScheduledService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Date;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * 定时任务
 *
 * @author 赵磊
 */
@Component
public class Scheduler {

    private ObjectMapper objectMapper = new ObjectMapper();
    @Inject
    private StringRedisTemplate redisTemplate;
    @Autowired
    @Qualifier("scheduledService1")
    private ScheduledService scheduledService;
    @Autowired
    private EndTagService endTagService;
    @Autowired
    private ReportService reportService;

    /**
     *
     */
    //@Scheduled(cron = "30 9/10 * * * ? ")
    @Scheduled(cron = "30 0/1 * * * ? ")
    public void hourlyTask() {
        //油井
        List<EndTag> oilWellList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if (oilWellList != null && !oilWellList.isEmpty()) {
            for (EndTag endTag : oilWellList) {
                OilWellHourlyDataRecord oilWellHourlyDataRecord = scheduledService.getOilWellHourlyDataRecordByCode(endTag.getCode(), 10, new Date());
                reportService.insertOilWellHourlyDataRecord(oilWellHourlyDataRecord);
                System.out.println(new Date().toString() + "写入时记录" + endTag.getCode() + "成功！");
            }
        }
        //水源井
        List<EndTag> waterList = endTagService.getByType(EndTagTypeEnum.SHUI_YUAN_JING.toString());
        if (waterList != null && !waterList.isEmpty()) {
            for (EndTag endTag : waterList) {
                WaterWellHourlyDataRecord record = scheduledService.getWaterWellHourlyDataRecordByCode(endTag.getCode(), 10, new Date());
                reportService.insertWaterWellHourlyDataRecord(record);
                System.out.println(new Date().toString() + "写入时记录" + endTag.getCode() + "成功！");
            }
        }
        //天然气井
//        System.out.println(EndTagTypeEnum.TIAN_RAN_QI_JING.toString());
        List<EndTag> gasList = endTagService.getByType("TIAN_RAN_QI_JING");       
        if (gasList != null && !gasList.isEmpty()) {
            for (EndTag endTag : gasList) {
                GasWellHourlyDataRecord record = scheduledService.getGasWellHourlyDataRecordByCode(endTag.getCode(), 10, new Date());
                reportService.insertGasWellHourlyDataRecord(record);
                System.out.println(new Date().toString() + "写入时记录" + endTag.getCode() + "成功！");
            }
        }
        //注水站
        List<EndTag> zhuShuiList = endTagService.getByType(EndTagTypeEnum.ZHU_SHUI_ZHAN.toString());
        if (zhuShuiList != null && !zhuShuiList.isEmpty()) {
            for (EndTag endTag : zhuShuiList) {
                ZhuShuiHourlyDataRecord record = scheduledService.getZhuShuiHourlyDataRecordByCode(endTag.getCode(), 10, new Date());
                reportService.insertZhuShuiHourlyDataRecord(record);
                System.out.println(new Date().toString() + "写入时记录" + endTag.getCode() + "成功！");
            }
        }
        //注汽站
        List<EndTag> zhuQiList = endTagService.getByType(EndTagTypeEnum.ZHU_QI_ZHAN.toString());
        if (zhuQiList != null && !zhuQiList.isEmpty()) {
            for (EndTag endTag : zhuQiList) {
                ZhuQiHourlyDataRecord record = scheduledService.getZhuQiHourlyDataRecordByCode(endTag.getCode(), 10, new Date());
                reportService.insertZhuQiHourlyDataRecord(record);
                System.out.println(new Date().toString() + "写入时记录" + endTag.getCode() + "成功！");
            }
        }

        System.out.println("现在时刻：" + new Date().toString());
    }

    @Scheduled(cron = "5 0 0 * * ? ")
    public void dailyTask() {
        //油井
        List<EndTag> oilWellList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if (oilWellList != null && !oilWellList.isEmpty()) {
            for (EndTag endTag : oilWellList) {
                OilWellDailyDataRecord oilWellDailyDataRecord = scheduledService.getYesterdayOilWellDailyDataRecordByCode(endTag.getCode());
                reportService.insertOilWellDailyDataRecord(oilWellDailyDataRecord);
                System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
            }
        }
        //水源井
        List<EndTag> waterWellList = endTagService.getByType(EndTagTypeEnum.SHUI_YUAN_JING.toString());
        if (waterWellList != null && !waterWellList.isEmpty()) {
            for (EndTag endTag : waterWellList) {
                WaterWellDailyDataRecord record = scheduledService.getYesterdayWaterWellDailyDataRecordByCode(endTag.getCode());
                reportService.insertWaterWellDailyDataRecord(record);
                System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
            }
        }
        //天然气井
         List<EndTag> gasWellList = endTagService.getByType(EndTagTypeEnum.TIAN_RAN_QI_JING.toString());
        if (gasWellList != null && !gasWellList.isEmpty()) {
            for (EndTag endTag : gasWellList) {
                GasWellDailyDataRecord record = scheduledService.getYesterdayGasWellDailyDataRecordByCode(endTag.getCode());
                reportService.insertGasWellDailyDataRecord(record);
                System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
            }
        }
        //增压站
         List<EndTag> zyzList = endTagService.getByType(EndTagTypeEnum.ZENG_YA_ZHAN.toString());
        if (zyzList != null && !zyzList.isEmpty()) {
            for (EndTag endTag : zyzList) {
                ZengYaZhanDailyDataRecord record = scheduledService.getYesterdayZengYaZhanDailyDataRecordByCode(endTag.getCode());
                reportService.insertZengYaZhanDailyDataRecord(record);
                System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
            }
        }
        //注水站
        List<EndTag> zszList = endTagService.getByType(EndTagTypeEnum.ZHU_SHUI_ZHAN.toString());
        if (zszList != null && !zszList.isEmpty()) {
            for (EndTag endTag : zszList) {
                ZhuShuiDailyDataRecord record = scheduledService.getYesterdayZhuShuiDailyDataRecordByCode(endTag.getCode());
                reportService.insertZhuShuiDailyDataRecord(record);
                System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
            }
        }
        //注汽站
        List<EndTag> zqzList = endTagService.getByType(EndTagTypeEnum.ZHU_QI_ZHAN.toString());
        if (zqzList != null && !zqzList.isEmpty()) {
            for (EndTag endTag : zqzList) {
                ZhuQiDailyDataRecord record = scheduledService.getYesterdayZhuQiDailyDataRecordByCode(endTag.getCode());
                reportService.insertZhuQiDailyDataRecord(record);
                System.out.println(new Date().toString() + "写入日记录" + endTag.getCode() + "成功！");
            }
        }
    }

    private void sendFaultData(FaultDiagnoseRecord record) throws JsonProcessingException {
        String message = objectMapper.writeValueAsString(record);
        redisTemplate.convertAndSend("FaultDiagnoseChannel", message);
    }
}
