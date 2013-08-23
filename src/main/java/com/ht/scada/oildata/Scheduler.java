package com.ht.scada.oildata;

import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.common.tag.util.VarGroupEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.dao.TestSGTDao;
import com.ht.scada.oildata.entity.FaultDiagnoseRecord;
import com.ht.scada.oildata.entity.GasWellDailyDataRecord;
import com.ht.scada.oildata.entity.GasWellHourlyDataRecord;
import com.ht.scada.oildata.entity.OilWellDailyDataRecord;
import com.ht.scada.oildata.entity.OilWellHourlyDataRecord;
import com.ht.scada.oildata.entity.TestSGT;
import com.ht.scada.oildata.entity.WaterWellDailyDataRecord;
import com.ht.scada.oildata.entity.WaterWellHourlyDataRecord;
import com.ht.scada.oildata.entity.WellData;
import com.ht.scada.oildata.entity.ZengYaZhanDailyDataRecord;
import com.ht.scada.oildata.entity.ZhuQiDailyDataRecord;
import com.ht.scada.oildata.entity.ZhuQiHourlyDataRecord;
import com.ht.scada.oildata.entity.ZhuShuiDailyDataRecord;
import com.ht.scada.oildata.entity.ZhuShuiHourlyDataRecord;
import com.ht.scada.oildata.service.ReportService;
import com.ht.scada.oildata.service.ScheduledService;
import com.ht.scada.oildata.util.String2FloatArrayUtil;

/**
 * 定时任务
 *
 * @author 赵磊
 */
@Component
public class Scheduler {

    @Autowired
    @Qualifier("scheduledService1")
    private ScheduledService scheduledService;
    @Autowired
    private EndTagService endTagService;
    @Autowired
    private ReportService reportService;
    @Autowired
    private TestSGTDao testSGTDao;
    @Autowired
	private RealtimeDataService realtimeDataService;

    /**
     *
     */
    //@Scheduled(cron = "30 0/2 * * * ? ")
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
        String trqType = EndTagTypeEnum.TIAN_RAN_QI_JING.toString();
        List<EndTag> gasList = endTagService.getByType(trqType);
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

    //@Scheduled(cron = "5 0 0 * * ? ")
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
   
    @Scheduled(cron = "0 0/1 * * * ? ")
    public void testSGT() {
    	
    	Map<String, String> map = realtimeDataService.getEndTagVarGroupInfo("test_001", VarGroupEnum.YOU_JING_SGT.toString());
		if(map != null) {
			TestSGT testSGT = new TestSGT();
			testSGT.setChongCheng(Float.valueOf(map.get(VarSubTypeEnum.CHONG_CHENG.toString().toLowerCase())));
			testSGT.setChongCi(Float.valueOf(map.get(VarSubTypeEnum.CHONG_CI.toString().toLowerCase())));
			testSGT.setChongCiShang(Float.valueOf(map.get(VarSubTypeEnum.SHANG_XING_CHONG_CI.toString().toLowerCase())));
			testSGT.setChongCiXia(Float.valueOf(map.get(VarSubTypeEnum.XIA_XING_CHONG_CI.toString().toLowerCase())));
			testSGT.setMinZaihe(Float.valueOf(map.get(VarSubTypeEnum.ZUI_XIAO_ZAI_HE.toString().toLowerCase())));
			testSGT.setMaxZaihe(Float.valueOf(map.get(VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase())));
			
			String weiyi = realtimeDataService.getEndTagVarYcArray("test_001" ,VarSubTypeEnum.WEI_YI_ARRAY.toString().toLowerCase());
			String zaihe = realtimeDataService.getEndTagVarYcArray("test_001" ,VarSubTypeEnum.ZAI_HE_ARRAY.toString().toLowerCase());
			
			testSGT.setWeiyi(weiyi);
			testSGT.setZaihe(zaihe);
			testSGT.setDate(new Date());
			float[] weiyiArray = String2FloatArrayUtil.string2FloatArrayUtil(weiyi, ",");
			float[] zaiheArray = String2FloatArrayUtil.string2FloatArrayUtil(zaihe, ",");
			float weiyiSum = 0, zaiheSum = 0;
			for(float f : weiyiArray) {
				weiyiSum += f;
			}
			for(float f : zaiheArray) {
				zaiheSum += f;
			}
			testSGT.setWeiyiSum(weiyiSum);
			testSGT.setZaiheSum(zaiheSum);
			
			
			
			testSGTDao.save(testSGT);
			System.out.println("写入功图测试数据成功！");
		}
    	
    	
    	
    }
    
    private void sendFaultData(FaultDiagnoseRecord record) {
        String message = JSON.toJSONString(record);
        //redisTemplate.convertAndSend("FaultDiagnoseChannel", message);
    }
}
