package com.ht.scada.oildata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.service.TagService;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.oildata.entity.FaultDiagnoseRecord;
import com.ht.scada.oildata.entity.OilWellDailyDataRecord;
import com.ht.scada.oildata.entity.OilWellHourlyDataRecord;
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
    @Scheduled(cron = "30 9/10 * * * ? ")
    //@Scheduled(cron = "30 0/1 * * * ? ")
    public void hourlyTask() {
        List<EndTag> endTagList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if(endTagList != null && !endTagList.isEmpty()) {
            for(EndTag endTag : endTagList) {
                OilWellHourlyDataRecord oilWellHourlyDataRecord = scheduledService.getOilWellHourlyDataRecordByCode(endTag.getCode(), 10, new Date());
                reportService.insertOilWellHourlyDataRecord(oilWellHourlyDataRecord);
                System.out.println( new Date().toString() + "写入时记录"+endTag.getCode()+"成功！");
            }
        }
        System.out.println("现在时刻：" + new Date().toString());
    }
    
    @Scheduled(cron = "5 0 0 * * ? ")
    public void dailyTask() {
        List<EndTag> endTagList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if(endTagList != null && !endTagList.isEmpty()) {
            for(EndTag endTag : endTagList) {
                OilWellDailyDataRecord oilWellDailyDataRecord = scheduledService.getYesterdayOilWellDailyDataRecordByCode(endTag.getCode());
                reportService.insertOilWellDailyDataRecord(oilWellDailyDataRecord);
                System.out.println( new Date().toString() + "写入日记录"+endTag.getCode()+"成功！");
            }
        }
    }

    private void sendFaultData(FaultDiagnoseRecord record) throws JsonProcessingException {
        String message = objectMapper.writeValueAsString(record);
        redisTemplate.convertAndSend("FaultDiagnoseChannel", message);
    }
}
