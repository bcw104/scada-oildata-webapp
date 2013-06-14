package com.ht.scada.oildata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ht.scada.oildata.entity.FaultDiagnoseRecord;
import java.text.SimpleDateFormat;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: 薄成文 13-5-24 下午11:44
 * To change this template use File | Settings | File Templates.
 */
@Component
public class Scheduler {

    private ScheduledExecutorService executorService;
    private ObjectMapper objectMapper = new ObjectMapper();
    @Inject
    private StringRedisTemplate redisTemplate;
    // todo 自动注入各种服务类接口
    
    private int interval = 3;//分钟间隔

    @PostConstruct
    public void init() {
        executorService = Executors.newSingleThreadScheduledExecutor();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar now = Calendar.getInstance();
        long delay = interval*60 - ((now.getTimeInMillis()/1000) % 60);
        System.out.println(sdf.format(now.getTime()));
        System.out.println(delay);
        // 从下个整点开始每隔1小时计算一次功图数据
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // todo 计算功图数据并写入数据库、生成功图图片、故障诊断
            	System.out.println("现在时刻：" + new Date().toString());
            }
        }, delay, interval*60, TimeUnit.SECONDS);
    }

    private void sendFaultData(FaultDiagnoseRecord record) throws JsonProcessingException {
        String message = objectMapper.writeValueAsString(record);
        redisTemplate.convertAndSend("FaultDiagnoseChannel", message);
    }

    @PreDestroy
    private void destroy() {
        executorService.shutdownNow();
    }
}
