package com.ht.scada.oildata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ht.scada.oildata.entity.FaultDiagnoseRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.Calendar;
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

    @PostConstruct
    private void init() {
        executorService = Executors.newSingleThreadScheduledExecutor();

        Calendar now = Calendar.getInstance();
        long delay = 3600 - (now.getTimeInMillis() % 3600);
        // 从下个整点开始每隔1小时计算一次功图数据
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // todo 计算功图数据并写入数据库、生成功图图片、故障诊断
            }
        }, delay, 3600, TimeUnit.SECONDS);
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
