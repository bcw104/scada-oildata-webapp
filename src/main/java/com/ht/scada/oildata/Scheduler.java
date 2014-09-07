package com.ht.scada.oildata;

import com.ht.scada.oildata.service.CommonScdtService;
import com.ht.scada.oildata.service.OilProductCalcService;
import com.ht.scada.oildata.service.OilWellDataCalcService;
import com.ht.scada.oildata.service.WaterWellDataCalcService;
import com.ht.scada.oildata.service.WellInfoInsertService;
import com.ht.scada.oildata.service.WetkSgtInsertService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 定时任务
 *
 * @author 赵磊
 */
@Component
public class Scheduler {

    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);
    @Autowired
    private OilProductCalcService oilProductCalcService;    //产量计算
    @Autowired
    private OilWellDataCalcService oilWellDataCalcService;  //油井日报班报
    @Autowired
    private WaterWellDataCalcService waterWellDataCalcService;  //水井日报班报
    @Autowired
    private WetkSgtInsertService wetkSgtInsertService;      //威尔泰克功图数据写入
    @Autowired
    private WellInfoInsertService wellInfoInsertService;    //油井信息录入
    @Autowired
    private CommonScdtService commonScdtService;    

    /**
     * 测试你的方法，启动时运行
     */
    private void testYourMathod() {
//        oilWellDataCalcService.runRiBaoTask();
//        oilWellDataCalcService.runBanBaoTask();
//        wetkSgtInsertService.wetkTask();     //威尔泰克功图
//        oilProductCalcService.oilProductCalcTask();   //功图分析
//        wellInfoInsertService.wellInfoSaveTask(); //井基本数据录入任务
//        waterWellDataCalcService.runBanBaoTask();
//        waterWellDataCalcService.runRiBaoTask();
//        commonScdtService.wellClosedInfo();
    }

    /**
     * 凌晨1秒任务
     */
//    @Scheduled(cron = "1 0 0 * * ? ")
    private void dailyTask() {
        wellInfoInsertService.wellInfoSaveTask(); //井基本数据录入任务
    }
    
    /**
     * 每隔10分钟定时任务
     */
//    @Scheduled(cron = "0 0/10 * * * ? ")
    private void hourly10Task() {
        wetkSgtInsertService.wetkTask();     //威尔泰克功图数据写入
    }

    /**
     * 每隔30分钟定时任务
     */
//    @Scheduled(cron = "0 0/30 * * * ? ")
    private void hourly30Task() {
        oilProductCalcService.oilProductCalcTask();   //功图分析
    }

    /**
     * 每天7点半将报表数据写入数据库
     */
    @Scheduled(cron = "0 55 7 * * ? ")
    private void reportTask() {
        oilWellDataCalcService.runRiBaoTask();
    }

    /**
     * 9、11、13、15、17、19、21、23、1、3、5、7
     */
    @Scheduled(cron = "0 45 1/2 * * ? ")
    private void banbaoTask() {
        oilWellDataCalcService.runBanBaoTask();
        commonScdtService.wellClosedInfo();
    }

}
