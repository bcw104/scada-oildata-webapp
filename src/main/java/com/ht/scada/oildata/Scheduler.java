package com.ht.scada.oildata;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.oildata.service.CommonScdtService;
import com.ht.scada.oildata.service.OilProductCalcService;
import com.ht.scada.oildata.service.OilWellDataCalcService;
import com.ht.scada.oildata.service.ScslService;
import com.ht.scada.oildata.service.WaterWellDataCalcService;
import com.ht.scada.oildata.service.WellInfoInsertService;
import com.ht.scada.oildata.service.WetkSgtInsertService;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
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
    @Autowired
    private ScslService scslService;
    @Autowired
    private EndTagService endTagService;
    public static List<EndTag> youJingList;
    public static List<EndTag> shuiJingList;
    public static List<String> youCodeList = new ArrayList<>();

    /**
     * 测试你的方法，启动时运行
     */
    private void testYourMathod() {
        init();
//        oilWellDataCalcService.runRiBaoTask();
//        oilWellDataCalcService.runBanBaoTask();
//        wetkSgtInsertService.wetkTask();     //威尔泰克功图
//        oilProductCalcService.oilProductCalcTask();   //功图分析
//        wellInfoInsertService.wellInfoSaveTask(); //井基本数据录入任务
//        waterWellDataCalcService.runBanBaoTask();
//        waterWellDataCalcService.runRiBaoTask();
//        waterWellDataCalcService.runPsfzTask(Calendar.getInstance());
//        commonScdtService.wellClosedInfo();
//        scslService.calcOilWellScsj();
//        scslService.calcWaterWellScsj(Calendar.getInstance());
//        commonScdtService.test();

    }

    private void init() {
        youJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        shuiJingList = endTagService.getByType(EndTagTypeEnum.ZHU_SHUI_JING.toString());
        for (EndTag endTag : youJingList) {
            youCodeList.add(endTag.getCode());
        }
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
    @Scheduled(cron = "0 0/10 * * * ? ")
    private void hourly10Task() {
        wetkSgtInsertService.wetkTask();     //威尔泰克功图数据写入
        oilProductCalcService.oilProductCalcTask();   //功图分析计算
    }

    /**
     * 9、11、13、15、17、19、21、23、1、3、5、7
     */
//    @Scheduled(cron = "0 45 1/2 * * ? ")
    private void oilBanBaoTask() {
        oilWellDataCalcService.runBanBaoTask();
    }

    /**
     * 每天7点半将报表数据写入数据库
     */
//    @Scheduled(cron = "0 0 8 * * ? ")
    private void oilRiBaoTask() {
        oilWellDataCalcService.runRiBaoTask();
    }

//    @Scheduled(cron = "0 0 12 * * ? ")
    private void wellClosedInfo() {
        commonScdtService.wellClosedInfo();
    }

//    @Scheduled(cron = "0 50 1/2 * * ? ")
    private void waterBanBaoTask() {
        waterWellDataCalcService.runBanBaoTask();
    }

//    @Scheduled(cron = "0 2 8 * * ? ")
    private void waterRiBaoTask() {
        waterWellDataCalcService.runRiBaoTask();
    }

    /**
     * 每隔一分钟任务
     */
//    @Scheduled(cron = "0 0/1 * * * ? ")
    private void minite1Task() {
        Calendar c = Calendar.getInstance();
        scslService.calcOilWellScsj(c);
        scslService.calcWaterWellScsj(c);

    }
    
     /**
     * 每隔五分钟任务
     */
//    @Scheduled(cron = "0 0/5 * * * ? ")
    private void minite5Task() {
        waterWellDataCalcService.runPsfzTask(Calendar.getInstance());
    }
}
