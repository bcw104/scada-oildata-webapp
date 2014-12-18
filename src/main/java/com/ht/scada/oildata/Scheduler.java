package com.ht.scada.oildata;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.oildata.service.CommonScdtService;
import com.ht.scada.oildata.service.OilProductCalcService;
import com.ht.scada.oildata.service.OilWellDataCalcService;
import com.ht.scada.oildata.service.QkOilWellRecordService;
import com.ht.scada.oildata.service.ScslService;
import com.ht.scada.oildata.service.SgtAnalyzeService;
import com.ht.scada.oildata.service.SlytGljService;
import com.ht.scada.oildata.service.WaterWellDataCalcService;
import com.ht.scada.oildata.service.WellInfoInsertService;
import com.ht.scada.oildata.service.WetkSgtInsertService;
import com.ht.scada.oildata.service.impl.SgtCalcService;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * 定时任务
 *
 * @author 赵磊
 */
@Component
public class Scheduler {

    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);
//    @Autowired
//    private OilProductCalcService oilProductCalcService;    //产量计算
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
    @Autowired
    private QkOilWellRecordService qkOilWellRecordService;
    @Autowired
    private SlytGljService slytGljService;
    @Autowired
    private SgtAnalyzeService sgtAnalyzeService;
    
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
//        qkOilWellRecordService.runRiBaoTask();
//        qkOilWellRecordService.runQjRiBaoTask();
//        qkOilWellRecordService.runSjRiBaoTask();
//        commonScdtService.test();
//          slytGljService.runSckhzbTask();
//        sgtAnalyzeService.sgtAnalyze();
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
//        oilProductCalcService.oilProductCalcTask();   //功图分析计算
    }
    /**
     * 计算未监听到的示功图，后期程序稳定后删除该任务
     */
    
    @Scheduled(cron = "0 25 0/1 * * ? ")
    private void sgtCalc() {
        sgtAnalyzeService.sgtAnalyze(); 
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

//    @Scheduled(cron = "0 5 8 * * ? ")
    private void waterRiBaoTask() {
        waterWellDataCalcService.runRiBaoTask();
    }

    /**
     * 计算生产时间
     */
//    @Scheduled(cron = "0 0/1 * * * ? ")
    private void minite1Task() {
        Calendar c = Calendar.getInstance();
        scslService.calcOilWellScsj(c);
        scslService.calcWaterWellScsj(c);

    }

    /**
     * 写入配水阀组数据
     */
//    @Scheduled(cron = "0 0/5 * * * ? ")
    private void minite5Task() {
        waterWellDataCalcService.runPsfzTask(Calendar.getInstance());
    }

    /**
     * 威尔泰克功图报警
     */
//    @Scheduled(cron = "0 0/5 * * * ? ")
    private void reportGtAlarmTask() {
        Calendar calendar = Calendar.getInstance();
        try {
            commonScdtService.reportGtAlarm(calendar);
        } catch (Exception e) {
        }
    }

    /**
     * 获取标准功图
     */
//    @Scheduled(cron = "0 0/10 * * * ? ")
    private void getBzgtDataFromWetk() {
        try {
            commonScdtService.getBzgtDataFromWetk();
        } catch (Exception e) {
        }
    }
    
    /**
     * 清理实时功图数据
     */
//    @Scheduled(cron = "0 10 0 * * ? ")
    private void deleteRtdbGTByNum() {
        try {
            commonScdtService.deleteRtdbGTByNum();
        } catch (Exception e) {
        }
    }
    
    /**
     * 网络诊断
     */
//    @Scheduled(cron = "0 0/10 * * * ? ")
    private void netChecking() {
        try {
            commonScdtService.netChecking();
        } catch (Exception e) {
        }
    }
    
    
    /*******************START 桥口定时任务****************************************/
     /**
     * 每天八点将报表数据写入数据库
     */
//    @Scheduled(cron = "0 12 8 * * ? ")
    private void qkOilRiBaoTask() {
        qkOilWellRecordService.runRiBaoTask();
    }
         /**
     * 每天八点将报表数据写入数据库
     */
//    @Scheduled(cron = "0 15 8 * * ? ")
    private void qkQiRiBaoTask() {
        qkOilWellRecordService.runQjRiBaoTask();
    }
    
//    @Scheduled(cron = "0 0 0/1 * * ? ")
    private void qkSjTask() {
        qkOilWellRecordService.runSjRiBaoTask();
    }
    /*******************END 桥口定时任务****************************************/
    
    /*******************START 胜利油田局生产指标考核****************************************/
//    @Scheduled(cron = "0 0 7 * * ? ")
    private void sczbkhTask() {
        slytGljService.runSckhzbTask();
    }
    /*******************START 胜利油田局生产指标考核****************************************/
}
