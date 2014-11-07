package com.ht.scada.oildata.service;

import java.util.Calendar;


public interface WaterWellDataCalcService {
    /**
     * 班报任务
     */
    void runBanBaoTask();
    /**
     * 日报任务
     */
    void runRiBaoTask();
    
    void runPsfzTask(Calendar c);
    
    void testMathod();
}
