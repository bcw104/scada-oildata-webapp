package com.ht.scada.oildata.service;


public interface QkOilWellRecordService {

    /**
     * 油井日报任务
     */
    void runRiBaoTask();

    /**
     * 气井日报任务
     */
    void runQjRiBaoTask();
    /**
     * 水井日报任务
     */
    void runSjRiBaoTask();
}
