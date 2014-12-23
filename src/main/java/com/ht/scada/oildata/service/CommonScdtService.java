package com.ht.scada.oildata.service;

import java.util.Calendar;


public interface CommonScdtService {
    /**
     * 关井信息
     */
    void wellClosedInfo();
    void test();
    void getBzgtDataFromWetk();
    void reportGtAlarm(Calendar cal);
//    void netChecking();
    void deleteRtdbGTByNum();
}
