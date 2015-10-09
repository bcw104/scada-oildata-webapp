/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service;

/**
 *
 * @author zhao
 */
public interface SlytGljService {
    /**
     * 生产考核指标 - 系统运行指标
     */
    void runSckhzbTask();
    
    /**
     * 系统运行指标更新函数
     */
    void runSckhzbTask_SystemRunUpdate();
    
    /**
     * 生产考核指标，更新函数 - 经营管理指标、系统运行指标
     * 说明：多数参数来源于 源点库, 更新时间放到10:00以后
     */
    void runSckhzbUpdateTask();
    
    /**
     * 生产考核指标，更新函数 - 经营管理指标、系统运行指标
     * 说明：多数参数来源于 本地库, 更新时间放到 8:15以后
     */
    void runSckhzbUpdateTaskFromRealDate();
    
    /**
     * 四化运维考核
     */
    void shywkh() ;
}
