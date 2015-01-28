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
     * 生产考核指标，更新函数 - 经营管理指标、系统运行指标
     */
    void runSckhzbUpdateTask();
    
    /**
     * 四化运维考核
     */
    void shywkh() ;
}
