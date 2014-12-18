/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.webapp.entity;

import java.util.Date;

/**
 * 油井信息
 *
 * @author 赵磊 2014-8-3 15:20:52
 */
public class WellInfoWrapper {
    private String code;
    private Date datetime;
    private Float chong_ci;
    private String wei_yi_array;
    private String zai_he_array;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Date getDatetime() {
        return datetime;
    }

    public void setDatetime(Date datetime) {
        this.datetime = datetime;
    }

    public Float getChong_ci() {
        return chong_ci;
    }

    public void setChong_ci(Float chong_ci) {
        this.chong_ci = chong_ci;
    }

    public String getWei_yi_array() {
        return wei_yi_array;
    }

    public void setWei_yi_array(String wei_yi_array) {
        this.wei_yi_array = wei_yi_array;
    }

    public String getZai_he_array() {
        return zai_he_array;
    }

    public void setZai_he_array(String zai_he_array) {
        this.zai_he_array = zai_he_array;
    }
    
    
}
