/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.webapp.entity;

import java.util.Date;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

/**
 *
 * @author 赵磊 2013-12-6 13:43:20
 */
//@Entity
//@Table(name = "T_SOE_Record")
public class SoeRecord{

    @Column(name = "soe_value")
    private String soeValue;    //varName1:value1;varName2:value2
    @Column(name = "shreshold_value")
    private String shresholdValue;  //门限值，格式：varName1:value1;varName2:value2
    @Temporal(TemporalType.TIMESTAMP)
    private Date deviceTime;
    @Transient
    private SoeStorage soeStorage;
    @Id
    private String id;	// 唯一主键
    @Column(name = "end_id")
    private int endId;
    @Column(name = "end_name")
    private String endName;
    @Column(name = "tag_name")
    private String tagName;// 中文名称
    private String code;// 计量点编号(回路号、井号等)
    private String name;// 变量名称
    private String info;// 故障信息
    private Boolean value;
    @Column(name = "action_time")
    @Temporal(TemporalType.TIMESTAMP)
    private Date actionTime;
    @Column(name = "resume_time")
    @Temporal(TemporalType.TIMESTAMP)
    private Date resumeTime;
    @Column(name = "ensure_time")
    @Temporal(TemporalType.TIMESTAMP)
    private Date ensureTime;    //确认时间
    @Column(name = "user_name")
    private String username;    //确认者
    @Transient
    private boolean persisted;// 是否已经写入数据库
    @Column(name = "alarm_level")
    private Integer alarmLevel;//报警级别
    @Column(name = "alarm_type")
    private String alarmType;	//报警类型

    public SoeRecord() {
        this.id = UUID.randomUUID().toString().replace("-", "");
    }

    public SoeRecord(int endId, String endName, String code, String name, String tagName, String info, boolean value, Date actionTime, String soeValue, String shresholdValue, Date deviceTime, Integer alarmLevel, String alarmType) {
        this.id = UUID.randomUUID().toString().replace("-", "");
        this.endId = endId;
        this.endName = endName;
        this.code = code;
        this.name = name;
        this.tagName = tagName;
        this.info = info;
        this.value = value;
        this.actionTime = actionTime;
        this.soeValue = soeValue;
        this.shresholdValue = shresholdValue;
        this.deviceTime = deviceTime;
        this.alarmLevel = alarmLevel;
        this.alarmType = alarmType;
    }

    public String getSoeValue() {
        return soeValue;
    }

    public void setSoeValue(String soeValue) {
        this.soeValue = soeValue;
    }

    public Date getDeviceTime() {
        return deviceTime;
    }

    public void setDeviceTime(Date deviceTime) {
        this.deviceTime = deviceTime;
    }

    public String getShresholdValue() {
        return shresholdValue;
    }

    public void setShresholdValue(String shresholdValue) {
        this.shresholdValue = shresholdValue;
    }

    public SoeStorage getSoeStorage() {
        return soeStorage;
    }

    public void setSoeStorage(SoeStorage soeStorage) {
        this.soeStorage = soeStorage;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getEndId() {
        return endId;
    }

    public void setEndId(int endId) {
        this.endId = endId;
    }

    public String getEndName() {
        return endName;
    }

    public void setEndName(String endName) {
        this.endName = endName;
    }

    public String getTagName() {
        return tagName;
    }

    public void setTagName(String tagName) {
        this.tagName = tagName;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public Boolean getValue() {
        return value;
    }

    public void setValue(Boolean value) {
        this.value = value;
    }

    public Date getActionTime() {
        return actionTime;
    }

    public void setActionTime(Date actionTime) {
        this.actionTime = actionTime;
    }

    public Date getResumeTime() {
        return resumeTime;
    }

    public void setResumeTime(Date resumeTime) {
        this.resumeTime = resumeTime;
    }

    public boolean isPersisted() {
        return persisted;
    }

    public void setPersisted(boolean persisted) {
        this.persisted = persisted;
    }
    
    public boolean isNew() {

        return null == getId();
    }

    public Date getEnsureTime() {
        return ensureTime;
    }

    public void setEnsureTime(Date ensureTime) {
        this.ensureTime = ensureTime;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Integer getAlarmLevel() {
        return alarmLevel;
    }

    public void setAlarmLevel(Integer alarmLevel) {
        this.alarmLevel = alarmLevel;
    }

    public String getAlarmType() {
        return alarmType;
    }

    public void setAlarmType(String alarmType) {
        this.alarmType = alarmType;
    }
    
    
}
