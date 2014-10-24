/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.EndTagSubTypeEnum;
import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.Scheduler;
import com.ht.scada.oildata.entity.WetkSGT;
import com.ht.scada.oildata.service.WellInfoService;
import com.ht.scada.oildata.service.WetkSGTService;
import com.ht.scada.oildata.service.WetkSgtInsertService;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.sql2o.Sql2o;

/**
 *
 * @author 赵磊 2014-8-14 23:49:22
 */
@Transactional
@Service("wetkSgtInsertService")
public class WetkSgtInsertServiceImpl implements WetkSgtInsertService {

    private static final Logger log = LoggerFactory.getLogger(WetkSgtInsertServiceImpl.class);
    @Autowired
    private RealtimeDataService realtimeDataService;
    @Autowired
    private WetkSGTService wetkSGTService;
    @Autowired
    private WellInfoService wellInfoService;
    private Map<String, String> dateMap = new HashMap<>();
    @Inject
    protected Sql2o sql2o;

    public Sql2o getSql2o() {
        return sql2o;
    }

    public void setSql2o(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    /**
     * 威尔泰克功图数据
     *
     * @author 陈志强
     */
    @Override
    public void wetkTask() {
        log.info("开启功图计产任务——现在时刻：" + CommonUtils.date2String(new Date()));
        // 功图id(32位随机数)
        String gtId;
        float CC, CC1, ZDZH, ZXZH;

        if (Scheduler.youJingList != null && Scheduler.youJingList.size() > 0) {
            for (EndTag youJing : Scheduler.youJingList) {
                try {
                    if (!youJing.getSubType().equals(EndTagSubTypeEnum.YOU_LIANG_SHI.toString()) && !youJing.getSubType().equals(EndTagSubTypeEnum.GAO_YUAN_JI.toString())) {
                        continue;
                    }
                    String code = youJing.getCode();
                    // 1.判断功图时间是否更新
                    String newDateTime = realtimeDataService.getEndTagVarYcArray(code, RedisKeysEnum.GT_DATETIME.toString());
                    if (newDateTime == null || "".equals(newDateTime)) {
                        continue;
                    }
                    if (dateMap.get(code) != null && dateMap.get(code).equals(newDateTime)) {
                        continue;
                    }
                    dateMap.put(code, newDateTime);

                    // 2.判断是否有功图
                    if (getRealtimeData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) > 0) { // 有功图才写进行持久化
                        WetkSGT wetkSGT = new WetkSGT();
                        gtId = CommonUtils.getCode();
                        CC = getRealtimeData(code, VarSubTypeEnum.CHONG_CHENG.toString().toLowerCase());
                        CC1 = getRealtimeData(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase());
                        ZDZH = getRealtimeData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase());
                        ZXZH = getRealtimeData(code, VarSubTypeEnum.ZUI_XIAO_ZAI_HE.toString().toLowerCase());
                        wetkSGT.setID(gtId);
                        wetkSGT.setJH(code);
                        wetkSGT.setCJSJ(CommonUtils.string2Date(newDateTime));
                        wetkSGT.setCC(CC); // 冲程
                        wetkSGT.setCC1(CC1); // 冲次
                        wetkSGT.setSXCC1(getRealtimeData(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase())); //todo 上行冲次，暂时与冲次值相同
                        wetkSGT.setXXCC1(getRealtimeData(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase())); //todo 下行冲次，暂时与冲次值相同
                        wetkSGT.setWY(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.WEI_YI_ARRAY.toString().toLowerCase()), 3));
                        wetkSGT.setZH(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.ZAI_HE_ARRAY.toString().toLowerCase()), 3));
                        wetkSGT.setGL(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.GONG_LV_ARRAY.toString().toLowerCase()), 3));
                        wetkSGT.setDL(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.DIAN_LIU_ARRAY.toString().toLowerCase()), 3));
                        wetkSGT.setBPQSCGL(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.GONG_LV_YIN_SHU_ARRAY.toString().toLowerCase()), 3));
                        wetkSGT.setZJ(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.DIAN_GONG_TU_ARRAY.toString().toLowerCase()), 3));
                        wetkSGT.setZDZH(ZDZH);// 最大载荷
                        wetkSGT.setZXZH(ZXZH); // 最小载荷
                        wetkSGT.setBZGT(null); // 暂时为空
                        wetkSGT.setGLYS(getRealtimeData(code, VarSubTypeEnum.GV_GLYS.toString().toLowerCase()));
                        wetkSGT.setYGGL(getRealtimeData(code, VarSubTypeEnum.GV_YG.toString().toLowerCase()));
                        wetkSGT.setWGGL(getRealtimeData(code, VarSubTypeEnum.GV_WG.toString().toLowerCase()));

                        wetkSGTService.addOneRecord(wetkSGT); // 持久化

                        Map<String, Object> basicInforOfthisEndtag = wellInfoService.findBasicCalculateInforsByCode(code);
                        Float bengJing = null, hanShui = null, yymd = null;
                        if (basicInforOfthisEndtag != null) {		// 不为空
                            bengJing = Float.parseFloat(((BigDecimal) basicInforOfthisEndtag.get("bj")).toString());
                            hanShui = Float.parseFloat(((BigDecimal) basicInforOfthisEndtag.get("hs")).toString());
                            yymd = Float.parseFloat(((BigDecimal) basicInforOfthisEndtag.get("dmyymd")).toString());
                        }
                        wetkSGTService.addOneGTFXRecord(gtId, code, CommonUtils.string2Date(newDateTime), CC, CC1, ZDZH, ZXZH, bengJing, hanShui, yymd, 1F); // 功图分析表持久化数据
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        log.info("完成功图计产任务——现在时刻：" + CommonUtils.date2String(new Date()));
    }

    /**
     * 根据井号和变量名获取实时数据
     *
     * @param code
     * @param varName
     * @return
     */
    private float getRealtimeData(String code, String varName) {
        String value = realtimeDataService.getEndTagVarInfo(code, varName);
        if (value != null && !value.isEmpty()) {
            return CommonUtils.string2Float(value, 4); // 保留四位小数
        } else {
            return 0;
        }
    }
}