/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.data.service.impl.HistoryDataServiceImpl2;
import com.ht.scada.oildata.service.SlytGljService;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

/**
 * @author 赵磊 2014-12-15 19:04:22
 */
@Transactional
@Service("slytGljService")
public class SlytGljServiceImpl implements SlytGljService {

    private static final Logger log = LoggerFactory.getLogger(SlytGljServiceImpl.class);
    @Autowired
    private EndTagService endTagService;
    @Autowired
    private RealtimeDataService realtimeDataService;
    @Autowired
    private HistoryDataServiceImpl2 historyDataServiceImpl2;
    @Inject
    protected Sql2o sql2o;
    @Inject
    @Named("sql2o1")
    protected Sql2o sql2o1;

    @Override
    public void runSckhzbTask() {
        log.info("开始写入生产考核指标数据——现在时刻：" + CommonUtils.date2String(new Date()));

        String GLQDM = "30202009";  //管理区代码 YS_DAB08@YDK
        Float MKSXL = 90f; //生产单元模块上线率
        Float SJBDL = null; //数据标定率
        Float SJQQL = 90f; //采集数据齐全率
        Float BJPC = 2f;  //单元模块报警频次
        Float CZJSL = 90f;  //报警处置及时率
        Float CZFHL = 90f; //报警处置符合率
        Float YJSL = null;  //油井时率
        Float YJTJL = null; //油井躺井率
        Float PHHGL = null; //平衡合格率
        Float ZSJSL = null; //注水井时率
        Float PZWCL = null; //配注完成率
        Float ZSBH = null;  //注水标耗
        Float CYBH = null;  //采油标耗
        Float ZRDJL = null; //自然递减率
        Float GKHGL = null; //工况合格率
        String GLQMC = "孤岛采油管理四区";

        /**
         * *计算生产单元模块上线率********
         */
        float scdyNum = 0; //实际上线的数量
        float allNum = 0;  //应上线的数量

        List<Map<String, Object>> list = null;
        try (Connection con = sql2o.open()) {
            list = con.createQuery("select * from T_END_TAG").executeAndFetchTable().asList();
        } catch (Exception e) {
        }
        if (list != null) {
            scdyNum = 0;
            for (Map<String, Object> map : list) {
                String type = (String) map.get("type");
                String code = (String) map.get("code");
                switch (type) {
                    case "YOU_JING":
                        allNum++;
                        //有通讯井+措施关井+长停井
                        if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status"))) {
                            scdyNum++;
                        } else if (realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.CTJ.toString()) != null
                                && !"".equals(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.CTJ.toString()))) {
                            scdyNum++;
                        } else if (realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GJYY.toString()) != null
                                && !"".equals(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GJYY.toString()))) {
                            scdyNum++;
                        }
                        break;
                    case "JI_LIANG_ZHAN":
                        allNum++;
                        if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status"))) {
                            scdyNum++;
                        }
                        break;
                    case "PEI_SHUI_JIAN":
                        allNum++;
                        if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status"))) {
                            scdyNum++;
                        }
                        break;
                    case "ZHU_SHUI_JING":
                        allNum++;
                        scdyNum++;
                        break;
                    default:
                        break;
                }
            }
        }
        List<Map<String, Object>> spList = null;
        try (Connection con = sql2o.open()) {
            spList = con.createQuery("select RELATEDCODE,VAR_NAME from R_NETCHECKING where DEVICETYPE='摄像头'").executeAndFetchTable().asList();
        } catch (Exception e) {
        }
        if (spList != null) {
            allNum = allNum + spList.size();
            int spOnNum = 0;
            for (Map<String, Object> map : spList) {
                String varName = (String) map.get("var_name");
                String code = (String) map.get("relatedcode");
                String status = realtimeDataService.getEndTagVarInfo(code, varName);
                if (status != null && "true".equals(status)) {
                    spOnNum++;
                }
            }
            scdyNum = scdyNum + spOnNum;
            log.info("视频总数：{}", spList.size());
            log.info("在线视频数：{}", spOnNum);
        }

        if (allNum > 0) {
            MKSXL = scdyNum * 100 / allNum;
            log.info("应上线生产单元模块总数：" + allNum);
            log.info("实际上线的生产单元模块数：" + scdyNum);
            log.info("模块上线率：" + MKSXL);
        }

        /**
         * *计算采集数据齐全率********
         */
        if (list != null) {
            float allQqNum = 0;
            float onNum = 0;
            for (Map<String, Object> map : list) {
                String type = (String) map.get("type");
                String subType = (String) map.get("sub_type");
                String code = (String) map.get("code");
                if ("YOU_JING".equals(type) && "true".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status"))) {
                    allQqNum = allQqNum + 4;
                    if ("YOU_LIANG_SHI".equals(subType)) {
                        allQqNum++;
                        if (getRealtimeData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) > 0) {
                            onNum++;
                        }
                    }
                    if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "ty_zai_xian_cgq6"))) {//套压
                        onNum++;
                    }
                    if (getRealtimeData(code, VarSubTypeEnum.JING_KOU_WEN_DU.toString().toLowerCase()) > 0) {
                        onNum++;
                    }
                    if (getRealtimeData(code, VarSubTypeEnum.HUI_YA.toString().toLowerCase()) > 0) {
                        onNum++;
                    }
                    if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "zndb_zai_xian_cgq15"))) {//智能电表
                        onNum++;
                    }
                }
            }
            if (allQqNum > 0) {
                SJQQL = onNum * 100 / allQqNum;
                log.info("应上线数据项：" + allQqNum);
                log.info("实际上线的数据项：" + onNum);
                log.info("数据齐全率：" + SJQQL);
            }
        }

        /**
         * *计算单元模块报警频次********
         */
        float bjNum = 0;    //报警次数
        //昨天6:20到今天6:20的报警数目
        Calendar startTime = Calendar.getInstance();
        startTime.set(Calendar.MINUTE, 20);
        startTime.set(Calendar.SECOND, 0);
        startTime.set(Calendar.MILLISECOND, 0);
        startTime.set(Calendar.DAY_OF_MONTH, startTime.get(Calendar.DAY_OF_MONTH) - 1);
        Calendar endTime = Calendar.getInstance();
        endTime.set(Calendar.MINUTE, 20);
        List<Map<String, Object>> alarmList = null;
        try (Connection con = sql2o.open()) {
            alarmList = con.createQuery("select * from T_ALARM_RECORD A where A.ACTION_TIME >=:startTime and A.ACTION_TIME<=:endTime")
                    .addParameter("startTime", startTime.getTime())
                    .addParameter("endTime", endTime.getTime())
                    .executeAndFetchTable().asList();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (alarmList != null) {
            bjNum = alarmList.size();
            if (scdyNum > 0) {
                BJPC = bjNum / scdyNum;
                log.info("阶段单元模块报警次数：" + bjNum);
                log.info("阶段内单元模块数：" + scdyNum);
                log.info("报警频次：" + BJPC);
            }
        }

        /**
         * *计算报警处置及时率********
         */
        float jsbjNum = 0;  //及时报警数
        if (alarmList != null) {
            for (Map<String, Object> map : alarmList) {
                Integer ID = Integer.valueOf(((BigDecimal) map.get("id")).toString());
                Date date = (Date) map.get("action_time");
                Calendar c = Calendar.getInstance();
                c.setTime(date);
                //报警发生30分钟内进行处置为及时处置
                c.set(Calendar.MINUTE, c.get(Calendar.MINUTE) + 30);
                try (Connection con = sql2o.open()) {
                    //Integer num = con.createQuery("select count(*) from T_ALARM_HANDLE A where A.ID = :ID and HANDLE_TIME is not null and HANDLE_TIME >=:startTime and HANDLE_TIME <=:endTime ")
                    Integer num = con.createQuery("select count(*) from T_ALARM_HANDLE A where A.ALARMRECORD_ID = :ID and HANDLE_TIME is not null ")
                            //.addParameter("startTime", date)
                            //.addParameter("endTime", c.getTime())
                            .addParameter("ID", ID)
                            .executeScalar(Integer.class);
                    if (num != null && num > 0) {
                        jsbjNum++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        if (bjNum > 0) {
            CZJSL = jsbjNum * 100 / bjNum;
            log.info("报警发生次数：" + bjNum);
            log.info("处置及时的报警次数：" + jsbjNum);
            log.info("报警处置及时率：" + CZJSL);
        }

        /**
         * *计算报警处置符合率********
         */
        float fhNum = 0;   //报警符合数
        float czNum = 0;    //报警处置数
        List<Map<String, Object>> handleAlarmList = null;
        try (Connection con = sql2o.open()) {
            handleAlarmList = con.createQuery("select * from T_ALARM_HANDLE A where A.HANDLE_TIME >=:startTime")
                    .addParameter("startTime", startTime.getTime())
                    .executeAndFetchTable().asList();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (handleAlarmList != null) {
            czNum = handleAlarmList.size();
            fhNum = czNum;
            for (Map<String, Object> map : handleAlarmList) {//两个小时内不再出现为符合报警
                Integer ID = Integer.valueOf(((BigDecimal) map.get("alarmrecord_id")).toString());
                Date date = (Date) map.get("handle_time");
                try (Connection con = sql2o.open()) {//在T_ALARM_RECORD里查询报警记录
                    List<Map<String, Object>> mapList = con.createQuery("select * from T_ALARM_RECORD A where A.ID = :ID")
                            .addParameter("ID", ID)
                            .executeAndFetchTable().asList();
                    if (mapList != null) {
                        Map<String, Object> myMap = mapList.get(0);
                        String VAR_NAME = (String) myMap.get("var_name");
                        Integer ENDTAG_ID = Integer.valueOf(((BigDecimal) myMap.get("endtag_id")).toString());
                        Date date2 = (Date) myMap.get("action_time");

                        Calendar eTime = Calendar.getInstance();
                        eTime.setTime(date2);
                        eTime.set(Calendar.MINUTE, 0);
                        eTime.set(Calendar.SECOND, 0);
                        eTime.set(Calendar.MILLISECOND, 0);
                        eTime.set(Calendar.HOUR_OF_DAY, eTime.get(Calendar.HOUR_OF_DAY) + 2);

                        try (Connection con1 = sql2o.open()) {
                            Integer num = con1.createQuery("select count(*) from T_ALARM_RECORD A where A.VAR_NAME = :VAR_NAME and ENDTAG_ID=:ENDTAG_ID "
                                    + " and ACTION_TIME > :startTime and ACTION_TIME <=:endTime ")
                                    .addParameter("startTime", date2)
                                    .addParameter("endTime", eTime.getTime())
                                    .addParameter("VAR_NAME", VAR_NAME)
                                    .addParameter("ENDTAG_ID", ENDTAG_ID)
                                    .executeScalar(Integer.class);
                            if (num != null && num > 0) {
                                fhNum--;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            CZFHL = fhNum * 100 / czNum;
            log.info("报警处置次数：" + czNum);
            log.info("报警处置符合现场实际数：" + fhNum);
            log.info("报警处置符合率：" + CZFHL);
        }

        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        String sql = "insert into SHPT.SHPT_SCKHZB "
                + "(GLQDM, RQ, MKSXL, SJBDL, SJQQL, BJPC, CZJSL, CZFHL, YJSL, YJTJL, PHHGL, ZSJSL, PZWCL, ZSBH, CYBH, ZRDJL, GKHGL, GLQMC) "
                + "values (:GLQDM, :RQ, :MKSXL, :SJBDL, :SJQQL, :BJPC, :CZJSL, :CZFHL, :YJSL, :YJTJL, :PHHGL, :ZSJSL, :PZWCL, :ZSBH, :CYBH, :ZRDJL, :GKHGL, :GLQMC)";
        try (Connection con = sql2o1.open()) {
            con.createQuery(sql)
                    .addParameter("GLQDM", GLQDM) //管理区代码
                    .addParameter("RQ", calendar.getTime()) //日期
                    .addParameter("MKSXL", MKSXL) //生产单元模块上线率
                    .addParameter("SJBDL", SJBDL) //数据标定率
                    .addParameter("SJQQL", SJQQL) //采集数据齐全率
                    .addParameter("BJPC", BJPC) //单元模块报警频次
                    .addParameter("CZJSL", CZJSL) //报警处置及时率
                    .addParameter("CZFHL", CZFHL) //报警处置符合率
                    .addParameter("YJSL", YJSL) //油井时率
                    .addParameter("YJTJL", YJTJL) //油井躺井率
                    .addParameter("PHHGL", PHHGL) //平衡合格率
                    .addParameter("ZSJSL", ZSJSL) //注水井时率
                    .addParameter("PZWCL", PZWCL) //配注完成率
                    .addParameter("ZSBH", ZSBH) //注水标耗
                    .addParameter("CYBH", CYBH) //采油标耗
                    .addParameter("ZRDJL", ZRDJL) //自然递减率
                    .addParameter("GKHGL", GKHGL) //工况合格率
                    .addParameter("GLQMC", GLQMC) //管理区名称
                    .executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("完成写入生产考核指标数据——现在时刻：" + CommonUtils.date2String(new Date()));
    }

    private float getRealtimeData(String code, String varName) {
        String value = realtimeDataService.getEndTagVarInfo(code, varName);
        if (value != null && !value.isEmpty()) {
            double f = Double.parseDouble(value);
            BigDecimal bg = new BigDecimal(f);
            float f1 = bg.setScale(3, BigDecimal.ROUND_HALF_UP).floatValue();
            return f1;
        } else {
            return 0;
        }
    }
}