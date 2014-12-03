/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.EndTagSubTypeEnum;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarGroupEnum;
import com.ht.scada.data.Config;
import com.ht.scada.data.kv.VarGroupData;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.data.service.RealtimeDataService1;
import com.ht.scada.data.service.RealtimeDataService2;
import com.ht.scada.data.service.impl.HistoryDataServiceImpl2;
import com.ht.scada.oildata.Scheduler;
import com.ht.scada.oildata.service.CommonScdtService;
import com.ht.scada.oildata.webapp.entity.SoeRecord;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import javax.inject.Inject;
import javax.inject.Named;
import org.apache.commons.lang.ArrayUtils;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;
import org.sql2o.data.Row;

/**
 *
 * @author 赵磊 2014-8-14 23:49:22
 */
@Transactional
@Service("commonScdtService")
public class CommonScdtServiceImpl implements CommonScdtService {

    private static final Logger log = LoggerFactory.getLogger(CommonScdtServiceImpl.class);
    @Autowired
    private EndTagService endTagService;
    @Autowired
    private RealtimeDataService realtimeDataService;
    @Autowired
    private RealtimeDataService1 realtimeDataService1;
    @Autowired
    private RealtimeDataService2 realtimeDataService2;
    @Autowired
    private HistoryDataServiceImpl2 historyDataServiceImpl2;
    @Inject
    protected Sql2o sql2o;
//    @Inject
//    @Named("sql2o1")
//    protected Sql2o sql2o1;
    private Map<String, String> myDateMap = new HashMap<>();

    public Sql2o getSql2o() {
        return sql2o;
    }

    public void setSql2o(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    @Override
    public void wellClosedInfo() {
        log.info("开始更新关井原因——现在时刻：" + CommonUtils.date2String(new Date()));
        List<EndTag> youJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        Map<String, String> map = getClosedInfo();
        Map<String, String> ctjMap = getChangTingClosedInfo();
        if (youJingList != null && youJingList.size() > 0) {
            for (EndTag youJing : youJingList) {
                //长停井
                if (ctjMap.get(youJing.getCode()) != null) {
                    System.out.println("长停井：" + youJing.getCode());
                    realtimeDataService.putValue(youJing.getCode(), RedisKeysEnum.CTJ.toString(), map.get(youJing.getCode()) != null ? "长停井：" + map.get(youJing.getCode()) : "长停井");
                } else {
                    realtimeDataService.putValue(youJing.getCode(), RedisKeysEnum.CTJ.toString(), "");
                }
                //措施关井
                if (map.get(youJing.getCode()) != null) {
                    System.out.println("措施关井：" + youJing.getCode());
                    realtimeDataService.putValue(youJing.getCode(), RedisKeysEnum.GJYY.toString(), map.get(youJing.getCode()));
                } else {
                    realtimeDataService.putValue(youJing.getCode(), RedisKeysEnum.GJYY.toString(), "");
                }
            }
        }
        log.info("完成更新关井原因——现在时刻：" + CommonUtils.date2String(new Date()));
    }

    private Map<String, String> getClosedInfo() {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.HOUR_OF_DAY, 0);

        Map<String, String> map = new HashMap<>();

        String sql = "select a.JH jh,a.RQ rq,b.DMMC dmmc,a.BZ bz FROM YS_DBA01@YDK a left join FLA15@YDK b on a.bzdm=b.dm  where a.RQ=:dateTime " + //
                " and a.jh in (select code from t_end_tag where type='YOU_JING') and a.BZDM is not null ";

        try (Connection con = sql2o.open()) {
            org.sql2o.Query query = con.createQuery(sql);
            query.addParameter("dateTime", c.getTime());
            List<Row> dataList = query.executeAndFetchTable().rows();
            for (Row row : dataList) {
                String bz = row.getString("BZ") == null ? "" : ":" + row.getString("BZ");
                map.put(row.getString("jh"), row.getString("dmmc") + bz);
//                log.info(map.get(row.getString("jh")));
            }
        }
        return map;
    }

    /**
     * 三个月内生产时间平均值为0
     *
     * @return
     */
    private Map<String, String> getChangTingClosedInfo() {
        Calendar endTime = Calendar.getInstance();
        Calendar startTime = Calendar.getInstance();
        startTime.set(Calendar.MONTH, endTime.get(Calendar.MONTH) - 3);

        Map<String, String> map = new HashMap<>();

        String sql = "select * from (select avg(scsj) a,jh FROM YS_DBA01@YDK where jh in (select code from t_end_tag where type='YOU_JING')  "
                + " and rq>=:startTime and rq<=:endTime group by jh) "
                + " where a<1 ";

        try (Connection con = sql2o.open()) {
            org.sql2o.Query query = con.createQuery(sql);
            query.addParameter("startTime", startTime.getTime());
            query.addParameter("endTime", endTime.getTime());
            List<Row> dataList = query.executeAndFetchTable().rows();
            for (Row row : dataList) {
//                String bz = row.getString("BZ") == null ? "" : ":" + row.getString("BZ");
                map.put(row.getString("jh"), "CTJ");
                log.info(map.get(row.getString("jh")));
            }
        }
        return map;
    }

    @Override
    public void test() {
        System.out.println("开启测试任务！");
//        txzd();//通讯中断
//        gtDataToRTDB();
//        yjlx();
//        deleteRtdbGT();
//        deleteRtdbYc();
//        getBzgtData();
//        insertScsjData();
//        getBzgtDataFromWetk();
//        reportGtAlarm(Calendar.getInstance());
        netChecking();
        System.out.println("结束测试任务！");
    }

    /**
     * 通讯状态
     */
    private void txzd() {
        int i = 1;
        for (EndTag endTag : Scheduler.youJingList) {
            String status = realtimeDataService.getEndTagVarInfo(endTag.getCode(), "rtu_rj45_status");
            String ctj = realtimeDataService.getEndTagVarInfo(endTag.getCode(), RedisKeysEnum.CTJ.toString());
            String gjyy = realtimeDataService.getEndTagVarInfo(endTag.getCode(), RedisKeysEnum.GJYY.toString());
            if (status.equals("false") && ctj.trim().equals("") && gjyy.trim().equals("")) {
                System.out.println(i + ": " + endTag.getCode() + "通讯不通——" + LocalDateTime.fromCalendarFields(Calendar.getInstance()).toString("yyyy-MM-dd HH:mm:ss"));
                i++;
            }
        }
    }

    /**
     * 油井类型
     */
    private void yjlx() {
        int i = 1;
        for (EndTag endTag : Scheduler.youJingList) {
            System.out.println(i + ": " + endTag.getCode() + ":" + endTag.getSubType());
            i++;
        }
    }

    /**
     * 从历史库中导出功图到实时库
     */
    private void gtDataToRTDB() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = null;
        Date end = null;
        try {
//            start = df.parse("2014-10-15 0:0:0");
//            end = df.parse("2014-10-30 0:0:0");
            start = df.parse(Config.INSTANCE.getConfig().getString("start", "2014-10-15 0:0:0"));
            end = df.parse(Config.INSTANCE.getConfig().getString("end", "2014-10-30 0:0:0"));
        } catch (ParseException ex) {
            java.util.logging.Logger.getLogger(CommonScdtServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (Scheduler.youJingList != null && Scheduler.youJingList.size() > 0) {
            int i = 1;
            for (EndTag youJing : Scheduler.youJingList) {
                try {
                    if (!youJing.getSubType().equals(EndTagSubTypeEnum.YOU_LIANG_SHI.toString()) && !youJing.getSubType().equals(EndTagSubTypeEnum.GAO_YUAN_JI.toString())) {
                        continue;
                    }

                    List<VarGroupData> list = historyDataServiceImpl2.getVarGroupData(youJing.getCode(), VarGroupEnum.YOU_JING_SGT, start, end, 5000);
                    for (VarGroupData data : list) {
                        Map<String, String> map = new HashMap<>();
                        for (String key : data.getYcValueMap().keySet()) {
                            map.put(key, String.valueOf(data.getYcValueMap().get(key)));
                        }
                        for (String key : data.getArrayValueMap().keySet()) {
                            map.put(key, Joiner.on(',').join(ArrayUtils.toObject(data.getArrayValueMap().get(key))));
                        }
//                        realtimeDataService1.putListValue(youJing.getCode() + "_SGT_TIME", String.valueOf(data.getDatetime().getTime()));
//                        realtimeDataService1.updateEndModel(youJing.getCode() + "_" + String.valueOf(data.getDatetime().getTime()) + "_SGT", map);
                    }
                    System.out.println(i + ":" + youJing.getCode() + " 功图数据写入完毕！");
                } catch (Exception e) {
                    log.error(e.toString());
                }
                i++;
            }
        }
    }

    /**
     * 删除实时库历史功图
     */
    private void deleteRtdbGT() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar endCal = Calendar.getInstance();
        endCal.set(Calendar.DAY_OF_MONTH, endCal.get(Calendar.DAY_OF_MONTH) - 15);
        endCal.set(Calendar.HOUR_OF_DAY, 0);
        endCal.set(Calendar.MINUTE, 0);
        endCal.set(Calendar.SECOND, 0);
        endCal.set(Calendar.MILLISECOND, 0);

        if (Scheduler.youJingList != null && Scheduler.youJingList.size() > 0) {
            int i = 1;
            for (EndTag youJing : Scheduler.youJingList) {
                try {
                    if (!youJing.getSubType().equals(EndTagSubTypeEnum.YOU_LIANG_SHI.toString()) && !youJing.getSubType().equals(EndTagSubTypeEnum.GAO_YUAN_JI.toString())) {
                        continue;
                    }
                    List<String> list = realtimeDataService1.lrange(youJing.getCode() + "_SGT_TIME", 0, -1);
                    if (list != null) {
                        for (String key : list) {
                            if (new Date(Long.parseLong(key)).before(endCal.getTime())) {
                                realtimeDataService1.delValue(youJing.getCode() + "_" + key + "_SGT");
                                realtimeDataService1.remListValue(youJing.getCode() + "_SGT_TIME", 0, key);
                                System.out.println("删除 " + youJing.getCode() + " " + df.format(new Date(Long.parseLong(key))) + " 功图！");
                            }
                        }
                    }
                    System.out.println(i + ":" + youJing.getCode() + " 功图数据删除完毕！");
                } catch (Exception e) {
                    log.error(e.toString());
                }
                i++;
            }
        }
    }

    /**
     * 删除密集实时数据
     */
    private void deleteRtdbYc() {
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar endTime = Calendar.getInstance();
        endTime.set(Calendar.DAY_OF_MONTH, endTime.get(Calendar.DAY_OF_MONTH) - 1);
        endTime.set(Calendar.HOUR_OF_DAY, 0);
        endTime.set(Calendar.MINUTE, 0);
        endTime.set(Calendar.SECOND, 0);
        endTime.set(Calendar.MILLISECOND, 0);

        if (Scheduler.youJingList != null && Scheduler.youJingList.size() > 0) {
            for (EndTag youJing : Scheduler.youJingList) {
                String code = youJing.getCode();
                String sql = "select var_name from T_TAG_CFG_TPL t join T_END_TAG e on e.tpl_name=t.tpl_name "
                        + "where e.code =:CODE and var_type='YC'";
                try (Connection con = sql2o.open()) {
                    org.sql2o.Query query = con.createQuery(sql);
                    query.addParameter("CODE", code);
                    List<String> dataList = query.executeAndFetch(String.class);
                    for (String s : dataList) {
                        long l = realtimeDataService2.llen(code + s);
                        System.out.println("数据长度：" + l);
                        if (l > 80) {//一天的数据量
                            System.out.println("数据长度：" + l);
                            realtimeDataService2.ltrim(code + s, 80, -1);
                        }
//                        String result = realtimeDataService2.rpopListValue(code + s);
//                        if (result != null) {
//                            try {
//                                String time = result.split("\\|")[1];
//                                while (sdf.parse(time).before(endTime.getTime())) {
//                                    result = realtimeDataService2.rpopListValue(code + s);
//                                    System.out.println(result);
//                                    time = result.split("\\|")[1];
//                                }
//                            } catch (ParseException ex) {
//                                try {
//                                    String time = result.split("\\|")[1];
//                                    while (sdf.parse(time).before(endTime.getTime())) {
//                                        result = realtimeDataService2.rpopListValue(code + s);
//                                        System.out.println(result);
//                                        time = result.split("\\|")[1];
//                                    }
//                                } catch (Exception e) {
//                                    continue;
//                                }
//                            }
//                        }
                    }
                }
                System.out.println("删除油井数据：" + code);
            }
        }
    }

    /**
     * 从10.67.169.100导入标准功图数据
     */
    private void getBzgtData() {
        if (Scheduler.youJingList != null && Scheduler.youJingList.size() > 0) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (EndTag youJing : Scheduler.youJingList) {
                try {
                    if (!youJing.getSubType().equals(EndTagSubTypeEnum.YOU_LIANG_SHI.toString()) && !youJing.getSubType().equals(EndTagSubTypeEnum.GAO_YUAN_JI.toString())) {
                        continue;
                    }
                    List<Map<String, Object>> list = null;
//                    try (Connection con = sql2o1.open()) {
//                        list = con.createQuery("select sgt,csrq from dca01 where JH=:code and rownum = 1 order by csrq desc ")
//                                .addParameter("code", youJing.getCode())
//                                .executeAndFetchTable().asList();
//                    }
                    if (list != null && !list.isEmpty()) {
                        Map<String, Object> map = list.get(0);
                        String sgt = ((oracle.sql.CLOB) map.get("sgt")).stringValue();
                        Date date = (Date) map.get("csrq");
                        if (date != null) {
                            String time = sdf.format(date);
                            System.out.println("标准功图时间：" + time);
                            realtimeDataService.putValue(youJing.getCode(), RedisKeysEnum.BZ_GT_DATETIME.toString(), time);
                        }
                        if (sgt != null && !sgt.trim().equals("")) {
                            String sgts[] = sgt.split(";");
                            if (sgts != null) {
                                int len = sgts.length;
                                String weiyi[] = new String[len];
                                String zaihe[] = new String[len];
                                for (int i = 0; i < len; i++) {
                                    String wz[] = sgts[i].split(",");
                                    if (wz != null) {
                                        weiyi[i] = wz[0];
                                        zaihe[i] = wz[1];
                                    }
                                }
                                String wy = Joiner.on(',').join(weiyi);
                                String zh = Joiner.on(',').join(zaihe);
//                            System.out.println("标准位移：" + wy);
//                            System.out.println("标准载荷：" + zh);
                                realtimeDataService.putValue(youJing.getCode(), RedisKeysEnum.BZ_WEI_YI.toString(), wy);
                                realtimeDataService.putValue(youJing.getCode(), RedisKeysEnum.BZ_ZAI_HE.toString(), zh);
                                System.out.println(youJing.getCode() + " 标准功图数据写入完毕！");
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error(e.toString());
                }
            }
        }
    }

    /**
     * 从威尔泰克获取标准功图
     */
    @Override
    public void getBzgtDataFromWetk() {
        log.info("开启初始化标准功图任务：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Map<String, Object>> list = null;
        try (Connection con = sql2o.open()) {
            list = con.createQuery("select well_id,well_time from wellwgstd@WELL").executeAndFetchTable().asList();
        }
        if (list != null) {
            for (Map<String, Object> map : list) {
                try {
                    String code = (String) map.get("well_id");
                    Date date = (Date) map.get("well_time");
                    String newDateTime = sdf.format(date);
                    if (myDateMap.get(code) != null && myDateMap.get(code).equals(newDateTime)) {
                        continue;
                    }
//                    log.info(code + " 标准功图时间 : " + newDateTime);

                    //标记标准功图
                    try (Connection con = sql2o.open()) {
                        con.createQuery("update QYSCZH.SCY_SGT_GTCJ set BZGT='1' where jh = :CODE and cjsj = :CJSJ")
                                .addParameter("CODE", code)
                                .addParameter("CJSJ", date)
                                .executeUpdate();
                        log.info(code + newDateTime + " 标记标准功图！");
                    }

                    //将标准功图数据写入实时库
                    List<Map<String, Object>> dataList;
                    try (Connection con = sql2o.open()) {
                        dataList = con.createQuery("select * from wellwgstd@WELL where WELL_ID=:CODE")
                                .addParameter("CODE", code)
                                .executeAndFetchTable().asList();
                    }
                    if (dataList != null) {
                        realtimeDataService.putValue(code, RedisKeysEnum.BZ_GT_DATETIME.toString(), newDateTime);
                        realtimeDataService.putValue(code, RedisKeysEnum.BZ_WEI_YI.toString(), ((String) dataList.get(0).get("well_moves")).replaceAll(";", ","));
                        realtimeDataService.putValue(code, RedisKeysEnum.BZ_ZAI_HE.toString(), ((String) dataList.get(0).get("well_loads")).replaceAll(";", ","));
                        realtimeDataService.putValue(code, RedisKeysEnum.BZ_CHONG_CHENG.toString(), ((BigDecimal) dataList.get(0).get("well_distance")).toString());
                        realtimeDataService.putValue(code, RedisKeysEnum.BZ_CHONG_CI.toString(), ((BigDecimal) dataList.get(0).get("well_times")).toString());
                        realtimeDataService.putValue(code, RedisKeysEnum.BZ_MAX_ZH.toString(), ((BigDecimal) dataList.get(0).get("well_maxload")).toString());
                        realtimeDataService.putValue(code, RedisKeysEnum.BZ_MIN_ZH.toString(), ((BigDecimal) dataList.get(0).get("well_minload")).toString());
                        realtimeDataService.putValue(code, RedisKeysEnum.BZ_GGGL.toString(), ((BigDecimal) dataList.get(0).get("well_rodpower")).toString());
                        realtimeDataService.putValue(code, RedisKeysEnum.BZ_GTMJ.toString(), ((BigDecimal) dataList.get(0).get("well_area")).toString());
                        log.info(code + newDateTime + " 标准功图数据写入实时库！");
                    }
                    myDateMap.put(code, newDateTime);
                } catch (Exception e) {
                    log.error(e.toString());
                    continue;
                }
            }
        }
        log.info("完成初始化标准功图任务：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public void reportGtAlarm(Calendar cal) {
        log.info("开始检测威尔泰克功图报警：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) - 5);
        List<Map<String, Object>> list = null;
        try (Connection con = sql2o.open()) {
            list = con.createQuery("select * from QYSCZH.SYX_BJ_BJXX where BJSJ>:BJSJ")
                    .addParameter("BJSJ", cal.getTime())
                    .executeAndFetchTable().asList();
        } catch (Exception e) {
            log.error(e.toString());
        }
        if (list != null) {
            for (Map<String, Object> map : list) {
                try {
                    String code = (String) map.get("bjd");
                    Date date = (Date) map.get("bjsj");
                    String bjxx = (String) map.get("bjxx");
                    String xx1 = ((BigDecimal) map.get("xx1")).toString();
                    String yz = ((BigDecimal) map.get("yz")).toString();
                    Integer id = null;
                    String tagName = "";
                    for (EndTag endtag : Scheduler.youJingList) {
                        if (endtag.getCode().equals(code)) {
                            id = endtag.getId();
                            tagName = endtag.getName();
                            break;
                        }
                    }
                    SoeRecord record = new SoeRecord(id, tagName, code, "gt_bj", bjxx, bjxx + "越限报警；报警值：" + xx1 + "；阈值：" + yz, true, new Date(), null, null, date, 5, "功图报警");
                    log.info(code + " 发生功图报警！" + bjxx + "越限报警；报警值：" + xx1 + "；阈值：" + yz);
                    realtimeDataService.publish(JSON.toJSONString(record));
                } catch (Exception e) {
                    log.error(e.toString());
                }
            }
        }
        log.info("完成检测威尔泰克功图报警：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
    }

    private void insertScsjData() {
        log.info("开始插入运行时间数据：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        if (Scheduler.youJingList != null && Scheduler.youJingList.size() > 0) {
            String sql = "Insert into T_Oil_Well_Calc_Data"
                    + "(ID, CODE, MINITE, IS_ON, LRSJ) "
                    + "values (:ID, :CODE, :MINITE, :ISON, :LRSJ)";
            for (EndTag youJing : Scheduler.youJingList) {
                String code = youJing.getCode();
                String oldCode = null;
                try (Connection con = sql2o.open()) {
                    String sqlQuery = "select code from T_Oil_Well_Calc_Data where code = :CODE";
                    Query query1 = con.createQuery(sqlQuery);
                    oldCode = query1.addParameter("CODE", code).executeAndFetchFirst(String.class);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (oldCode != null) {
                    continue;
                }
                for (int i = 0; i < 120; i++) {
                    try (Connection con1 = sql2o.open()) {
                        Query query = con1.createQuery(sql);
                        query.addParameter("CODE", code)
                                .addParameter("ID", UUID.randomUUID().toString().replace("-", ""))
                                .addParameter("MINITE", String.valueOf(i))
                                .addParameter("ISON", 1)
                                .addParameter("LRSJ", new Date())
                                .executeUpdate();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                log.info("写入完毕：" + code);
            }
        }
        log.info("结束插入运行时间数据：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public void netChecking() {
        log.info("开始网络诊断：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        List<Map<String, Object>> list = null;
        try (Connection con = sql2o.open()) {
//            list = con.createQuery("select * from R_NETCHECKING where DEVICETYPE not like 'RTU' and DEVICETYPE not like '传%'")
            list = con.createQuery("select * from R_NETCHECKING")
                    .executeAndFetchTable().asList();
        } catch (Exception e) {
            log.error(e.toString());
        }
        if (list != null) {
            for (Map<String, Object> map : list) {
                try {
                    String code = (String) map.get("relatedcode");
                    String varName = (String) map.get("var_name");
                    String ip = (String) map.get("ipaddress");
                    boolean ok = false;
                    if (ip != null && !"".equals(ip.trim())) {
                        ok = isNetOk(ip);
                    } else {
                        continue;
                    }
                    int i = ok ? 1 : 0;

                    String updateSql = "update R_NETCHECKING set status = :STATUS where relatedcode = :CODE ";
                    try (Connection con = sql2o.open()) {
                        con.createQuery(updateSql)
                                .addParameter("CODE", code)
                                .addParameter("STATUS", i)
                                .executeUpdate();
                    } catch (Exception e) {
                        log.error(e.toString());
                    }
                    log.info(code + "——" + varName + "——" + ip + "：" + (ok?"通":"不通"));
                } catch (Exception e) {
                }
            }
        }
        log.info("结束网络诊断：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
    }

    private boolean isNetOk(String ip) {
        Runtime runtime = Runtime.getRuntime(); // 获取当前程序的运行进对象
        Process process = null; // 声明处理类对象
        String line = null; // 返回行信息
        InputStream is = null; // 输入流
        InputStreamReader isr = null; // 字节流
        BufferedReader br = null;
        boolean res = false;// 结果
        try {
            process = runtime.exec("ping " + ip); // PING
            is = process.getInputStream(); // 实例化输入流
            isr = new InputStreamReader(is);// 把输入流转换成字节流
            br = new BufferedReader(isr);// 从字节中读取文本
            while ((line = br.readLine()) != null) {
                if (line.contains("TTL")) {
                    res = true;
                    break;
                }
            }
            is.close();
            isr.close();
            br.close();
        } catch (IOException e) {
            System.out.println(e);
            runtime.exit(1);
        }
        return res;
    }
}