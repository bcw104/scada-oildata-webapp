/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.google.common.base.Joiner;
import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.EndTagSubTypeEnum;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarGroupEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.Config;
import com.ht.scada.data.kv.VarGroupData;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.data.service.RealtimeDataService1;
import com.ht.scada.data.service.impl.HistoryDataServiceImpl2;
import com.ht.scada.oildata.Scheduler;
import com.ht.scada.oildata.service.CommonScdtService;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import javax.inject.Inject;
import org.apache.commons.lang.ArrayUtils;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.sql2o.Connection;
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
//    @Autowired
//    private RealtimeDataService1 realtimeDataService1;
    @Autowired
    private HistoryDataServiceImpl2 historyDataServiceImpl2;
    @Inject
    protected Sql2o sql2o;

    public Sql2o getSql2o() {
        return sql2o;
    }

    public void setSql2o(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    /**
     * 每天8点更新电表读数
     *
     * @author 赵磊
     */
    @Override
    public void dbdsTask() {
        log.info("开始更新电表读数——现在时刻：" + CommonUtils.date2String(new Date()));
        List<EndTag> youJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if (youJingList != null && youJingList.size() > 0) {
            for (EndTag youJing : youJingList) {
                String code = youJing.getCode();
                String num = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.DL_ZX_Z.toString().toLowerCase()) == null ? "0"
                        : realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.DL_ZX_Z.toString().toLowerCase());
                realtimeDataService.putValue(code, RedisKeysEnum.RI_LINGSHI_DBDS.toString(), num);
            }
        }
        log.info("结束更新电表读数——现在时刻：" + CommonUtils.date2String(new Date()));
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

//        System.out.println(LocalDateTime.fromCalendarFields(startTime));
//        System.out.println(LocalDateTime.fromCalendarFields(endTime));

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
//        txzd();//通讯中断
        gtDataToRTDB();
//        yjlx();
//        deleteRtdb();
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
    private void deleteRtdb() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Date start = null;
        Date end = null;
        try {
//            start = df.parse("2014-10-15 0:0:0");
//            end = df.parse("2014-10-30 0:0:0");
//            start = df.parse(Config.INSTANCE.getConfig().getString("start", "2014-10-15 0:0:0"));
            end = df.parse(Config.INSTANCE.getConfig().getString("end", "2014-10-17 0:0:0"));
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

//                    List<String> list = realtimeDataService1.lrange(youJing.getCode() + "_SGT_TIME", 0, -1);
//                    if (list != null) {
//                        for(String key : list) {
//                            if(new Date(Long.parseLong(key)).before(end)) {
//                                realtimeDataService1.delValue(youJing.getCode() + "_" + key + "_SGT"); 
//                                realtimeDataService1.remListValue(youJing.getCode() + "_SGT_TIME", 0, key);
//                                System.out.println("删除 " +youJing.getCode() +" " + df.format(new Date(Long.parseLong(key))) + " 功图！");
//                            }
//                        }
//                    }
                    System.out.println(i + ":" + youJing.getCode() + " 功图数据删除完毕！");
                } catch (Exception e) {
                    log.error(e.toString());
                }
                i++;
            }
        }
    }
}