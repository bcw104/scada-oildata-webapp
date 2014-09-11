/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.service.CommonScdtService;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
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
                if(ctjMap.get(youJing.getCode()) != null) {
                    System.out.println("长停井：" + youJing.getCode());
                    realtimeDataService.putValue(youJing.getCode(), RedisKeysEnum.CTJ.toString(), map.get(youJing.getCode())!=null?"长停井：" + map.get(youJing.getCode()):"长停井");
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
     * 一个月内生产时间平均值为0
     * @return 
     */
    private Map<String, String> getChangTingClosedInfo() {
        Calendar endTime = Calendar.getInstance();
        Calendar startTime = Calendar.getInstance();
        startTime.set(Calendar.MONTH, endTime.get(Calendar.MONTH) - 1);

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
}