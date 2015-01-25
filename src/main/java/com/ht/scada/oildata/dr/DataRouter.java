/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.dr;

import com.google.common.base.Joiner;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.Config;
import com.ht.scada.data.service.RealtimeDataService;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;

/**
 * 数据转储任务
 *
 * @author 赵磊 2015-1-21 22:52:25
 */
@Component
public class DataRouter {

    private static final Logger log = LoggerFactory.getLogger(DataRouter.class);
    @Inject
    @Named("sql2o2")
    protected Sql2o sql2o2;
    @Inject
    @Named("sql2o")
    protected Sql2o sql2o;
    private Connection con;
    private Connection con2;
    @Autowired
    private RealtimeDataService realtimeDataService;
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private void dataRouter() {
        List<Map<String, Object>> taskList = null;
        try {
            con = sql2o.open();
            con2 = sql2o2.open();
            taskList = con.createQuery("select * from D_TASK")
                    .executeAndFetchTable().asList();
        } catch (Exception e) {
        }
        if (taskList != null) {
            for (Map<String, Object> map : taskList) {
                try {
                    String sid = (String) map.get("sid");
                    //分片处理
                    if (!sid.equals(Config.INSTANCE.getConfig().getString("dr.sid", "1"))) {
                        continue;
                    }
                    final String taskName = (String) map.get("name");
                    Integer interval = ((BigDecimal) map.get("interval")).intValue();
                    String timeUnit = ((String) map.get("timeunit")).toLowerCase();
                    Integer delay = ((BigDecimal) map.get("delay")).intValue();
                    final String tableName = (String) map.get("tablename");
                    final Integer isUpdate = ((BigDecimal) map.get("isupdate")).intValue();
                    final Integer isCreateTalbe = ((BigDecimal) map.get("iscreate")).intValue();
                    final String updateKey = ((String) map.get("updatekey")) == null ? null : ((String) map.get("updatekey")).toLowerCase();
                    TimeUnit tu;
                    switch (timeUnit) {
                        case "second":
                            tu = TimeUnit.SECONDS;
                            break;
                        case "minute":
                            tu = TimeUnit.MINUTES;
                            break;
                        default:
                            tu = TimeUnit.SECONDS;
                            break;
                    }
                    final List<Map<String, Object>> fieldList = con.createQuery("select * from D_TASK_FIELD where RWMC=:RWMC")
                            .addParameter("RWMC", taskName)
                            .executeAndFetchTable().asList();
                    final List<Map<String, Object>> recordList = con.createQuery("select * from D_TASK_RECORD where RWMC=:RWMC")
                            .addParameter("RWMC", taskName)
                            .executeAndFetchTable().asList();
                    if ("油井设备档案实时数据".equals(taskName.trim())) {
                        YouJingSbdazc yjsbzc = new YouJingSbdazc(con2, recordList, realtimeDataService);
                        executorService.scheduleAtFixedRate(yjsbzc, delay, interval, tu);
                        continue;
                    }

                    final List<String> fields = new ArrayList<>();
                    if (fieldList != null) {//字段非空
                        for (Map<String, Object> fieldMap : fieldList) {
                            String zdmc = (String) fieldMap.get("zdmc");
                            fields.add(zdmc);
                        }
                        try {
                            if(isCreateTalbe>0) {
                                createTable(tableName, fieldList);//初始化数据库
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        if (recordList != null) {//记录非空
                            final String insertSql = generateInsertSql(tableName, fields);
                            final String updateSql = generateUpdateSql(tableName, fields, updateKey);
                            executorService.scheduleAtFixedRate(new Runnable() {
                                int hasUpdate = isUpdate;

                                @Override
                                public void run() {
                                    log.info("执行——{}——任务", taskName);
                                    Date date = new Date();
                                    try {
                                        if (hasUpdate > 0) {
                                            con2.createQuery("delete from " + tableName).executeUpdate();
                                            insertData(insertSql, fieldList, recordList, date);
                                            hasUpdate = -1;
                                            System.out.println("初始化update");
                                        } else {
                                            if (isUpdate > 0) {
                                                if (updateKey != null && !"".equals(updateKey)) {
                                                    updateData(updateSql, fieldList, recordList, date, updateKey);
                                                    System.out.println("update");
                                                } else {
                                                    log.error("无update条件！");
                                                }

                                            } else {
                                                insertData(insertSql, fieldList, recordList, date);
                                                System.out.println("insert");
                                            }
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            }, delay, interval, tu);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error(e.getMessage());
                    continue;
                }
            }
        }
    }

    /**
     * 生成插入SQL
     */
    private String generateInsertSql(String tableName, List<String> fields) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("insert into " + tableName + " (");
        Joiner.on(", ").appendTo(sqlBuilder, fields);
        sqlBuilder.append(") values \n (:");
        Joiner.on(", :").appendTo(sqlBuilder, fields);
        sqlBuilder.append(")");
        String sql = sqlBuilder.toString();
        log.info(sql);
        return sql;
    }

    /**
     * 插入数据
     */
    private void insertData(String sql, List<Map<String, Object>> fieldList, List<Map<String, Object>> recordList, Date date) {
        con2 = sql2o2.beginTransaction();
        Query query = con2.createQuery(sql);
        int i = 0;
        for (Map<String, Object> map : recordList) {
            String jh = (String) map.get("jlmc");
            String code = (String) map.get("ysjlmc");
            String tsjlmc = (String) map.get("tsjlmc"); //a,b,c
            for (Map<String, Object> fieldMap : fieldList) {
                String zdmc = (String) fieldMap.get("zdmc");
                String tscl = (String) fieldMap.get("tscl");
                String gjz = (String) fieldMap.get("gjz");
                String tsgjz = (String) fieldMap.get("tsgjz");  //i_c-1
                if (tsjlmc != null && !"".equals(tsjlmc) && tsgjz != null && !"".equals(tsgjz)) {
                    String gjzs[] = tsgjz.split("-");
                    gjz = gjzs[0];
                    int codeIndex = Integer.parseInt(gjzs[1]);
                    code = tsjlmc.split(",")[codeIndex - 1];
                }
                String myTscl = tscl == null ? "" : tscl.toLowerCase();
                switch (myTscl) {
                    case "":
                        String value = null;
                        if (gjz != null && !"".equals(gjz.trim())) {
                            value = realtimeDataService.getEndTagVarInfo(code, gjz);
                        }
                        query.addParameter(zdmc, value);
                        break;
                    case "cjsj":    //采集时间
                        query.addParameter(zdmc, date);
                        break;
                    case "jh":      //井号
                        query.addParameter(zdmc, jh);
                        break;
                    case "yjyxzt":  //油井运行状态
                        String zt = null;
                        String s1 = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.RTU_RJ45_STATUS.toString().toLowerCase());
                        if ("true".equals(s1)) {
                            String s2 = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.YOU_JING_YUN_XING.toString().toLowerCase());
                            if ("true".equals(s2)) {
                                zt = "1";
                            } else {
                                zt = "0";
                            }
                        }
                        query.addParameter(zdmc, zt);
                        break;
                    case "null":
                        query.addParameter(zdmc, (String) null);
                        break;
                    case "yx":
                        String yx = null;
                        if (gjz != null && !"".equals(gjz.trim())) {
                            String v = realtimeDataService.getEndTagVarInfo(code, gjz);
                            if ("true".equals(v)) {
                                yx = "1";
                            } else if ("false".equals(v)) {
                                yx = "0";
                            }
                        }
                        query.addParameter(zdmc, yx);
                        break;
                    case "snzt_g":  //使能状态（高位）
                        query.addParameter(zdmc, getSnztG(code));
                        break;
                    case "snzt_d":  //使能状态（低位）
                        query.addParameter(zdmc, getSnztD(code));
                        break;
                    case "zxzt_g":  //在线状态（高位）
                        query.addParameter(zdmc, getZxztG(code));
                        break;
                    case "zxzt_d":  //在线状态（低位）
                        query.addParameter(zdmc, getZxztD(code));
                        break;
                    default:
                        query.addParameter(zdmc, (String) null);
                        break;
                }
            }
            query.addToBatch();
            i++;
            if (i >= 2) {// 每?条执行一次提交
                query.executeBatch();
                i = 0;
            }
        }
        if (i > 0) {
            query.executeBatch();
        }
        con2.commit();
    }

    /**
     * 生成更新SQL
     */
    private String generateUpdateSql(String tableName, List<String> fields, String key) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("update " + tableName + " set ");
        for (String f : fields) {
            sqlBuilder.append(f + "=:" + f + ",");
        }
        sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
        if (key != null && !"".equals(key)) {
            String s1 = key.split(",")[0];
            String s2 = key.split(",")[1];
            sqlBuilder.append(" where " + s1 + " = :" + s2);
        }

        String sql = sqlBuilder.toString();
        log.info(sql);
        return sql;
    }

    /**
     * 更新数据
     */
    private void updateData(String sql, List<Map<String, Object>> fieldList, List<Map<String, Object>> recordList, Date date, String key) {
        String s2 = key.split(",")[1];
        con2 = sql2o2.beginTransaction();
        Query query = con2.createQuery(sql);
        int i = 0;
        for (Map<String, Object> map : recordList) {
            String jh = (String) map.get("jlmc");
            String code = (String) map.get("ysjlmc");
            for (Map<String, Object> fieldMap : fieldList) {
                String zdmc = (String) fieldMap.get("zdmc");
                String tscl = (String) fieldMap.get("tscl");
                String myTscl = tscl == null ? "" : tscl.toLowerCase();
                switch (myTscl) {
                    case "":
                        query.addParameter(zdmc, "1");
                        break;
                    case "cjsj":
                        query.addParameter(zdmc, date);
                        break;
                    case "jh":
                        query.addParameter(zdmc, jh);
                        break;
                    default:
                        query.addParameter(zdmc, "1");
                        break;
                }
            }
            query.addParameter(s2, (String) map.get(s2.toLowerCase()));
            query.addToBatch();
            i++;
            if (i >= 2) {// 每?条执行一次提交
                query.executeBatch();
                i = 0;
            }
        }
        if (i > 0) {
            query.executeBatch();
        }
        con2.commit();
    }

    /**
     * 初始化建表
     */
    private void createTable(String tableName, List<Map<String, Object>> fieldList) {
        String isExitSql = "select count(*) from user_tables where TABLE_NAME='" + tableName + "'";
        int tableNum = con2.createQuery(isExitSql).executeScalar(Integer.class);
        if (tableNum == 0) {
            StringBuilder createTableBuilder = new StringBuilder();
            createTableBuilder.append("create table " + tableName + "(");
            for (Map<String, Object> map : fieldList) {
                String zdmc = (String) map.get("zdmc");
                String zdlx = ((String) map.get("zdlx")).toLowerCase();
                createTableBuilder.append(zdmc + " " + zdlx + ",");
            }
            createTableBuilder.deleteCharAt(createTableBuilder.length() - 1);
            createTableBuilder.append(")");
            String createTableSql = createTableBuilder.toString();
            log.info(createTableSql);
            try {
                con2.createQuery(createTableSql).executeUpdate();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private String getSnztG(String code) {
        String s16 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "wyyth_shi_neng_cgq16")) ? "1" : "0"; //一体化温压变
        String s15 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "zndb_shi_neng_cgq15")) ? "1" : "0"; //智能电表
        String s14 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "bpq_shi_neng_cgq14")) ? "1" : "0"; //变频器
        String s13 = "0"; //掺稀配水阀使能
        String s12 = "0"; //水套液位使能
        String s11 = "0"; //加热炉油温使能
        String s10 = "0"; //烟囱温度使能
        String s9 = "0";  //储罐液位仪使能
        return s16 + s15 + s14 + s13 + s12 + s11 + s10 + s9;
    }

    private String getSnztD(String code) {
        String s8 = "0"; //汇管温度使能
        String s7 = "0"; //汇管压力使能
        String s6 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "ty_shi_neng_cgq6")) ? "1" : "0"; //套压使能
        String s5 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "yw_shi_neng_cgq5")) ? "1" : "0"; //油温使能
        String s4 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "yy_shi_neng_cgq4")) ? "1" : "0"; //油压使能
        String s3 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "wy_shi_neng_cgq3")) ? "1" : "0"; //位移使能
        String s2 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "zh_shi_neng_cgq2")) ? "1" : "0"; //载荷使能
        String s1 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "yth_shi_neng_cgq1")) ? "1" : "0"; //一体化载荷位移使能
        return s8 + s7 + s6 + s5 + s4 + s3 + s2 + s1;
    }
    private String getZxztG(String code) {
        String s16 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "wyyth_zai_xian_cgq16")) ? "1" : "0"; //一体化温压变
        String s15 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "zndb_zai_xian_cgq15")) ? "1" : "0"; //智能电表
        String s14 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "bpq_zai_xian_cgq14")) ? "1" : "0"; //变频器
        String s13 = "0"; //掺稀配水阀
        String s12 = "0"; //水套液位
        String s11 = "0"; //加热炉油温
        String s10 = "0"; //烟囱温度
        String s9 = "0";  //储罐液位仪
        return s16 + s15 + s14 + s13 + s12 + s11 + s10 + s9;
    }

    private String getZxztD(String code) {
        String s8 = "0"; //汇管温度
        String s7 = "0"; //汇管压力
        String s6 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "ty_zai_xian_cgq6")) ? "1" : "0"; //套压
        String s5 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "yw_zai_xian_cgq5")) ? "1" : "0"; //油温
        String s4 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "yy_zai_xian_cgq4")) ? "1" : "0"; //油压
        String s3 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "wy_zai_xian_cgq3")) ? "1" : "0"; //位移
        String s2 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "zh_zai_xian_cgq2")) ? "1" : "0"; //载荷
        String s1 = "true".equals(realtimeDataService.getEndTagVarInfo(code, "yth_zai_xian_cgq1")) ? "1" : "0"; //一体化载荷位移
        return s8 + s7 + s6 + s5 + s4 + s3 + s2 + s1;
    }
}
