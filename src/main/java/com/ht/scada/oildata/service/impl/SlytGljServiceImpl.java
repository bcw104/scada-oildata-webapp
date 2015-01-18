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
import java.util.ArrayList;
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
 * @author PengWang 2014-12-29 23:10:00 胜利油田 考核指标及运维日报 ,2015.1.14修改
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
        Float MKSXL = 90f; 			//生产单元模块上线率
        Float SJBDL = null; 		//数据标定率
        Float SJQQL = 90f; 			//采集数据齐全率
        Float BJPC = 2f; 			//单元模块报警频次
        Float CZJSL = 90f;  		//报警处置及时率
        Float CZFHL = 90f; 			//报警处置符合率
        Float YJSL = null;  		//油井时率
        Float YJTJL = null; 		//油井躺井率
        Float PHHGL = null; 		//平衡合格率
        Float ZSJSL = null; 		//注水井时率
        Float PZWCL = null; 		//配注完成率
        Float ZSBH = null;  		//注水标耗
        Float CYBH = null;  		//采油标耗
        Float ZRDJL = null; 		//自然递减率
        Float GKHGL = null; 		//工况合格率
        String GLQMC = "孤岛采油管理四区";

        /**
         * *计算生产单元模块上线率********
         */
        float scdyNum = 0; 			//实际上线的数量
        float allNum = 0;  			//应上线的数量

        List<Map<String, Object>> list = null;
        try (Connection con = sql2o.open()) {
            list = con.createQuery("select * from T_END_TAG where code <> 'GD1-19-815'").executeAndFetchTable().asList();
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
//        if (list != null) {
//            float allQqNum = 0;
//            float onNum = 0;
//            for (Map<String, Object> map : list) {
//                String type = (String) map.get("type");
//                String subType = (String) map.get("sub_type");
//                String code = (String) map.get("code");
//                if ("YOU_JING".equals(type) && "true".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status"))) {
//                    allQqNum = allQqNum + 4;
//                    if ("YOU_LIANG_SHI".equals(subType)) {
//                        allQqNum++;
//                        if (getRealtimeData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) > 0) {
//                            onNum++;
//                        }
//                    }
//                    if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "ty_zai_xian_cgq6"))) {//套压
//                        onNum++;
//                    }
//                    if (getRealtimeData(code, VarSubTypeEnum.JING_KOU_WEN_DU.toString().toLowerCase()) > 0) {
//                        onNum++;
//                    }
//                    if (getRealtimeData(code, VarSubTypeEnum.HUI_YA.toString().toLowerCase()) > 0) {
//                        onNum++;
//                    }
//                    if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "zndb_zai_xian_cgq15"))) {//智能电表
//                        onNum++;
//                    }
//                }
//            }
        log.info( "（数据齐全率）下面打印缺失数据信息： " );
        if (list != null) {
            float allQqNum = 0;
            float onNum = 0;
            for (Map<String, Object> map : list) {
                String type = (String) map.get("type");
                String subType = (String) map.get("sub_type");
                String code = (String) map.get("code");
                if ("YOU_JING".equals(type) && "true".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status"))) {
                	
                	if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing"))) {	// 排除作业井、关井、长停井、措施关井，只留开井
                		allQqNum = allQqNum + 4;
                        if ("YOU_LIANG_SHI".equals(subType) || "GAO_YUAN_JI".equals(subType) ) {
                            allQqNum++;
                            if (getRealtimeDataNew(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) > -10000) {
                                onNum++;
                            } else {
                            	log.info(code + " " +realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing") + " 载荷为： " , getRealtimeDataNew(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) );
                            }
                        }
//                        if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "ty_zai_xian_cgq6"))) {//套压
//                            onNum++;
//                        }else {
//                         	log.info(code + " " +realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing") + " 套压为： " , realtimeDataService.getEndTagVarInfo(code, "ty_zai_xian_cgq6") );
//                        }
                        if (getRealtimeDataNew(code, VarSubTypeEnum.TAO_YA.toString().toLowerCase()) >  -10000) {//套压
                            onNum++;
                        }else {
                        	log.info(code + " " +realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing") + " 套压为： " , getRealtimeDataNew(code, VarSubTypeEnum.TAO_YA.toString().toLowerCase()) );
                        }
                        if (getRealtimeDataNew(code, VarSubTypeEnum.JING_KOU_WEN_DU.toString().toLowerCase()) >  -10000) {
                            onNum++;
                        }else {
                        	log.info(code+ " " +realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing") + " 井口温度为： " , getRealtimeDataNew(code, VarSubTypeEnum.JING_KOU_WEN_DU.toString().toLowerCase()) );
                        }
                        if (getRealtimeDataNew(code, VarSubTypeEnum.HUI_YA.toString().toLowerCase()) >  -10000) {
                            onNum++;
                        } else {
                        	log.info(code + " " +realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing") + " 回压为： " , getRealtimeDataNew(code, VarSubTypeEnum.HUI_YA.toString().toLowerCase()) );
                        }
                        if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "zndb_zai_xian_cgq15"))) {//智能电表
                            onNum++;
                        } else {
                        	log.info(code + " " +realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing") + " 智能电表为： " , realtimeDataService.getEndTagVarInfo(code, "zndb_zai_xian_cgq15") );
                        }
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
        List<Map<String, Object>> alarmList = null;		//排除了开井报警的报警（目前现场未对开井报警进行处置，为了增加频次，顾在计算式让报警数往多了统计）
        List<Map<String, Object>> alarmListALL = null; // 所有的报警
        try (Connection con = sql2o.open()) {
            alarmList = con.createQuery("select * from T_ALARM_RECORD2 A where A.ACTION_TIME >=:startTime and A.ACTION_TIME<=:endTime and A.alarm_level>4 and info <> '开井报警' and A.ENDTAG_ID in (select T.ID from T_END_TAG T where T.TYPE='YOU_JING')")
                    .addParameter("startTime", startTime.getTime())
                    .addParameter("endTime", endTime.getTime())
                    .executeAndFetchTable().asList();
            
            alarmListALL = con.createQuery("select * from T_ALARM_RECORD2 A where A.ACTION_TIME >=:startTime and A.ACTION_TIME<=:endTime ")
                    .addParameter("startTime", startTime.getTime())
                    .addParameter("endTime", endTime.getTime())
                    .executeAndFetchTable().asList();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (alarmListALL != null) {
            bjNum = alarmList.size();	// 不含开井报警的报警数
            if (scdyNum > 0) {
                BJPC = alarmListALL.size() / scdyNum;
                log.info("阶段单元模块报警次数(不含开井)：" + bjNum);
                log.info("阶段单元模块报警次数（总）：" + alarmListALL.size() );
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
                    //Integer num = con.createQuery("select count(*) from T_ALARM_HANDLE A where A.ALARMRECORD_ID = :ID and HANDLE_TIME is not null ")
                	  Integer num = con.createQuery("select count(*) from T_ALARM_HANDLE_RECORD A where A.ALARMRECORD_ID = :ID and ASSIGN_TIME is not null ")	// 落实时间不为空
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
        float fhNum = 0;   	//报警符合数
        float czNum = 0;    //报警处置数
        List<Map<String, Object>> handleAlarmList = null;
        try (Connection con = sql2o.open()) {
            handleAlarmList = con.createQuery("select * from T_ALARM_HANDLE_RECORD A where A.ASSIGN_TIME >=:startTime")
                    .addParameter("startTime", startTime.getTime())
                    .executeAndFetchTable().asList();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (handleAlarmList != null) {
            czNum = handleAlarmList.size();
            fhNum = czNum;
            for (Map<String, Object> map : handleAlarmList) {	//两个小时内不再出现为符合报警
                Integer ID = Integer.valueOf(((BigDecimal) map.get("alarmrecord_id")).toString());
                Date date = (Date) map.get("assign_time");
                try (Connection con = sql2o.open()) {			//在T_ALARM_RECORD里查询报警记录
                    List<Map<String, Object>> mapList = con.createQuery("select * from T_ALARM_RECORD2 A where A.ID = :ID")
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
                            Integer num = con1.createQuery("select count(*) from T_ALARM_RECORD2 A where A.VAR_NAME = :VAR_NAME and ENDTAG_ID=:ENDTAG_ID "
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

        System.out.println("报警频次为： " + BJPC);
        System.out.println("报警处置及时率：" + CZJSL);
        System.out.println("报警处置符合率" + CZFHL);
        
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

    // 判断是否获取到了非空值，空用数字-10000代替
    private float getRealtimeDataNew(String code, String varName) {
        String value = realtimeDataService.getEndTagVarInfo(code, varName);
        if (value != null && !value.isEmpty()) {		// 非空即返回数据库中的真实值，不能返回0，有些值就是0
            double f = Double.parseDouble(value);
            BigDecimal bg = new BigDecimal(f);
            float f1 = bg.setScale(3, BigDecimal.ROUND_HALF_UP).floatValue();
            return f1;
        } else {
            return -10000;								// 没有值
        }
    }


    //  四化运维考核 2015.1.10
    @Override
    public void shywkh() {

    	String SFQDM = "30202009";  	//管理区代码 YS_DAB08@YDK
    	
    	Float RBJS = null;				// 日报警数
    	Float RBJCZS = null;			// 日报警处置数 
    	Float RYJS = null;				// 日预警数
    	Float RYJCZS = null;			// 日预警处置数 
    	Float KJS = null;				// 开井数
    	Float TXGZS = null;				// 通讯故障数
    	Float YJSL = 90f;				// 油井时率
    	Float PHDX = null;				// 平衡度<80 的数目
    	Float PHDZC = null;				// 平衡度80-120 的数目
    	Float PHSD = null;				// 平衡度>120 的数目 
    	Float GJS = null;				// 关井数 
    	Float ZJS = null;				// 总井数
    	String SFQMC = "孤岛采油管理四区";	// 示范区名称
    	Float CFBJ = null;				// 重复报警条数（3小时内出现相同报警）
    	Float CQWCZW = null;			// 长期未处置完成数（24小时内未处置完成的条数） 
    	Float BJCZCS = null;			// 报警处置次数（合并处置的算处置一次） 
    	Float QTJS = null;				// 无功图个数
    	        
        List<Map<String, Object>> list = null;
        try (Connection con = sql2o.open()) {
            list = con.createQuery("select * from T_END_TAG where type = 'YOU_JING' ").executeAndFetchTable().asList();
        } catch (Exception e) {
        }
        
        // ----------------计算 井数、故障井、运行井、停井 （开始） ------------------------------------------------------------
        float wellTotalNum = 0;							// 总井数
        float disconnectWellNum = 0;					// 通讯故障井
        float runWellNum = 0;							// 正常运行井
        float unrunWellNum = 0; 						// 处于停止状态的井，正常停井、措施停井和长停井
        if (list != null) {								// 存在监控对象
        	
            for (Map<String, Object> map : list) {		// 计算油井状态
                String type = (String) map.get("type");
                String code = (String) map.get("code");
                switch (type) {
                    case "YOU_JING":	
                    	wellTotalNum++;			 		// 总井数 + 1
                        //有通讯井+措施关井+长停井
                        if ("false".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status"))) {
                        	
                        	if (realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.CTJ.toString()) != null
                                && !"".equals(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.CTJ.toString()))) {
                        		unrunWellNum ++;
                        	} else if (realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GJYY.toString()) != null
                                    && !"".equals(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GJYY.toString()))) {
                        		unrunWellNum ++ ;
                        	} else {
                        		disconnectWellNum ++ ;		// 通讯故障井 + 1
                        	}
                        } else if ( "true".equals(realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing"))) {
                        	runWellNum ++ ;				// 正常运行井 + 1
                        } else if ("false".equals(realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing"))) {
                        	unrunWellNum ++ ;
                        }
                        
                        break;
                    default:
                        break;
                }
            }
            
            // ---------------- 计算无功图数 ---------------------------------------------------------------------------------------------
            int withoutGT = 0;	// 无功图数
            int withGT =0;		// 应有功图数
            for (Map<String, Object> map : list) {		// 计算功图有无
            	 String subtype = (String) map.get("sub_type");
            	 String code = (String) map.get("code");
            	 if ( subtype.equals("YOU_LIANG_SHI") || subtype.equals("GAO_YUAN_JI") ) {
            		 
            		 if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status")) 
            				 && "true".equals(realtimeDataService.getEndTagVarInfo(code,  "you_jing_yun_xing"))		// 能通讯上且运行的井才判断
            				 && !code.equals("GD1-19-815")) {														// 排除自喷井
            			 
            			 withGT++;
            			 float zdzh = getRealtimeDataNew(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase());
                		 if ( zdzh > -10000 && zdzh != 0 ) {
                			 // System.out.println(code + "  的最大载荷为：  " + getRealtimeDataNew(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) );
                         } else {
                        	withoutGT++;
                         	log.info(code + " 井未捕获到功图数据！" + zdzh + " , 油井状态： " + realtimeDataService.getEndTagVarInfo(code,  "you_jing_yun_xing"));
                         }
                	 }
            		 
            	 } else if ( subtype.equalsIgnoreCase("LUO_GAN_BENG") ) {
            		 // doNothing
            	 }
            }
            QTJS = (float) withoutGT;
            log.info("共有带功图井（排除了停井、长停、措施及通讯中断的井）： " + withGT + " 个！其中，无功图： " + withoutGT + " 个！");
            
        }
        
        ZJS = wellTotalNum;			// 设置总井数
        TXGZS = disconnectWellNum ;	// 通讯故障井数
        KJS = runWellNum; 			// 正常开井数
        GJS = unrunWellNum;			// 停井数
        // ----------------计算 井数、故障井、运行井、停井 （结束） ------------------------------------------------------------
        
        
        // ---------日报警数：报警时间在查询日期的前一天8:00到查询日期8:00所有产生的报警数量（开始）-------------------------------
        Calendar startTime = Calendar.getInstance();		// 查询开始时间
        startTime.set(Calendar.HOUR_OF_DAY, 8);
        startTime.set(Calendar.MINUTE, 00);
        startTime.set(Calendar.SECOND, 0);
        startTime.set(Calendar.MILLISECOND, 0);
        startTime.set(Calendar.DAY_OF_MONTH, startTime.get(Calendar.DAY_OF_MONTH) - 1);
        Calendar endTime = Calendar.getInstance();			// 查询结束时间
        endTime.set(Calendar.HOUR_OF_DAY, 8);
        endTime.set(Calendar.MINUTE, 0);
        endTime.set(Calendar.SECOND, 0);
        endTime.set(Calendar.MILLISECOND, 0);
        
        List<Map<String, Object>> alarmList = null;			// 排除了开井报警的报警（目前现场未对开井报警进行处置）
        try (Connection con = sql2o.open()) {
            alarmList = con.createQuery("select * from T_ALARM_RECORD2 A where A.ACTION_TIME >=:startTime and A.ACTION_TIME<=:endTime and info <> '开井报警' ")
                    .addParameter("startTime", startTime.getTime())
                    .addParameter("endTime", endTime.getTime())
                    .executeAndFetchTable().asList();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (alarmList != null) {
        	RBJS = (float) alarmList.size();	// 不含开井报警的报警数
        }
        
        // -------处置次数： 报警时间在查询日期前一天8:00到查询日期8:00范围内所有报警的处置数量（合并处置算作一次处置操作）---------------
        // -------报警处置完成数：报警时间在查询日期的前一天8:00到查询日期8:00的报警处置完成的数量------------------------------------
        List<Map<String, Object>> handleAlarmList = null;				// 处置报警数
        List<Map<String, Object>> handleCompletedAlarmList = null;		// 处置完成报警数
        try (Connection con = sql2o.open()) {
//            handleAlarmList = con.createQuery("select * from T_ALARM_HANDLE_RECORD A where A.ASSIGN_TIME >=:startTime and A.ASSIGN_TIME<=:endTime")
            handleAlarmList = con.createQuery("select * from T_ALARM_HANDLE_RECORD where alarmrecord_id in ( select id from T_ALARM_RECORD2 A where A.ACTION_TIME between :startTime and :endTime and info <> '开井报警') ")
                    .addParameter("startTime", startTime.getTime())
                    .addParameter("endTime", endTime.getTime())
                    .executeAndFetchTable().asList();
            
//            handleCompletedAlarmList = con.createQuery("select * from T_ALARM_HANDLE_RECORD A where A.ASSIGN_TIME >=:startTime and A.ASSIGN_TIME<=:endTime and A.is_complete = 1 ")
            handleCompletedAlarmList = con.createQuery("select * from T_ALARM_HANDLE_RECORD where alarmrecord_id in (select id from T_ALARM_RECORD2 A where A.ACTION_TIME between :startTime and :endTime) and is_complete = 1 ")
                    .addParameter("startTime", startTime.getTime())
                    .addParameter("endTime", endTime.getTime())
                    .executeAndFetchTable().asList();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (handleAlarmList != null) {
        	BJCZCS = (float) handleAlarmList.size();			// 报警处置次数
        }
        if (handleCompletedAlarmList != null) {
        	RBJCZS = (float) handleCompletedAlarmList.size();	// 报警处置完成数
        }
        

        // --------重复报警：3小时内相同报警点、同一种报警（报警代码相同的）产生条数大于1的报警-----------------------------------------
        Calendar startTime3Before = Calendar.getInstance();		// 查询开始时间
        startTime3Before.set(Calendar.MINUTE, 00);
        startTime3Before.set(Calendar.SECOND, 0);
        startTime3Before.set(Calendar.MILLISECOND, 0);
        startTime3Before.set(Calendar.HOUR_OF_DAY, startTime3Before.get(Calendar.HOUR_OF_DAY)-3);	// 3小时前
        Calendar endTimeNow = Calendar.getInstance();			// 查询结束时间

        List<Map<String, Object>> alarmListIn3Hour = null;		// 排除了开井报警的报警（目前现场未对开井报警进行处置）(3小时内的)
        try (Connection con = sql2o.open()) {
        	alarmListIn3Hour = con.createQuery("select * from T_ALARM_RECORD2 A where A.ACTION_TIME >=:startTime3Before and A.ACTION_TIME<=:endTimeNow and info <> '开井报警' ")
                    .addParameter("startTime3Before", startTime3Before.getTime())
                    .addParameter("endTimeNow", endTimeNow.getTime())
                    .executeAndFetchTable().asList();
            
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (alarmListIn3Hour != null) {
        	int cfNum = 0;												// 重复报警个数
        	ArrayList<String> cfAlarmInfo = new ArrayList<String>();	// 缓存重复报警
            for ( Map<String, Object> map : alarmListIn3Hour ) {		// 最近3个小时内不再出现为符合报警
                Integer endTag_id = Integer.valueOf(((BigDecimal) map.get("endtag_id")).toString());
                String alarmRemark = map.get("remark").toString();		// 利用 井号ID及Remark标记来判断是否是重复报警
                // System.out.println("ENDTAG_ID: " + endTag_id + " , remark: " + alarmRemark );
                
                try (Connection con = sql2o.open()) {				    // 在T_ALARM_RECORD2 里查询报警记录
                    List<Map<String, Object>> mapList = con.createQuery("select * from T_ALARM_RECORD2 A where A.ENDTAG_ID = :ENDTAG_ID and A.REMARK = :REMARK and A.ACTION_TIME >=:startTime3Before and A.ACTION_TIME<=:endTimeNow and info <> '开井报警'")
                            .addParameter("ENDTAG_ID", endTag_id)
                            .addParameter("REMARK", alarmRemark)
                            .addParameter("startTime3Before", startTime3Before.getTime())
                            .addParameter("endTimeNow", endTimeNow.getTime())
                            .executeAndFetchTable().asList();
                    
                    if ( mapList != null && mapList.size() > 1) {
                    	  
                    	   if (!cfAlarmInfo.contains(endTag_id + "," + alarmRemark)) {		// 未出现过的重复 + 1
                    		   cfAlarmInfo.add(endTag_id + "," + alarmRemark);
                        	   cfNum ++ ;	// 重复报警 +1
                        	   // System.out.println("重复个数： " + mapList.size() + ",  ENDTAG_ID: " + endTag_id + " , remark: " + alarmRemark );
                    	   }
                    	 
                    }
                 } catch (Exception e) {
                    e.printStackTrace();
                 }
            }
            CFBJ = (float) cfNum ;		// 设置重复报警数
        }

       
        // --------长期未处置完数：处置完成时间-报警时间>24小时的所有报警（所有完成时间在查询日期前一天8:00到查询日期8:00内的报警处置完成时间
        // 						  与报警时间相差大于24小时的数量 和 在此时间段内状态是未完成的报警在当前时间与报警时间相差大于24小时的报警）
        // 翻译 ----(1) 昨日8:00到今日8:00 所有报警，有解除时间且 解除时间-报警时间 > 24
        // --------(2) 昨日8:00到今日8:00 所有报警，没有解除时间且 当前时间 -报警时间> 24
        List<Map<String, Object>> CZBJ = null;			// 处置了的报警
        try (Connection con = sql2o.open()) {
        	CZBJ = con.createQuery("select * from T_ALARM_HANDLE_RECORD A where A.ALARMRECORD_ID in (select id from T_ALARM_RECORD2 B where B.ACTION_TIME >=:startTime and B.ACTION_TIME<=:endTime and info <> '开井报警') ")
                    .addParameter("startTime", startTime.getTime())
                    .addParameter("endTime", endTime.getTime())
                    .executeAndFetchTable().asList();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // System.out.println("报警个数： " + alarmList.size() + " ,  确认个数： " + CZBJ.size() );
        if (alarmList != null) {
        	int cqwwcbjs = 0;										// 长期未完成报警个数
            for ( Map<String, Object> map : alarmList ) {		
            	Integer id = Integer.valueOf(((BigDecimal) map.get("id")).toString());
            	
            	Date actionTime = (Date) map.get("action_time");
            	Calendar cActionTime = Calendar.getInstance();
            	cActionTime.setTime(actionTime);
            	
            	boolean has = false;			// 在报警处置表中是否存在
            	Integer isComplete = 0;			// 该条报警是否完成
            	Date writeTime = null;			// 完成时间
            	Calendar wWriteTime = Calendar.getInstance();
            	
            	for ( Map<String, Object> map1: CZBJ ) {
            		Integer alarmRecordId = Integer.valueOf(((BigDecimal) map1.get("alarmrecord_id")).toString());
            		writeTime = (Date) map1.get("write_time");
            		
            		if ( id.intValue() == alarmRecordId.intValue() ) {				// 报警号相同,被确认过
            			has = true;
            			isComplete = Integer.valueOf(((BigDecimal) map1.get("is_complete")).toString());
            			if ( isComplete.intValue() == 1 ){
            				wWriteTime.setTime(writeTime);
            				// System.out.println(actionTime.toString() + " , " + writeTime.toString() );
            			}
            			break;
            		}
            	}
            	
            	int shijiancha = 0;
            	if ( has == true && isComplete.intValue() == 1 && writeTime != null  ) {		// 有处置信息，并且完成了，处置时间不为空
            		shijiancha = (int) (( wWriteTime.getTimeInMillis() - cActionTime.getTimeInMillis() ) / 1000/ 60 / 60);
            		//System.out.println(id + " , 时间差1为： " + shijiancha);
            		
            	} else {
            		 // shijiancha = (int) (( Calendar.getInstance().getTimeInMillis() - cActionTime.getTimeInMillis() ) / 1000/ 60 / 60);
            		 // System.out.println(id + " , 时间差2为： " + shijiancha);
            	}
            	
            	if ( shijiancha > 24 ) {
            		cqwwcbjs ++ ;
            	}
            	
            }
            
            CQWCZW = (float) cqwwcbjs;
        }
        
        // --------日预警数：预警时间在查询日期的前一天8:00到查询日期8:00所有产生的预警数量(系统内暂无预警)
        RYJS = (float) 0;
        
        // --------日预警完成数：预警时间在查询日期的前一天8:00到查询日期8:00的预警 处置完成的数量
        RYJCZS = (float) 0;

        // --------平衡度：下行平均功率/上行平均功率*100 -------------------------------------------------------------------------
        int num=0; 				// 平衡率异常，一般为停井导致
        int phdLess80Num = 0;
        int phdMore120Num = 0;
        int phdBetween = 0 ;
        for (Map<String, Object> map : list) {
            String type = (String) map.get("type");
            String subType = (String) map.get("sub_type");
            String code = (String) map.get("code");
            switch (type) {
                case "YOU_JING":{
                	if ( !"LUO_GAN_BENG".equals(subType) ) {
                		String phlStr = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.PING_HENG_LV.toString());
                		if (phlStr!=null && !phlStr.equals("") ) {
                			double phl = Double.parseDouble( phlStr );
                			if (phl*100<80) {
                				phdLess80Num ++;
                			} else if ( phl*100>=80 && phl*100 <= 120 ) {
                				phdBetween ++ ;
                			} else {
                				phdMore120Num ++ ;
                			}
                		} else {
                			num ++;
                		}
                	}
            		// System.out.println(code + " ," + type + ", subType " + subType + " : " +realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.PING_HENG_LV.toString()));
                }	

                default:
                    break;
            }
        }
        // PHDX ，平衡度<80 的数目  	PHDZC，平衡度80-120 的数目  	PHSD，平衡度>120 的数目 
        PHDX = (float) phdLess80Num;	PHDZC = (float) phdBetween;		PHSD = (float) phdMore120Num;
        	
        log.info("总井数：" + ZJS + " ,通讯故障井：" + TXGZS + " ,正常开井数: " + KJS + " , 停井数： " + GJS + " | " + (ZJS-TXGZS-KJS) );
        log.info("日报警数： " + RBJS + " , 处置报警数： " + BJCZCS + " , 报警处置完成数： " + RBJCZS  + " , 重复报警数： " + CFBJ + " , 长期未处置完数： " + CQWCZW );
        log.info("平衡度<80： " + PHDX + "， 平衡度中间： " + PHDZC + ", 平衡度>120: " + PHSD +  ", 空： " + num);
    	
        
        // ------------------------- 数据库插入（更新） --------------------------------------------------------------------------------------------------------
        String sqlSearch = "select * from SHPT.SHPT_SFQYWSJ where SFQDM = :SFQDM and RQ >= :RQ ";
    	String sqlInsert = "insert into SHPT.SHPT_SFQYWSJ "
                 + "(SFQDM, RQ, RBJS, RBJCZS, RYJS, RYJCZS, KJS, TXGZS, YJSL, PHDX, PHDZC, PHSD, GJS, ZJS, SFQMC, CFBJ, CQWCZW, BJCZCS, QTJS) "
                 + "values (:SFQDM, :RQ, :RBJS, :RBJCZS, :RYJS, :RYJCZS, :KJS, :TXGZS, :YJSL, :PHDX, :PHDZC, :PHSD, :GJS, :ZJS, :SFQMC, :CFBJ, :CQWCZW, :BJCZCS, :QTJS)";
    	String sqlUpdate = "update SHPT.SHPT_SFQYWSJ set RQ = :RQ2, RBJS = :RBJS, RBJCZS = :RBJCZS, RYJS = :RYJS ,  RYJCZS = :RYJCZS , KJS = :KJS,"
    			+ "TXGZS = :TXGZS, YJSL = :YJSL, PHDX = :PHDX, PHDZC = :PHDZC , PHSD = :PHSD, GJS = :GJS, ZJS = :ZJS, SFQMC = :SFQMC, "
    			+ "CFBJ = :CFBJ , CQWCZW = :CQWCZW, BJCZCS = :BJCZCS , QTJS = :QTJS where SFQDM = :SFQDM and RQ >:RQ";
    	String runStr = sqlInsert;									// 将要执行的串
    	
    	Date todayBegin= new Date();								// 获得今日起始时间
    	todayBegin.setHours(0);
    	todayBegin.setMinutes(0);
    	todayBegin.setSeconds(0);
    	Date currentTime = new Date();								// 当前时间
    	
    	List<Map<String, Object>> currentRecordToday = null;		// 处置了的报警
    	try (Connection con = sql2o1.open()) {
         	currentRecordToday = con.createQuery(sqlSearch)
         			.addParameter("SFQDM", SFQDM) 					// 管理区代码
         			.addParameter("RQ", todayBegin) 				// 日期
                    .executeAndFetchTable().asList();
         	
         	
         	if (currentRecordToday != null && currentRecordToday.size() != 0) {			// 已有当天的记录
        		runStr = sqlUpdate;
        		currentTime = todayBegin;
        		log.info("更新当前记录，更新时间： " + new Date());
        		log.info("原先时间为： " + currentRecordToday.get(0).get("rq") );
        		
        		con.createQuery(runStr)
           	      .addParameter("SFQDM", SFQDM) 		// 管理区代码
           	      .addParameter("RQ2", new Date() )		// 更新时间
                  .addParameter("RQ", todayBegin) 		// 当日开始日期
                  .addParameter("RBJS", RBJS) 			// 日报警数
                  .addParameter("RBJCZS", RBJCZS)		// 日报警处置数
                  .addParameter("RYJS", RYJS) 			// 日预警数
                  .addParameter("RYJCZS", RYJCZS) 		// 日预警处置数
                  .addParameter("KJS", KJS) 			// 开井数
                  .addParameter("TXGZS", TXGZS) 		// 通讯故障数
                  .addParameter("YJSL", YJSL) 			// 油井时率
                  .addParameter("PHDX", PHDX) 			// 平衡度<80 的数目
                  .addParameter("PHDZC", PHDZC)         // 平衡度80-120 的数目
                  .addParameter("PHSD", PHSD) 			// 平衡度>120 的数目 
                  .addParameter("GJS", GJS) 			// 关井数
                  .addParameter("ZJS", ZJS) 			// 总井数
                  .addParameter("SFQMC", SFQMC) 		// 示范区名称
                  .addParameter("CFBJ", CFBJ)			// 重复报警
                  .addParameter("CQWCZW", CQWCZW) 		// 长期未处置完报警数
                  .addParameter("BJCZCS", BJCZCS) 		// 报警处置次数
                  .addParameter("QTJS", QTJS) 			// 无功图数
                  .executeUpdate();
        	 	 
         	} else {																	// 还没有当天的记录
         		
        		runStr = sqlInsert;
        		log.info("插入新记录，插入时间： " + new Date());
        		
        		con.createQuery(runStr)
             	  .addParameter("SFQDM", SFQDM) 		// 管理区代码
                  .addParameter("RQ", new Date()) 		// 日期
                  .addParameter("RBJS", RBJS) 			// 日报警数
                  .addParameter("RBJCZS", RBJCZS)		// 日报警处置数
                  .addParameter("RYJS", RYJS) 			// 日预警数
                  .addParameter("RYJCZS", RYJCZS) 		// 日预警处置数
                  .addParameter("KJS", KJS) 			// 开井数
                  .addParameter("TXGZS", TXGZS) 		// 通讯故障数
                  .addParameter("YJSL", YJSL) 			// 油井时率
                  .addParameter("PHDX", PHDX) 			// 平衡度<80 的数目
                  .addParameter("PHDZC", PHDZC)         // 平衡度80-120 的数目
                  .addParameter("PHSD", PHSD) 			// 平衡度>120 的数目 
                  .addParameter("GJS", GJS) 			// 关井数
                  .addParameter("ZJS", ZJS) 			// 总井数
                  .addParameter("SFQMC", SFQMC) 		// 示范区名称
                  .addParameter("CFBJ", CFBJ)			// 重复报警
                  .addParameter("CQWCZW", CQWCZW) 		// 长期未处置完报警数
                  .addParameter("BJCZCS", BJCZCS) 		// 报警处置次数
                  .addParameter("QTJS", QTJS) 			// 无功图数
                  .executeUpdate();
        		
        	}
         	 
         } catch (Exception e) {
             e.printStackTrace();
         }
    	 
    }
    
    
}