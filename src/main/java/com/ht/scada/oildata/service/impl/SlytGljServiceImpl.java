/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
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
import org.sql2o.data.Row;

import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.data.service.impl.HistoryDataServiceImpl2;
import com.ht.scada.oildata.service.SlytGljService;

/**
 * @author 赵磊 2014-12-15 19:04:22
 * @author PengWang 2014-12-29 23:10:00 胜利油田 考核指标及运维日报 ,2015.1.14修改
 * 		   PengWang 2015-01-27 10:44:00  生产运行指标、管理指标 数据获得
 * 		   PengWang 2015-03-24 09:00:00 应孤岛采油厂需求，全面提升 系统运行指标数据
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
        float sjNum = 0;			//水井个数

        List<Map<String, Object>> list = null;
        try (Connection con = sql2o.open()) {
            // list = con.createQuery("select * from T_END_TAG where code <> 'GD1-19-815'").executeAndFetchTable().asList();
            list = con.createQuery("select * from T_END_TAG where code not in ('GD1-19-815','jlz_pt_78','jlz_pt_9','jlz_pt_g2-6-2')")
            		.executeAndFetchTable().asList();
        } catch (Exception e) {
        	e.printStackTrace();
        }
        
        log.info("（1）生产单元模块上线率考核详细指标如下：");
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
                        } else {
                        	log.info("	" + code + " 处RTU未知通讯异常！");
                        }
                        break;
                    case "JI_LIANG_ZHAN":
                        allNum++;
                        if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status"))) {
                            scdyNum++;
                        } else {
                        	log.info("	" + code + " 处RTU未知通讯异常！");
                        }
                        break;
                    case "PEI_SHUI_JIAN":
                        allNum++;
                        if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status"))) {
                            scdyNum++;
                        } else {
                        	log.info("	" + code + " 处RTU未知通讯异常！");
                        }
                        break;
                    case "ZHU_SHUI_JING":	// 暂时未做深层次考核（考核时需要读取关联的RTU在线状态）
                        allNum++;
                        scdyNum++;
                        sjNum++;
                        break;
                    default:
                        break;
                }
            }
        }
        
        List<Map<String, Object>> spList = null;	// 视频上线率统计
        try (Connection con = sql2o.open()) {
            spList = con.createQuery("select RELATEDCODE,VAR_NAME, IPADDRESS from R_NETCHECKING where DEVICETYPE='摄像头'").executeAndFetchTable().asList();
        } catch (Exception e) {
        }
        
        int spOnNum = 0;
        if (spList != null) {
            allNum = allNum + spList.size();
            for (Map<String, Object> map : spList) {
                String varName = (String) map.get("var_name");
                String code = (String) map.get("relatedcode");
                String ip = (String)map.get("ipaddress");
                String status = realtimeDataService.getEndTagVarInfo(code, varName);
                if (status != null && "true".equals(status)) {
                    spOnNum++;
                } else {
                	log.info("	" + code + " 处关联的视频异常！ IP为： " + ip);
                }
            }
            scdyNum = scdyNum + spOnNum;
            log.info("在线视频数/视频总数: " + spOnNum + "/" +spList.size());
        }

        if (allNum > 0) {
            MKSXL = scdyNum * 100 / allNum;
            log.info("实际上线模块数/应上线模块总数：" + scdyNum + "/" + allNum + "	模块上线率： " + MKSXL + "\r\n");
        }
             
        
        /**
         * *计算采集数据齐全率********
         */
        log.info("（2）生产单元数据齐全率考核详细指标如下：");
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
                            	log.info("	" + code + " " +realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing") + " 载荷为： " , getRealtimeDataNew(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) );
                            }
                        }
                       if (getRealtimeDataNew(code, VarSubTypeEnum.TAO_YA.toString().toLowerCase()) >  -10000) {			//套压
//                        if ( "true".equals(realtimeDataService.getEndTagVarInfo(code, "ty_shi_neng_cgq6"))) {			//套压
//                        	System.out.println(getRealtimeDataNew(code, VarSubTypeEnum.TAO_YA.toString().toLowerCase()));
                        	//System.out.println(getRealtimeDataNew(code, "cgq_rtu_time_cgq6"));
                            onNum++;
//                            if ( getRealtimeDataNew(code, "cgq_rtu_time_cgq6") > 0  ) {
//                            	//System.out.println(code + getRealtimeDataNew(code, "cgq_rtu_time_cgq6"));
//                            	 System.out.println(code + "	"+ getRealtimeDataNew(code, "cgq_rtu_time_cgq6") +"	" + getRealtimeDataNew(code, VarSubTypeEnum.TAO_YA.toString().toLowerCase())) ;
//                                 
//                            }
                        }else {
//                        	log.info("使能：" + realtimeDataService.getEndTagVarInfo(code, "ty_shi_neng_cgq6"));
//                        	log.info("	" + code + " " +realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing") + " 套压为： " + getRealtimeDataNew(code, "cgq_rtu_time_cgq6") );
                        }
                        if (getRealtimeDataNew(code, VarSubTypeEnum.JING_KOU_WEN_DU.toString().toLowerCase()) >  -10000) {	// 油压
                            onNum++;
                        }else {
                        	log.info("	" + code+ " " +realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing") + " 井口温度为： " , getRealtimeDataNew(code, VarSubTypeEnum.JING_KOU_WEN_DU.toString().toLowerCase()) );
                        }
                        if (getRealtimeDataNew(code, VarSubTypeEnum.HUI_YA.toString().toLowerCase()) >  -10000) {			// 回压
                            onNum++;	
                        } else {
                        	log.info("	" + code + " " +realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing") + " 回压为： " , getRealtimeDataNew(code, VarSubTypeEnum.HUI_YA.toString().toLowerCase()) );
                        }
                        if ("true".equals(realtimeDataService.getEndTagVarInfo(code, "zndb_zai_xian_cgq15"))) {				//智能电表
                            onNum++;
                        } else {
                        	log.info("	" + code + " " +realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing") + " 智能电表为： " , realtimeDataService.getEndTagVarInfo(code, "zndb_zai_xian_cgq15") );
                        }
                	}
                    
                }
            }
            if (allQqNum > 0) {
                SJQQL = onNum * 100 / allQqNum;
                log.info("实际上线数据项/应上线数据项：" + onNum + "/" + allQqNum + "	数据齐全率： " + SJQQL + " \r\n");
            }
        }

        /**
         * *计算单元模块报警频次********
         */
        log.info("（3）单元模块报警频次：");
        float bjNum = 0;    //报警次数
        // 昨天6:50到今天6:50的报警数目 （用于报警频次考核）
        Calendar startTime = Calendar.getInstance();
        startTime.set(Calendar.DAY_OF_MONTH, startTime.get(Calendar.DAY_OF_MONTH) - 1);
        startTime.set(Calendar.HOUR_OF_DAY, 6);
        startTime.set(Calendar.MINUTE, 50);
        startTime.set(Calendar.SECOND, 0);
        startTime.set(Calendar.MILLISECOND, 0);
       
        Calendar endTime = Calendar.getInstance();
        endTime.set(Calendar.HOUR_OF_DAY, 6);	
        endTime.set(Calendar.MINUTE, 50);	
        endTime.set(Calendar.SECOND, 0);
        endTime.set(Calendar.MILLISECOND, 0);
        System.out.println(startTime.getTime() + " , " + endTime.getTime());
        
        // 昨天6:20到今天6:20的报警数目 （用于报警及时率考核）
        Calendar startTime1 = Calendar.getInstance();
        startTime1.set(Calendar.DAY_OF_MONTH, startTime1.get(Calendar.DAY_OF_MONTH) - 1);
        startTime1.set(Calendar.HOUR_OF_DAY, 6);
        startTime1.set(Calendar.MINUTE, 20);
        startTime1.set(Calendar.SECOND, 0);
        startTime1.set(Calendar.MILLISECOND, 0);
       
        Calendar endTime1 = Calendar.getInstance();
        endTime1.set(Calendar.HOUR_OF_DAY, 6);	
        endTime1.set(Calendar.MINUTE, 20);	
        endTime1.set(Calendar.SECOND, 0);
        endTime1.set(Calendar.MILLISECOND, 0);
        System.out.println(startTime1.getTime() + " , " + endTime1.getTime());
        
        
        List<Map<String, Object>> alarmList = null;		//排除了开井报警的报警（目前现场未对开井报警进行处置，为了增加频次，顾在计算式让报警数往多了统计）
        List<Map<String, Object>> alarmListALL = null; // 所有的报警
        try (Connection con = sql2o.open()) {
            // 仅包含油井报警
        	alarmList = con.createQuery("select * from T_ALARM_RECORD2 A where A.ACTION_TIME >=:startTime1 and A.ACTION_TIME<=:endTime1 and A.alarm_level>4 and info <> '开井报警' and A.ENDTAG_ID in (select T.ID from T_END_TAG T where T.TYPE='YOU_JING')")
                    .addParameter("startTime1", startTime1.getTime())
                    .addParameter("endTime1", endTime1.getTime())
                    .executeAndFetchTable().asList();
        	
        	// 另外包含预警
        	//alarmList = con.createQuery("select * from T_ALARM_RECORD2 A where A.ACTION_TIME >=:startTime1 and A.ACTION_TIME<=:endTime1 and info <> '开井报警' and A.ENDTAG_ID in (select T.ID from T_END_TAG T where T.TYPE in ('YOU_JING','JI_LIANG_ZHAN','PEI_SHUI_JIAN'))")
            //        .addParameter("startTime1", startTime1.getTime())
            //        .addParameter("endTime1", endTime1.getTime())
            //        .executeAndFetchTable().asList();
            
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
                BJPC = alarmListALL.size() / (scdyNum - spOnNum - sjNum);
                //log.info("阶段单元模块报警次数(不含开井)：" + bjNum);
                log.info("阶段单元模块报警次数/实际上线模块数：" + alarmListALL.size() + "/" + (scdyNum - spOnNum - sjNum) + "	报警频次： " + BJPC + " -------------" );
            }
        }

        /**
         * *计算报警处置及时率********
         */
        log.info("（4）报警处置及时率：");
        float jsbjNum = 0;  //及时报警数
        if (alarmList != null) {
            for (Map<String, Object> map : alarmList) {
                Integer ID = Integer.valueOf(((BigDecimal) map.get("id")).toString());
                Date date = (Date) map.get("action_time");
                Calendar c = Calendar.getInstance();
                c.setTime(date);
                c.set(Calendar.MINUTE, c.get(Calendar.MINUTE) + 31);	//报警发生30分钟内进行处置为及时处置
                
                try (Connection con = sql2o.open()) {
//                	Integer num = con.createQuery("select count(*) from T_ALARM_HANDLE_RECORD A where A.ALARMRECORD_ID = :ID and ASSIGN_TIME is not null ")	// 落实时间不为空
                	Integer num = con.createQuery("select count(*) from T_ALARM_HANDLE_RECORD A where A.ALARMRECORD_ID = :ID and ASSIGN_TIME <:endTime ")	// 落实时间不为空
                        	
                			//.addParameter("startTime", date)
                            .addParameter("endTime", c.getTime())
                            .addParameter("ID", ID)
                            .executeScalar(Integer.class);
                    if (num != null && num > 0) {
                        jsbjNum++;
                    } else {
                    	System.out.println("报警ID为： " + ID + "的报警未及时处置！");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        if (bjNum > 0) {
            CZJSL = jsbjNum * 100 / bjNum;
            log.info("报警及时处置数/发生次数：" +jsbjNum + "/" + bjNum + "	报警处置及时率：" + CZJSL + " -----------------------------------------");
        }

        /**
         * *计算报警处置符合率********
         */
        log.info("（5）报警处置符合率：");
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
//                    		+ " and A.alarm_type <> '开关井报警'")
                            .addParameter("ID", ID)
                            .executeAndFetchTable().asList();
                    if (mapList != null) {
                        Map<String, Object> myMap = mapList.get(0);
                        Integer alarmRecordID = Integer.valueOf(((BigDecimal) myMap.get("id")).toString());
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
                                    + " and ACTION_TIME > :startTime and ACTION_TIME <=:endTime"
                                    + " and A.alarm_type not in ( '开关井报警' , 'RTU离线报警')")
                                    .addParameter("startTime", date2)
                                    .addParameter("endTime", eTime.getTime())
                                    .addParameter("VAR_NAME", VAR_NAME)
                                    .addParameter("ENDTAG_ID", ENDTAG_ID)
                                    .executeScalar(Integer.class);
                            if (num != null && num > 0) {
                                fhNum--;
                                System.out.println("重复报警为：" + alarmRecordID  );
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
        log.info("完成写入生产运行指标数据——现在时刻：" + CommonUtils.date2String(new Date()));
    }

    /**
     * 判断实注水是否在配注水量允许范围内
     * @param sz			- 实际注水值 
     * @param pz			- 计划注水值
     * @param lowPersent	- 下限范围
     * @param highPersent	- 上限范围
     * @return
     */
    public boolean inRange(float sz, float pz, float lowPersent, float highPersent) {
    	boolean flag = false;		// 默认不在范围内
    	
    	if ( sz>=pz*lowPersent && sz<=pz*highPersent ){
    		flag = true;
    	}
    	
    	return flag;
    }
    
    /**
	 * 将时间输入串转换成 分钟 "23.40" = 23小时40分中
	 * @param timeStr
	 * @return
	 */
	public int getMinutes (String timeStr) {
		int minutes = 0 ;
		if ( timeStr != null && !"".equals(timeStr)) {
			if ( timeStr.contains(".") ) {
				String xiaoshuStr = timeStr.split("\\.")[1].length()>2 ? timeStr.split("\\.")[1].subSequence(0, 2).toString() : timeStr.split("\\.")[1];	// 防止类似 1.39383838出现
				minutes = Integer.parseInt( timeStr.split("\\.")[0] ) * 60  + Integer.parseInt( xiaoshuStr ) ;
			} else {
				minutes = Integer.parseInt(timeStr) * 60;
			}
		}
		
		return minutes;
	}
    
	 /**
     * 获得"时间范围内"生产时间持续为零的井号(从本地库获得实际数据)
     * @param startTime	- 开始时间
     * @param endTime	- 结束时间
     * @return
     */
    public List<String> getClosedWellInfoFromReal( Calendar startTime, Calendar endTime ) {
    	List<String> codes = new ArrayList<String>();
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");		// 范例: 2015-04-01 15:07:19
        log.info("躺井:  查询开始时间: " + sdf.format( startTime.getTime() ) +  "	结束时间: " + sdf.format(endTime.getTime()) );

//        String sql = "select * from (select avg(scsj) a,jh FROM YS_DBA01@YDK where jh in (select code from t_end_tag where type='YOU_JING')  "
//                + " and rq>=:startTime and rq<=:endTime group by jh) "
//                + " where a<1 ";
        
        String sql = "select * from (select avg(rljyxsj) a, code FROM t_well_daily_data "
        		+ "where code in (select code from t_end_tag where type='YOU_JING' and "
        		+ "code not in ('GD1-18-1', 'GD1-18-505', 'GD1-18C505', 'GD1-19XNB3', 'GD1-17P406', 'GD1-18P405', 'GD1-17X905', 'GD1-18X005', 'GD1-19N3', 'GD1-18N5', 'GD1-19-815') ) "
        		+ "and date_time between :startTime and :endTime group by code) where a<1 ";

        try (Connection con = sql2o.open()) {
            org.sql2o.Query query = con.createQuery(sql);
            query.addParameter("startTime", startTime.getTime());
            query.addParameter("endTime", endTime.getTime());
            List<Row> dataList = query.executeAndFetchTable().rows();
            for (Row row : dataList) {
                codes.add(row.getString("code"));
            }
        }
        // System.out.println("2天生产时间为零的井共 " + codes.size() + " 个！") ;
        return codes;
    }
	
    /**
     * 获得"时间范围内"生产时间持续为零的井号
     * @param startTime	- 开始时间
     * @param endTime	- 结束时间
     * @return
     */
    public List<String> getClosedWellInfo(Calendar startTime, Calendar endTime) {
        List<String> codes = new ArrayList<String>();
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");		// 范例: 2015-04-01 15:07:19
        log.info("躺井:  查询开始时间: " + sdf.format( startTime.getTime() ) +  "	结束时间: " + sdf.format(endTime.getTime()) );

        String sql = "select * from (select avg(scsj) a,jh FROM YS_DBA01@YDK where jh in (select code from t_end_tag where type='YOU_JING')  "
                + " and rq>=:startTime and rq<=:endTime group by jh) "
                + " where a<1 ";

        try (Connection con = sql2o.open()) {
            org.sql2o.Query query = con.createQuery(sql);
            query.addParameter("startTime", startTime.getTime());
            query.addParameter("endTime", endTime.getTime());
            List<Row> dataList = query.executeAndFetchTable().rows();
            for (Row row : dataList) {
                codes.add(row.getString("jh"));
            }
        }
        // System.out.println("2天生产时间为零的井共 " + codes.size() + " 个！") ;
        return codes;
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
    			+ "CFBJ = :CFBJ , CQWCZW = :CQWCZW, BJCZCS = :BJCZCS , QTJS = :QTJS where SFQDM = :SFQDM and RQ >= :RQ";
    	String runStr = sqlInsert;									// 将要执行的串
    	
    	Date todayBegin = CommonUtils.getTodayZeroHour();			// 获取当天零时，如 2014-08-24 00:00:00， 不能用date获取，该类型变量无毫秒级设置，锁定不住时间
    	
    	List<Map<String, Object>> currentRecordToday = null;		// 处置了的报警
    	try (Connection con = sql2o1.open()) {
         	currentRecordToday = con.createQuery(sqlSearch)
         			.addParameter("SFQDM", SFQDM) 					// 管理区代码
         			.addParameter("RQ", todayBegin)					// 日期
                    .executeAndFetchTable().asList();
         	
         	if (currentRecordToday != null && currentRecordToday.size() != 0) {			// 已有当天的记录
        		runStr = sqlUpdate;
        		log.info("更新当前记录，更新时间： " + new Date());
        		log.info("原先时间为： " + currentRecordToday.get(0).get("rq") );
        		log.info("当前共存在今日记录： " + currentRecordToday.size() + " 条！");
        	
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
        		log.info("插入新记录，插入时间： " + todayBegin );
        		
        		con.createQuery(runStr)
             	  .addParameter("SFQDM", SFQDM) 		// 管理区代码
                  .addParameter("RQ", todayBegin) 		// 日期，当日起始时间
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
    
    

    /**
     * 更新方法 - 生产运行指标、经营管理指标
     */
	@Override
	public void runSckhzbUpdateTask() {
		log.info("开始写入生产考核指标（生产运行、经营管理）数据——现在时刻：" + CommonUtils.date2String(new Date()));

        String GLQDM = "30202009";  //管理区代码 YS_DAB08@YDK
        Float YJSL = null;  		//油井时率
        Float YJTJL = null; 		//油井躺井率
        Float PHHGL = null; 		//平衡合格率
        Float ZSJSL = null; 		//注水井时率
        Float PZWCL = null; 		//配注完成率
        Float ZSBH = null;  		//注水标耗
        Float CYBH = null;  		//采油标耗
        Float ZRDJL = null; 		//自然递减率
        Float GKHGL = null; 		//工况合格率
        
        List<Map<String, Object>> list = null;
        try (Connection con = sql2o.open()) {
            list = con.createQuery("select * from T_END_TAG where code not in ('GD1-18-1', 'GD1-18-505', 'GD1-18C505', 'GD1-19XNB3', 'GD1-17P406', 'GD1-18P405', 'GD1-17X905', 'GD1-18X005', 'GD1-19N3', 'GD1-18N5', 'GD1-19-815')").executeAndFetchTable().asList();
        } catch (Exception e) {
        	e.printStackTrace();
        }
		
    	Calendar c = Calendar.getInstance();							// 当日开始时间
      	c.set(Calendar.MINUTE, 0);
      	c.set(Calendar.SECOND, 0);
      	c.set(Calendar.MILLISECOND, 0);
      	c.set(Calendar.HOUR_OF_DAY, 0);
        String wellSearchStr = "select * from ys_dba01@ydk where jh in (select code from t_end_tag where type = 'YOU_JING' and code not in ('GD1-18-1', 'GD1-18-505', 'GD1-18C505', 'GD1-19XNB3', 'GD1-17P406', 'GD1-18P405', 'GD1-17X905', 'GD1-18X005', 'GD1-19N3', 'GD1-18N5', 'GD1-19-815') ) and rq = :datetime";
      	String searchStrWater = "select * from YS_DBA02@YDK where rq = :datetime and jh in (select code from t_end_tag where type = 'ZHU_SHUI_JING' and code not in ('GD1-17X004','GD1-18-303') ) ";
        List<Map<String, Object>> wellDailyRecords = null;		// 油井日报记录 - 源点
        List<Map<String, Object>> waterDailyRecords = null;		// 水井日报记录 - 源点
        
      	try (Connection con = sql2o.open()) {
      		wellDailyRecords = con.createQuery(wellSearchStr)
      				.addParameter("datetime",c.getTime())
      				.executeAndFetchTable().asList();
      		waterDailyRecords = con.createQuery(searchStrWater)
      				.addParameter("datetime",c.getTime())
      				.executeAndFetchTable().asList();
      		
      	} catch (Exception e) {
      		e.printStackTrace();
      	}
      	
        /**
         * *************** 计算油井时率（生产运行指标）  *************
         * 算法：当天油井累计生产时间/范围内油井日历生产时间。
         * 		分为三种：（1）间开井
         * 				 （2）非间开井
         * 				 （3）通讯中断井
         */
        // YJSL - 时间来源，不用从实时库获得，用从关系库获得
        int wellNum = 0; 					// 总井数
        int ctcs_UnConnectedNum = 0; 		// 措施及长停井不进行计算(该处不计算井)
        int ctcsNum = 0; 					// 措施及长停井不进行计算
        Float realProduceTime = 0f;			// 实际生产时间 (分钟)
        Float calerdarProduceTime = 0f;		// 日历生产时间 (分钟)
        int calerdarHour = 24;				// 有效的日历生产小时
        if (wellDailyRecords != null) {
			for (Map<String, Object> map : wellDailyRecords) {
				String code = (String) map.get("jh");

				String ctjStr = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.CTJ.toString());			// 判断是否为长停井
				String gjyyStr = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GJYY.toString());			// 判断是否为措施关井
				if ( (ctjStr != null && !"".equals(ctjStr))  ||	(gjyyStr != null && !"".equals(gjyyStr)) ) {		// 为第二个参数使用的计数
					ctcsNum++;
				}

				// 措施关井+长停井,通讯不上的（能通讯上的一般可以检测生产时间）
				if ( ( (ctjStr != null && !"".equals(ctjStr)) || (gjyyStr != null && !"".equals(gjyyStr)) )
						 &&
					 ( "false".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status")))	) { 				// 此处可以优化，再排除‘关井’的井
					ctcs_UnConnectedNum++;
				} else {
					if (map.get("scsj") == null) {
						log.info(code + " 生产时间为： " + map.get("scsj"));
					} else {
						// String scsj = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJYXSJ.toString()); 	// 从试试库获得生产时间
						String scsj = ((BigDecimal) map.get("scsj")).toString(); 				// 获得源点库生产时间
						realProduceTime = realProduceTime + getMinutes(scsj);					// 时间累加
					}
				}
			}
        }
        wellNum = wellDailyRecords.size();														// 获得总井数
        calerdarProduceTime = (float) ( (wellNum - ctcs_UnConnectedNum) * calerdarHour*60 );	// 获得日历生产时间
        YJSL = ( realProduceTime / calerdarProduceTime );	
        log.info( "油井时率: " + YJSL.floatValue() + "	总井数: " + wellNum + ", 有效井数:" +  ( wellNum - ctcs_UnConnectedNum ) + ", 实际时间/日历时间(分): " + realProduceTime + "/" + calerdarProduceTime );
        
        
        /**
         * *************** 计算油井躺井率（生产运行指标） ************
         * 算法：计算范围：油井总数 - 长停井 - 间开井 - 作业井 - 自喷井。 在剩余的井中，连续两天生产时间为零即认为在计算时刻，该井是躺井的。
         */
        Calendar endTime   = Calendar.getInstance();
        Calendar startTime = Calendar.getInstance();
        startTime.set(Calendar.DAY_OF_MONTH, endTime.get(Calendar.DAY_OF_MONTH) - 1);	// 若8:00后执行，今日日报已经生成，用今日和昨日的，-1；若8:00前执行，用昨天和前天的，-2即可；
        List<String> unrunWells = getClosedWellInfo(startTime, endTime);				// 没有运行的井

        int useableWellNum = 0 ;		// 躺井的井
        int unCalcute = 0;				// 不在基数范围的井（长停、作业、自喷）
        log.info( "共有连续2天未生产井: " + unrunWells.size() + " 个！" );
        for ( int i=0; i< unrunWells.size(); i++ ) {
        	String code = unrunWells.get(i);
        	
        	// 长停井或者措施关井(怎么判断作业) XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        	if (  realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.CTJ.toString()) != null
                    && !"".equals(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.CTJ.toString()))  ) {
                // System.out.println( code + " 为长停井不计！"); 
        		unCalcute ++;
        		continue;       	
            } else if ( "true".equals(realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing")) ) {
            	// System.out.println( code + " 已经开井不计！");   
        		continue;
        	} else {
            	// 此处可以判断该井是不是作业井...，这样又能少一些
            	System.out.println(code + " 关井原因: " + realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GJYY.toString()));
            	// unCalcute ++;
            	//continue;
        	}
        	
        	useableWellNum++;            
        }
        YJTJL =  useableWellNum / (float)( wellNum - unCalcute);
        log.info( "躺井率: " + YJTJL + "		躺井井数/有效井数: " + useableWellNum + "/" + (wellNum - unCalcute) );
        
        
        /**
         * *************** 计算油井平衡合格率（生产运行指标）  ********
         * 算法：暂定60~160为合格范围，计算范围是冲程冲次大于零的井
         */
        int countNum=0; 				// 需要计算的井个数
        int phdQualified = 0 ;			// 处于合格范围的个数
        for (Map<String, Object> map : list) {
            String type = (String) map.get("type");
            String subType = (String) map.get("sub_type");
            String code = (String) map.get("code");
            switch (type) {
                case "YOU_JING":{
                	if ( !"LUO_GAN_BENG".equals(subType) ) {
                		String phlStr = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.PING_HENG_LV.toString());
                		// System.out.println( code + " 平衡率为 : " + phlStr);
                		if (phlStr!=null && !phlStr.equals("") ) {
                			countNum ++;
                			double phl = Double.parseDouble( phlStr );
                			
                			if ( phl*100>=60 && phl*100<=160 ) {
                				phdQualified ++;
                			} else {
                				// System.out.println( code + " 平衡率为 : " + phl * 100);
                			}
                		} else { }
                	}
                }	

                default:
                    break;
            }
        }
        PHHGL = phdQualified/(float)countNum;
        log.info("平衡合格率： " + PHHGL + "	合格数/总数： " + phdQualified + "/" + countNum );
        
        
        /**
         * *************** 计算水井时率（生产运行指标）  *************
         * 算法：（1）日配注量< 12m³的，若完成按24小时计算，若未完成按照实注/配注百分比转换时间；
         * 		（2）日配注量>=12m³的，按照实际的运行时间计算；
         */
        int waterWellNum = waterDailyRecords.size();		// 注水井个数
    	int jhgjs = 0;										// 计划关井个数
        Float realProduceTimeWW = 0f;						// 实际生产时间 (分钟)
        Float calerdarProduceTimeWW = 0f;					// 日历生产时间 (分钟)
        if (waterDailyRecords != null) {
        	for (Map<String, Object> map : waterDailyRecords) {
        		String code = (String) map.get("jh");
        		String bz = (String) map.get("bz");
        		if ( bz!=null && bz.contains("关") ) {
        			jhgjs ++;
        			// log.info( code + " 处于关井状态(不在计算范围): " + bz);
        		} else {
        			if (map.get("scsj") == null) {
        				log.info(code + " 生产时间为： " + map.get("scsj"));
        			} else {
        				// String scsj = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.BAN_LJYXSJ.toString()); 	// 从试试库获得生产时间
        				String scsj = ((BigDecimal) map.get("scsj")).toString(); 			// 获得源点库生产时间
        				realProduceTimeWW = realProduceTimeWW + getMinutes(scsj);			// 时间累加
        			}
        		}
        	}
        }
        calerdarProduceTimeWW = (float) ( (waterWellNum-jhgjs)*calerdarHour*60 );			// 获得日历生产时间
        ZSJSL = realProduceTimeWW / calerdarProduceTimeWW;
        log.info( "水井时率: " + ZSJSL + "	 详情-" + "总井数(有效井数): " + waterWellNum + "(" +  (waterWellNum-jhgjs) + "), 实际生产/日历生产(分): " + realProduceTimeWW + "/" + calerdarProduceTimeWW );
        
        
        /**
         * *************** 计算配注完成率（生产运行指标）  ***********
         * 算法：针对不同层位：（1）合格层（正常层） 90~120
         * 					（2）控制层（高渗层） 70~110
         * 					（3）加强层（欠注层） 90~130
         * （说明：若逻辑不做修改，可与"水井时率"算法合并）
         */
      	// 读取关系库日报数据 (层位对应的中文含义，flu表及)
     
        jhgjs = 0;				// 计划关井个数
      	int whgpzjs = 0;		// 未合格配注注水井井数
      	if (waterDailyRecords != null) {
      		for (Map<String, Object> map : waterDailyRecords) {
      			String code = (String) map.get("jh");
      			String bz = (String) map.get("bz");
      			if ( bz!=null && bz.contains("关") ) {
      				jhgjs ++ ;
      				// log.info( code + " 处于关井状态(不在计算范围): " + bz);
      			} else {
      				String rpzsl =  ((BigDecimal) map.get("rpzsl")).toString();
      				String rzsl =  "";
            	  
      				if ( map.get("rzsl") == null ) {													// （1）实注为空且非计划关井，不合格
      					whgpzjs ++ ;
      					// log.info( code + " 未完成配注！	配注量: " + rpzsl + " , 实注: (空)"  );
      				} else {
      					rzsl =  ((BigDecimal) map.get("rzsl")).toString();
      					if ( !inRange(Float.parseFloat(rzsl), Float.parseFloat(rpzsl), 0.7f, 1.3f) ) {	// (2) 实注未在合格范围内，不合格
      						whgpzjs ++;
      						// log.info( code + " 未完成配注！	配注量: " + rpzsl + " , 实注: " + rzsl  );
      					}
      				}
      			}
      		}
      	}
      	PZWCL = ( 1 - whgpzjs/(float)(waterDailyRecords.size()-jhgjs) );								// 配注合格率
      	log.info( "配注合格率: " + PZWCL + "	总井数(有效井数): " + waterWellNum + "(" +  (waterWellNum-jhgjs) + "),	合格井数: " + (waterDailyRecords.size() - jhgjs - whgpzjs));
      
      	
        /**
         * *************** 计算注水标耗（经营管理指标）  ********（数据来源不明，暂不计算）
         */
        ZSBH = null;  		//注水标耗
        
        /**
         * *************** 计算采油标耗（经营管理指标）  ************
         */
        Float hdlTotal = 0.0f;		// 耗电总量
        Float cydySum = 0.0f;		// 产液量*动液面 的和
        if (wellDailyRecords != null) {
			for (Map<String, Object> map : wellDailyRecords) {
				String code = (String) map.get("jh");

				String ctjStr = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.CTJ.toString());			// 判断是否为长停井
				String gjyyStr = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GJYY.toString());			// 判断是否为措施关井
				if ( ( (ctjStr != null && !"".equals(ctjStr)) || (gjyyStr != null && !"".equals(gjyyStr)) )			// 长停或措施停井且通讯不上的不在计算范围内
						 &&
					 ( "false".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status")))	) {
					// doNothing
				} else {
			
					// 耗电量>0,产液量>0,动液面>0
					if (  (map.get("hdl") != null && ( Float.parseFloat( ((BigDecimal)map.get("hdl")).toString() ) > 0 ) )
							&& ( map.get("rcyl1") !=null && ( Float.parseFloat( ((BigDecimal)map.get("rcyl1")).toString() ) > 0 ) ) 
							&& ( realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DONG_YE_MIAIN.toString()) != null && !"测不出".equals(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DONG_YE_MIAIN.toString())) && Float.parseFloat(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DONG_YE_MIAIN.toString())) > 0 )	) {
						 // System.out.println( code + " 耗电量为： " + map.get("hdl") + ", 产液量: "+map.get("rcyl1") + ", 动液面: " +  realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DONG_YE_MIAIN.toString()) );
						 hdlTotal = hdlTotal + Float.parseFloat( ((BigDecimal)map.get("hdl")).toString() ) * 100;
						 cydySum = cydySum + Float.parseFloat( ((BigDecimal)map.get("rcyl1")).toString() ) * Float.parseFloat(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DONG_YE_MIAIN.toString()));
					} else {
						
					}
				}
			}
		}
        CYBH = hdlTotal/cydySum;  				//采油标耗
        log.info( "采油标耗: " + CYBH + "		KW·100/h·m³·m， 	耗电量*100： " + hdlTotal + ", 产液量*动液面: " + cydySum);
        
        /**
         * *************** 计算自然递减率（经营管理指标）  *******
         * 	要求：前阶段末标定的老井日产量*阶段累计日历时间-老井阶段累计产油量）/前阶段末标定的老井日产量×阶段累计日历时间
         * 		   可以将“前阶段”理解为当前的前一个阶段，“老井阶段”理解为“前阶段”前的一个阶段
         *       按时间顺序ABC,若当前阶段为C，那么"前阶段为"B, "老井阶段"为A
         */
        Date [] periodTimesArray = periodTimes();				// 获得各个阶段起始时间
        List<Map<String, Object>> a_periodCYLSum = null;		// 老井A阶段各个单井累计产液量
        List<Map<String, Object>> b_periodCYLSum = null;		// 前个B阶段各个单井累计产液量
        String searchStr2 = "select jh, sum((rcyl1*(100-hs)/100)) jdljcyl from ys_dba01@ydk where jh in (select code from t_end_tag where type = 'YOU_JING' and code not in ('GD1-18-1', 'GD1-18-505', 'GD1-18C505', 'GD1-19XNB3', 'GD1-17P406', 'GD1-18P405', 'GD1-17X905', 'GD1-18X005', 'GD1-19N3', 'GD1-18N5', 'GD1-19-815') ) and rq between :starttime and :endtime group by jh";
    	try (Connection con = sql2o.open()) {
    		a_periodCYLSum = con.createQuery(searchStr2)
      				.addParameter("starttime",periodTimesArray[0])
      				.addParameter("endtime",periodTimesArray[1])
      				.executeAndFetchTable().asList();
    		
    		b_periodCYLSum = con.createQuery(searchStr2)
      				.addParameter("starttime",periodTimesArray[2])
      				.addParameter("endtime",periodTimesArray[3])
      				.executeAndFetchTable().asList();
      		
      	} catch (Exception e) {
      		e.printStackTrace();
      	}
    	
    	Float aSum = 0.0f;				// 老井A阶段, 所有井累计产液量
    	Float bSum = 0.0f;				// 前个B阶段, 所有井累计产液量
    	if ( a_periodCYLSum !=null && b_periodCYLSum !=null && a_periodCYLSum.size() == b_periodCYLSum.size() ) {
    		
    		for ( int i=0; i<a_periodCYLSum.size(); i++ ) {
    			Map<String, Object> a_map = a_periodCYLSum.get(i);
    			Map<String, Object> b_map = b_periodCYLSum.get(i);
    			
    			String a_code = (String) a_map.get("jh");
    			String b_code = (String) a_map.get("jh");
				// System.out.println("阶段累计产液量: " + a_code + " - " + a_map.get("jdljcyl") + "		" + b_code + " - " + b_map.get("jdljcyl"));
					
				if(  a_map.get("jdljcyl") !=null ) { 
					aSum = aSum + Float.parseFloat( ((BigDecimal)a_map.get("jdljcyl")).toString() );
				}
				
				if(  b_map.get("jdljcyl") !=null ) { 
					bSum = bSum + Float.parseFloat( ((BigDecimal)b_map.get("jdljcyl")).toString() );
				}
    			
    		}
    	} else {
    		// doNothing
    	}
    	ZRDJL =  (bSum-aSum)/bSum ; 		//自然递减率
    	log.info("自然递减率为: " + ( (bSum-aSum)/bSum ) + "	老井A阶段总产油量: " + aSum + "  , 前个B阶段总产油量: " + bSum);

    	
        /**
         * *************** 计算工况合格率（经营管理指标）  *******（暂不计算，数据获取不到）
         */
        GKHGL = null; 		//工况合格率
        
        String sql = "update SHPT.SHPT_SCKHZB set "
        		+ "YJSL = :YJSL, YJTJL = :YJTJL, PHHGL = :PHHGL, ZSJSL = :ZSJSL, PZWCL = :PZWCL, ZSBH = :ZSBH, CYBH = :CYBH, ZRDJL = :ZRDJL, GKHGL = :GKHGL"
        		+ " where glqdm = :glqdm and rq = :datetime";
        try (Connection con = sql2o1.open()) {
            con.createQuery(sql)
                    .addParameter("YJSL", YJSL * 100)	 			//油井时率
                    .addParameter("YJTJL", YJTJL * 100) 			//油井躺井率
                    .addParameter("PHHGL", PHHGL * 100) 			//平衡合格率
                    .addParameter("ZSJSL", ZSJSL * 100) 			//注水井时率
                    .addParameter("PZWCL", PZWCL * 100) 			//配注完成率
                    .addParameter("ZSBH", ZSBH == null ? null : ZSBH ) 				//注水标耗
                    .addParameter("CYBH", CYBH ) 									//采油标耗
                    .addParameter("ZRDJL", ZRDJL == null ? null : ZRDJL * 100) 		//自然递减率
                    .addParameter("GKHGL", GKHGL == null ? null : GKHGL * 100) 		//工况合格率
                    .addParameter("glqdm", GLQDM) 									//管理区代码
                    .addParameter("datetime", c.getTime())							// 日期
                    .executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("完成更新生产考核指标（生产运行、经营管理）数据——现在时刻：" + CommonUtils.date2String(new Date()));
        
	}
	
	/**
	 * 获得当前时间的头两个阶段起始和结束时间, 按时间顺序ABC,若当前阶段为C，那么"前阶段为"B, "前前阶段"为A
	 * @return array[0] - A 阶段起始时间, array[1] - A 阶段结束时间, 
	 * 		   array[2] - B 阶段起始时间, array[3] - B 阶段结束时间, 
	 * 		   array[4] - C 阶段起始时间
	 */
	private Date [] periodTimes () {
		Date [] periodArray = new Date[5];
		
		int periodLong = 3;				// 阶段长度, 以‘月’为单位。正常为1年(12个月),试运行阶段3个月为一周期。(1 2 3),(4 5 6),(7 8 9),(10 11 12)
		Date aPeriodStarttime = null;	// A 阶段起始时间
		Date aPeriodEndtime = null;		// A 阶段结束时间
		Date bPeriodStarttime = null;	// B 阶段起始时间
		Date bPeriodEndtime = null;		// B 阶段结束时间
		Date cPeriodStarttime = null;	// C 阶段起始时间
	
		Calendar currentDay = Calendar.getInstance();										// 当天起始日期
		currentDay.set(Calendar.DAY_OF_MONTH, 1);
		currentDay.set(Calendar.HOUR_OF_DAY, 0);											
		currentDay.set(Calendar.MINUTE, 0);
		currentDay.set(Calendar.SECOND, 0);
		currentDay.set(Calendar.MILLISECOND, 0);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");					// 范例: 2015-04-01 15:07:19
		int monthNum = Integer.parseInt( sdf.format(currentDay.getTime()).split("-")[1] );	// 获得月份
		if ( monthNum>=1 && monthNum<=3 ) {
			currentDay.set(Calendar.MONTH, Calendar.JANUARY);	// 1月
		} else if ( monthNum>=4 && monthNum<=6 ) {
			currentDay.set(Calendar.MONTH, Calendar.APRIL);		// 4月
		} else if ( monthNum>=7 && monthNum<=9 ) {
			currentDay.set(Calendar.MONTH, Calendar.JULY);		// 7月
		} else {
			currentDay.set(Calendar.MONTH, Calendar.OCTOBER);	// 10月
		}
		
	    cPeriodStarttime = currentDay.getTime();											// C阶段起始时间
	    currentDay.set(Calendar.SECOND, currentDay.getTime().getSeconds() - 1 );			// C阶段起始时间-1s，B阶段结束时间
	    bPeriodEndtime = currentDay.getTime();												// B阶段结束时间
	    currentDay.set(Calendar.SECOND, currentDay.getTime().getSeconds() + 1 );			// B阶段结束时间+1s，C阶段起始时间
	    currentDay.set(Calendar.MONTH, currentDay.getTime().getMonth() - periodLong );		// C阶段起始时间 - 阶段月份， B阶段起始时间
	    bPeriodStarttime = currentDay.getTime();											// B阶段起始时间
	    currentDay.set(Calendar.SECOND, currentDay.getTime().getSeconds() - 1 );			// B阶段起始时间-1s，A阶段结束时间
	    aPeriodEndtime = currentDay.getTime();												// A阶段结束时间
	    currentDay.set(Calendar.SECOND, currentDay.getTime().getSeconds() + 1 );			// A阶段结束时间+1s，B阶段起始时间
	    currentDay.set(Calendar.MONTH, currentDay.getTime().getMonth() - periodLong );		// B阶段起始时间 - 阶段月份， A阶段起始时间
	    aPeriodStarttime = currentDay.getTime();											// A阶段起始时间
	    
	    periodArray[0] = aPeriodStarttime;	// 赋值
	    periodArray[1] = aPeriodEndtime;
	    periodArray[2] = bPeriodStarttime;
	    periodArray[3] = bPeriodEndtime;
	    periodArray[4] = cPeriodStarttime;
	    
	    System.out.println("A_Time: " + sdf.format( aPeriodStarttime ) + " ~ " + sdf.format( aPeriodEndtime) + 
	    		"\r\nB_Time: " + sdf.format( bPeriodStarttime ) + " ~ " + sdf.format(bPeriodEndtime) + "\r\nC_Time(当前阶段开始时间): " + sdf.format( cPeriodStarttime ) );
	    
	    return periodArray;
		
	}

	/**
	 * 从本地实际库更新考核数据
	 */
	@Override
	public void runSckhzbUpdateTaskFromRealDate() {
		log.info("开始写入生产考核指标（生产运行、经营管理）数据——现在时刻：" + CommonUtils.date2String(new Date()));

        String GLQDM = "30202009";  //管理区代码 YS_DAB08@YDK
        Float YJSL = null;  		//油井时率
        Float YJTJL = null; 		//油井躺井率
        Float PHHGL = null; 		//平衡合格率
        Float ZSJSL = null; 		//注水井时率
        Float PZWCL = null; 		//配注完成率
        Float ZSBH = null;  		//注水标耗
        Float CYBH = null;  		//采油标耗
        Float ZRDJL = null; 		//自然递减率
        Float GKHGL = null; 		//工况合格率
        
        List<Map<String, Object>> list = null;
        try (Connection con = sql2o.open()) {
            list = con.createQuery("select * from T_END_TAG where code not in ('GD1-18-1', 'GD1-18-505', 'GD1-18C505', 'GD1-19XNB3', 'GD1-17P406', 'GD1-18P405', 'GD1-17X905', 'GD1-18X005', 'GD1-19N3', 'GD1-18N5', 'GD1-19-815')").executeAndFetchTable().asList();
        } catch (Exception e) {
        	e.printStackTrace();
        }
		
    	Calendar c = Calendar.getInstance();					// 当日开始时间
      	c.set(Calendar.MINUTE, 0);
      	c.set(Calendar.SECOND, 0);
      	c.set(Calendar.MILLISECOND, 0);
      	c.set(Calendar.HOUR_OF_DAY, 0);
        // String wellSearchStr = "select * from ys_dba01@ydk where jh in (select code from t_end_tag where type = 'YOU_JING' and code not in ('GD1-18-1', 'GD1-18-505', 'GD1-18C505', 'GD1-19XNB3', 'GD1-17P406', 'GD1-18P405', 'GD1-17X905', 'GD1-18X005', 'GD1-19N3', 'GD1-18N5', 'GD1-19-815') ) and rq = :datetime";
      	String wellSearchStr = "select * from t_well_daily_data where code in (select code from t_end_tag where type = 'YOU_JING' and code not in ('GD1-18-1', 'GD1-18-505', 'GD1-18C505', 'GD1-19XNB3', 'GD1-17P406', 'GD1-18P405', 'GD1-17X905', 'GD1-18X005', 'GD1-19N3', 'GD1-18N5', 'GD1-19-815') ) and date_time = :datetime";
        // String searchStrWater = "select * from YS_DBA02@YDK where rq = :datetime and jh in (select code from t_end_tag where type = 'ZHU_SHUI_JING' and code not in ('GD1-17X004','GD1-18-303') ) ";
      	String searchStrWater = "select * from t_water_well_daily_data where date_time = :datetime and code in (select code from t_end_tag where type = 'ZHU_SHUI_JING' and code not in ('GD1-17X004','GD1-18-303') ) ";
      	
      	List<Map<String, Object>> wellDailyRecords = null;		// 油井日报记录 - 本地
        List<Map<String, Object>> waterDailyRecords = null;		// 水井日报记录 - 本地
        
      	try (Connection con = sql2o.open()) {
      		wellDailyRecords = con.createQuery(wellSearchStr)
      				.addParameter("datetime",c.getTime())
      				.executeAndFetchTable().asList();
      		waterDailyRecords = con.createQuery(searchStrWater)
      				.addParameter("datetime",c.getTime())
      				.executeAndFetchTable().asList();
      	} catch (Exception e) {
      		e.printStackTrace();
      	}
      	
      	// 如果在今天的日报记录未查询到数据
      	if ( (wellDailyRecords == null || wellDailyRecords.size() == 0) || ( waterDailyRecords == null || waterDailyRecords.size() == 0 ) ) {
      		log.info( "今天的日报记录生成异常,考核记录将在10:00利用源点数据生成..." );
      		runSckhzbUpdateTask();	// 源点更新方法
      		return;
      	}
      	
        /**
         * *************** 计算油井时率（生产运行指标）  *************
         * 算法：当天油井累计生产时间/范围内油井日历生产时间。
         * 		分为三种：（1）间开井
         * 				 （2）非间开井
         * 				 （3）通讯中断井
         */
        // YJSL - 时间来源，不用从实时库获得，用从关系库获得
        int wellNum = 0; 					// 总井数
        int ctcs_UnConnectedNum = 0; 		// 措施及长停井不进行计算(该处不计算井)
        int ctcsNum = 0; 					// 措施及长停井不进行计算
        Float realProduceTime = 0f;			// 实际生产时间 (分钟)
        Float calerdarProduceTime = 0f;		// 日历生产时间 (分钟)
        int calerdarHour = 24;				// 有效的日历生产小时
        int www = 0;
        if (wellDailyRecords != null) {
			for (Map<String, Object> map : wellDailyRecords) {
				String code = (String) map.get("code");

				String ctjStr = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.CTJ.toString());			// 判断是否为长停井
				String gjyyStr = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GJYY.toString());			// 判断是否为措施关井
				if ( (ctjStr != null && !"".equals(ctjStr))  ||	(gjyyStr != null && !"".equals(gjyyStr)) ) {		// 为第二个参数使用的计数
					ctcsNum++;
				}

				// 措施关井+长停井,通讯不上的（能通讯上的一般可以检测生产时间）
				if ( ( (ctjStr != null && !"".equals(ctjStr)) || (gjyyStr != null && !"".equals(gjyyStr)) )
						 &&
					 ( "false".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status")))	) { 		// 此处可以优化，再排除‘关井’的井
					ctcs_UnConnectedNum++;
				} else {
					if (map.get("rljyxsj") == null) {
						log.info(code + " 生产时间为： " + map.get("rljyxsj"));
					} else {
						String scsj = ((BigDecimal) map.get("rljyxsj")).toString(); 			// 获得本地库生产时间
						// System.out.println( ++www + ": " +code +  ", 生产时间为： " + scsj + " = " + getMinutes(scsj) + " 分钟！");
						realProduceTime = realProduceTime + getMinutes(scsj);					// 时间累加
					}
				}
			}
        }
        wellNum = wellDailyRecords.size();														// 获得总井数
        calerdarProduceTime = (float) ( (wellNum - ctcs_UnConnectedNum) * calerdarHour*60 );	// 获得日历生产时间
        YJSL = ( realProduceTime / calerdarProduceTime );	
        log.info( "油井时率: " + YJSL.floatValue() + "	总井数: " + wellNum + ", 有效井数:" +  ( wellNum - ctcs_UnConnectedNum ) + ", 实际时间/日历时间(分): " + realProduceTime + "/" + calerdarProduceTime );
        
        
        /**
         * *************** 计算油井躺井率（生产运行指标） ************
         * 算法：计算范围：油井总数 - 长停井 - 间开井 - 作业井 - 自喷井。 在剩余的井中，连续两天生产时间为零即认为在计算时刻，该井是躺井的。
         */
        Calendar endTime   = Calendar.getInstance();
        Calendar startTime = Calendar.getInstance();
        startTime.set(Calendar.DAY_OF_MONTH, endTime.get(Calendar.DAY_OF_MONTH) - 1);	// 若8:00后执行，今日日报已经生成，用今日和昨日的，-1；若8:00前执行，用昨天和前天的，-2即可；
        startTime.set(Calendar.MINUTE, 0);
        startTime.set(Calendar.SECOND, 0);
        startTime.set(Calendar.MILLISECOND, 0);
        startTime.set(Calendar.HOUR_OF_DAY, 0);
        List<String> unrunWells = getClosedWellInfoFromReal(startTime, endTime);				// 没有运行的井

        int useableWellNum = 0 ;		// 躺井的井
        int unCalcute = 0;				// 不在基数范围的井（长停、作业、自喷）
        log.info( "共有连续2天未生产井: " + unrunWells.size() + " 个！" );
        for ( int i=0; i< unrunWells.size(); i++ ) {
        	String code = unrunWells.get(i);
        	
        	// 长停井或者措施关井(怎么判断作业) XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        	if (  realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.CTJ.toString()) != null
                    && !"".equals(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.CTJ.toString()))  ) {
                // System.out.println( code + " 为长停井不计！"); 
        		unCalcute ++;
        		continue;       	
            } else if ( "true".equals(realtimeDataService.getEndTagVarInfo(code, "you_jing_yun_xing")) ) {
            	// System.out.println( code + " 已经开井不计！");   
        		continue;
        	} else {
            	// 此处可以判断该井是不是作业井...，这样又能少一些
        		if ( null!= realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GJYY.toString()) &&				// 暂定，带‘关’字的为作业井，不算躺井
        				realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GJYY.toString()).contains("关") ) {
        			 continue;
        		}
            	// System.out.println(code + " 关井原因: " + realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GJYY.toString()));
            	// unCalcute ++;
            	// continue;
        	}
        	
        	useableWellNum++;            
        }
        YJTJL =  useableWellNum / (float)( wellNum - unCalcute);
        log.info( "躺井率: " + YJTJL + "	躺井井数/有效井数: " + useableWellNum + "/" + (wellNum - unCalcute) );
        
        
        /**
         * *************** 计算油井平衡合格率（生产运行指标）  ********
         * 算法：暂定60~160为合格范围，计算范围是冲程冲次大于零的井
         */
        int countNum=0; 				// 需要计算的井个数
        int phdQualified = 0 ;			// 处于合格范围的个数
        for (Map<String, Object> map : list) {
            String type = (String) map.get("type");
            String subType = (String) map.get("sub_type");
            String code = (String) map.get("code");
            switch (type) {
                case "YOU_JING":{
                	if ( !"LUO_GAN_BENG".equals(subType) ) {
                		String phlStr = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.PING_HENG_LV.toString());
                		// System.out.println( code + " 平衡率为 : " + phlStr);
                		if (phlStr!=null && !phlStr.equals("") ) {
                			countNum ++;
                			double phl = Double.parseDouble( phlStr );
                			
                			if ( phl*100>=60 && phl*100<=160 ) {
                				phdQualified ++;
                			} else {
                				// System.out.println( code + " 平衡率为 : " + phl * 100);
                			}
                		} else { }
                	}
                }	

                default:
                    break;
            }
        }
        PHHGL = phdQualified/(float)countNum;
        log.info("平衡合格率： " + PHHGL + "	合格数/总数： " + phdQualified + "/" + countNum );
        
        
        /**
         * *************** 计算水井时率（生产运行指标）  *************
         * 算法：（1）日配注量< 12m³的，若完成按24小时计算，若未完成按照实注/配注百分比转换时间；
         * 		（2）日配注量>=12m³的，按照实际的运行时间计算；
         */
        int waterWellNum = waterDailyRecords.size();		// 注水井个数
    	int jhgjs = 0;										// 计划关井个数 (如何获得停井的井数，参考源点方法修改)
        Float realProduceTimeWW = 0f;						// 实际生产时间 (分钟)
        Float calerdarProduceTimeWW = 0f;					// 日历生产时间 (分钟)
        if (waterDailyRecords != null) {
        	for (Map<String, Object> map : waterDailyRecords) {
        		String code = (String) map.get("code");
        		if (map.get("yxsj") == null) {
        			log.info(code + " 生产时间为： " + map.get("yxsj"));
        		} else {
        			String scsj = ((BigDecimal) map.get("yxsj")).toString(); 			// 获得源点库生产时间
        			realProduceTimeWW = realProduceTimeWW + getMinutes(scsj);			// 时间累加
        		}
        	}
        }
        calerdarProduceTimeWW = (float) ( (waterWellNum-jhgjs)*calerdarHour*60 );			// 获得日历生产时间
        ZSJSL = realProduceTimeWW / calerdarProduceTimeWW;
        log.info( "水井时率: " + ZSJSL + "	 详情-" + "总井数(有效井数): " + waterWellNum + "(" +  (waterWellNum-jhgjs) + "), 实际生产/日历生产(分): " + realProduceTimeWW + "/" + calerdarProduceTimeWW );
        
        
        /**
         * *************** 计算配注完成率（生产运行指标）  ***********
         * 算法：针对不同层位：（1）合格层（正常层） 90~120
         * 					（2）控制层（高渗层） 70~110
         * 					（3）加强层（欠注层） 90~130
         * （说明：若逻辑不做修改，可与"水井时率"算法合并）
         */
      	// 读取关系库日报数据 (层位对应的中文含义，flu表及)
     
        jhgjs = 0;				// 计划关井个数
      	int whgpzjs = 0;		// 未合格配注注水井井数
      	if (waterDailyRecords != null) {
      		for (Map<String, Object> map : waterDailyRecords) {
      			String rpzsl =  ((BigDecimal) map.get("rpzl")).toString();
      			String rzsl =  "";
            	  
      			if ( map.get("ljzsl") == null ) {													// （1）实注为空且非计划关井，不合格
      				whgpzjs ++ ;
      				// log.info( code + " 未完成配注！	配注量: " + rpzsl + " , 实注: (空)"  );
      			} else {
      				rzsl =  ((BigDecimal) map.get("ljzsl")).toString();
      				if ( !inRange(Float.parseFloat(rzsl), Float.parseFloat(rpzsl), 0.7f, 1.3f) ) {	// (2) 实注未在合格范围内，不合格
      					whgpzjs ++;
      					// log.info( code + " 未完成配注！	配注量: " + rpzsl + " , 实注: " + rzsl  );
      				}
      			}
      		}
      	}
      	PZWCL = ( 1 - whgpzjs/(float)(waterDailyRecords.size()-jhgjs) );							// 配注合格率
      	log.info( "配注合格率: " + PZWCL + "	总井数(有效井数): " + waterWellNum + "(" +  (waterWellNum-jhgjs) + "),	合格井数: " + (waterDailyRecords.size() - jhgjs - whgpzjs));
      
      	
        /**
         * *************** 计算注水标耗（经营管理指标）  ********（数据来源不明，暂不计算）
         */
        ZSBH = null;  		//注水标耗
        
        /**
         * *************** 计算采油标耗（经营管理指标）  ************
         */
        Float hdlTotal = 0.0f;		// 耗电总量
        Float cydySum = 0.0f;		// 产液量*动液面 的和
        if (wellDailyRecords != null) {
			for (Map<String, Object> map : wellDailyRecords) {
				String code = (String) map.get("code");

				String ctjStr = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.CTJ.toString());			// 判断是否为长停井
				String gjyyStr = realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.GJYY.toString());			// 判断是否为措施关井
				if ( ( (ctjStr != null && !"".equals(ctjStr)) || (gjyyStr != null && !"".equals(gjyyStr)) )			// 长停或措施停井且通讯不上的不在计算范围内
						 &&
					 ( "false".equals(realtimeDataService.getEndTagVarInfo(code, "rtu_rj45_status")))	) {
					// doNothing
				} else {
			
					// 耗电量>0,产液量>0,动液面>0
					if (  (map.get("hdl") != null && ( Float.parseFloat( ((BigDecimal)map.get("hdl")).toString() ) > 0 ) )
							&& ( map.get("cyl") !=null && ( Float.parseFloat( ((BigDecimal)map.get("cyl")).toString() ) > 0 ) ) 
							&& ( realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DONG_YE_MIAIN.toString()) != null && !"测不出".equals(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DONG_YE_MIAIN.toString())) && Float.parseFloat(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DONG_YE_MIAIN.toString())) > 0 )	) {
						 // System.out.println( code + " 耗电量为： " + map.get("hdl") + ", 产液量: "+map.get("rcyl1") + ", 动液面: " +  realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DONG_YE_MIAIN.toString()) );
						 hdlTotal = hdlTotal + Float.parseFloat( ((BigDecimal)map.get("hdl")).toString() ) * 100;
						 cydySum = cydySum + Float.parseFloat( ((BigDecimal)map.get("cyl")).toString() ) * Float.parseFloat(realtimeDataService.getEndTagVarInfo(code, RedisKeysEnum.DONG_YE_MIAIN.toString()));
					} else {
						
					}
				}
			}
		}
        CYBH = hdlTotal/cydySum;  				//采油标耗
        log.info( "采油标耗: " + CYBH + "	KW·100/h·m³·m， 	耗电量*100： " + hdlTotal + ", 产液量*动液面: " + cydySum);
        
        /**
         * *************** 计算自然递减率（经营管理指标）  *******
         * 	要求：前阶段末标定的老井日产量*阶段累计日历时间-老井阶段累计产油量）/前阶段末标定的老井日产量×阶段累计日历时间
         * 		   可以将“前阶段”理解为当前的前一个阶段，“老井阶段”理解为“前阶段”前的一个阶段
         *       按时间顺序ABC,若当前阶段为C，那么"前阶段为"B, "老井阶段"为A
         */
        Date [] periodTimesArray = periodTimes();				// 获得各个阶段起始时间
        List<Map<String, Object>> a_periodCYLSum = null;		// 老井A阶段各个单井累计产液量
        List<Map<String, Object>> b_periodCYLSum = null;		// 前个B阶段各个单井累计产液量
        String searchStr2 = "select jh, sum((rcyl1*(100-hs)/100)) jdljcyl from ys_dba01@ydk where jh in (select code from t_end_tag where type = 'YOU_JING' and code not in ('GD1-18-1', 'GD1-18-505', 'GD1-18C505', 'GD1-19XNB3', 'GD1-17P406', 'GD1-18P405', 'GD1-17X905', 'GD1-18X005', 'GD1-19N3', 'GD1-18N5', 'GD1-19-815') ) and rq between :starttime and :endtime group by jh";
    	try (Connection con = sql2o.open()) {
    		a_periodCYLSum = con.createQuery(searchStr2)
      				.addParameter("starttime",periodTimesArray[0])
      				.addParameter("endtime",periodTimesArray[1])
      				.executeAndFetchTable().asList();
    		
    		b_periodCYLSum = con.createQuery(searchStr2)
      				.addParameter("starttime",periodTimesArray[2])
      				.addParameter("endtime",periodTimesArray[3])
      				.executeAndFetchTable().asList();
      		
      	} catch (Exception e) {
      		e.printStackTrace();
      	}
    	
    	Float aSum = 0.0f;				// 老井A阶段, 所有井累计产液量
    	Float bSum = 0.0f;				// 前个B阶段, 所有井累计产液量
    	if ( a_periodCYLSum !=null && b_periodCYLSum !=null && a_periodCYLSum.size() == b_periodCYLSum.size() ) {
    		
    		for ( int i=0; i<a_periodCYLSum.size(); i++ ) {
    			Map<String, Object> a_map = a_periodCYLSum.get(i);
    			Map<String, Object> b_map = b_periodCYLSum.get(i);
    			
    			String a_code = (String) a_map.get("jh");
    			String b_code = (String) a_map.get("jh");
				// System.out.println("阶段累计产液量: " + a_code + " - " + a_map.get("jdljcyl") + "		" + b_code + " - " + b_map.get("jdljcyl"));
					
				if(  a_map.get("jdljcyl") !=null ) { 
					aSum = aSum + Float.parseFloat( ((BigDecimal)a_map.get("jdljcyl")).toString() );
				}
				
				if(  b_map.get("jdljcyl") !=null ) { 
					bSum = bSum + Float.parseFloat( ((BigDecimal)b_map.get("jdljcyl")).toString() );
				}
    			
    		}
    	} else {
    		// doNothing
    	}
    	ZRDJL =  (bSum-aSum)/bSum ; 		//自然递减率
    	log.info("自然递减率为: " + ZRDJL + "	老井A阶段总产油量: " + aSum + "  , 前个B阶段总产油量: " + bSum);

    	
        /**
         * *************** 计算工况合格率（经营管理指标）  *******（暂不计算，数据获取不到）
         */
        GKHGL = null; 		//工况合格率
        
        String sql = "update SHPT.SHPT_SCKHZB set "
        		+ "YJSL = :YJSL, YJTJL = :YJTJL, PHHGL = :PHHGL, ZSJSL = :ZSJSL, PZWCL = :PZWCL, ZSBH = :ZSBH, CYBH = :CYBH, ZRDJL = :ZRDJL, GKHGL = :GKHGL"
        		+ " where glqdm = :glqdm and rq = :datetime";
//        try (Connection con = sql2o1.open()) {
//            con.createQuery(sql)
//                    .addParameter("YJSL", YJSL * 100)	 			//油井时率
//                    .addParameter("YJTJL", YJTJL * 100) 			//油井躺井率
//                    .addParameter("PHHGL", PHHGL * 100) 			//平衡合格率
//                    .addParameter("ZSJSL", ZSJSL * 100) 			//注水井时率
//                    .addParameter("PZWCL", PZWCL * 100) 			//配注完成率
//                    .addParameter("ZSBH", ZSBH == null ? null : ZSBH ) 				//注水标耗
//                    .addParameter("CYBH", CYBH ) 									//采油标耗
//                    .addParameter("ZRDJL", ZRDJL == null ? null : ZRDJL * 100) 		//自然递减率
//                    .addParameter("GKHGL", GKHGL == null ? null : GKHGL * 100) 		//工况合格率
//                    .addParameter("glqdm", GLQDM) 									//管理区代码
//                    .addParameter("datetime", c.getTime())							// 日期
//                    .executeUpdate();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        log.info("完成更新生产考核指标（生产运行、经营管理）数据——现在时刻：" + CommonUtils.date2String(new Date()));
	
	}
	
    
    
}