/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.ht.scada.oildata.dr;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;

import com.ht.scada.data.service.RealtimeDataService;

/**
 * 油井设备档案实时数据转储
 * @author PengWang 2015.1.26
 */
public class YouJingSbdazc implements Runnable {
	
    private Sql2o sql2o;
    private List<Map<String, Object>> recordList;
    private RealtimeDataService realtimeDataService;
    
    // 1-载荷位移一体化传感器、	2-载荷传感器、 3-位移传感器、 4-回压传感器、 5-油温传感器、 6-套压传感器、 7-汇管压力传感器、 	8-汇管温度传感器、 9-井口储罐液位仪、 10-加热炉烟囱温度传感器、
	// 11-加热炉油温传感器、 12-水套液位传感器、 13-掺稀配水阀、 14-变频器（设备）、 15-智能电表（设备）、 16-温度压力一体化传感器、 17-无线死点开关传感器、 18-RTU
    // 电池低电量标示，固定字符串数组
    private static String [] dianChiBiaoShiArray = {"yth_dian_chi_cgq1", "zh_dian_chi_cgq2", "wy_dian_chi_cgq3", "yy_dian_chi_cgq4", "yw_dian_chi_cgq5", 
    	"ty_dian_chi_cgq6", "hgyl_dian_chi_cgq7", "hgwd_dian_chi_cgq8", "ywy_dian_chi_cgq9", "jrlycwd_dian_chi_cgq10", "jrlyw_dian_chi_cgq11", 
    	"styw_dian_chi_cgq12", "cxpsf_dian_chi_cgq13", "bpq_dian_chi_cgq14", "zndb_dian_chi_cgq15", "wyyth_dian_chi_cgq16", "", "" };

    public YouJingSbdazc(Sql2o con2, List<Map<String, Object>> recordList, RealtimeDataService realtimeDataService) {
        this.sql2o = con2;
        this.recordList = recordList;
        this.realtimeDataService = realtimeDataService;
    }

    @Override
    public void run() {
    	
    	// 插入语句
    	String insertSqlStr = "insert into  QYSCZH.sjc_ss_sb "
    			+ "(SBBM, YBLX, CJSJ, YBDM, CJDM, ZDLC1, ZXLC1, XSWS1, ZDLC2, ZXLC2, XSWS2, ZDLC3, ZXLC3, XSWS3, CC_N, CC_Y, CC_R, TJLC, XIH1, XIH2, XIH3, XUH1, XUH2, QDDM, SYDL, DDLBS, YXSJ_G, YXSJ_D) "
    			+ "values (:SBBM, :YBLX, :CJSJ, :YBDM, :CJDM, :ZDLC1, :ZXLC1, :XSWS1, :ZDLC2, :ZXLC2, :XSWS2, :ZDLC3, :ZXLC3, :XSWS3, :CC_N, :CC_Y, :CC_R, :TJLC, :XIH1, :XIH2, :XIH3, :XUH1, :XUH2, :QDDM, :SYDL, :DDLBS, :YXSJ_G, :YXSJ_D)";
    	
    	Connection con2 = sql2o.beginTransaction();	
    	Query query = con2.createQuery(insertSqlStr);
    	 
    	for ( int i=0; i<recordList.size(); i++ ) {										// 处理每口井
    		
    		int ybNum = 18;																// 仪表类型码 1 ~18 (18为RTU单独处理)
    		for (int yblxm = 1; yblxm <=ybNum; yblxm++ ) {								// 处理每个传感器
    			
    			// 变量声明 （且每次相当于清空）----------------------------------------------------------------------------------------------------------------
    			String SBBM = "";			// 设备编码：井号、泵编码、罐编码等
    	    	String YBLX = "";			// 仪表类型：参考sfl_yblx，对应中文
    	    	String YBDM = "";			// 仪表代码，即RTU中设备代码
    	    	String CJDM = "";			// 厂家代码
    	    	Float ZDLC1 = null;			// 最大量程1，默认只有一个参数采集的，只采最大、最小量程
    	    	Float ZXLC1 = null;			// 最小量程1，默认只有一个参数采集的，只采最大、最小量程
    	    	Float XSWS1 = null;			// 小数位数1，默认只有一个参数采集的，只采最大、最小量程
    	    	Float ZDLC2 = null;			// 最大量程2，对于温、压一体的两个量程，掺稀的三个量程：流量、温度、压力
    	    	Float ZXLC2 = null;			// 最小量程2
    	    	Float XSWS2 = null;			// 小数位数2
    	    	Float ZDLC3 = null;			// 最大量程3，对于温、压一体的两个量程，掺稀的三个量程：流量、温度、压力
    	    	Float ZXLC3 = null;			// 最小量程3
    	    	Float XSWS3 = null;			// 小数位数3
    	    	Float CC_N = null;			// 出厂年份
    	    	Float CC_Y = null;			// 出厂月份
    	    	Float CC_R = null;			// 出厂日
    	    	Float TJLC = null;			// 调教量程
    	    	Float XIH1 = null;			// 产品序列号 -- 型号1
    	    	Float XIH2 = null;			// 产品序列号 -- 型号2
    	    	Float XIH3 = null;			// 产品序列号 -- 型号3
    	    	Float XUH1 = null;			// 产品序列号 -- 序号1
    	    	Float XUH2 = null;			// 产品序列号 -- 序号2
    	    	String QDDM = "";			// 变频器驱动代码
    	    	Float SYDL = null;			// 电池剩余电量百分比
    	    	Float DDLBS = null;			// 电池低电量标示:1代表低电量，0代表高电量
    	    	String YXSJ_G = "";			// 运行时间（高字）
    	    	String YXSJ_D = "";			// 运行时间（低字）
    			
    			// 相关参数设置---------------------------------------------------------------------------------------------------------------------------------
    			SBBM = (String)recordList.get(i).get("jlmc");							// 设备编码：井号、泵编码、罐编码等
            	YBLX = yblxm + "";														// 仪表类型：参考sfl_yblx，暂时对应数字
            	
            	try {
            		// 现场使用的传感器
                	if ( yblxm == 1 || yblxm == 2 || yblxm == 3 || yblxm == 4 || yblxm == 5 || yblxm == 6 || yblxm == 14 || yblxm == 15 || yblxm == 16 ) {	

        				// 获得相关值
                    	YBDM = getFloatValue(SBBM, "she_bei_dai_ma_cgq" + yblxm) + "";			// 仪表代码，即RTU中设备代码。	实时库Key:she_bei_dai_ma_cgqX
                    	CJDM = getFloatValue(SBBM, "chang_jia_dai_ma_cgq" + yblxm) + "";		// 厂家代码，即RTU中厂家代码。	实时库Key:chang_jia_dai_ma_cgqX
                    	CC_N = getFloatValue(SBBM, "chu_chang_nian_cgq" + yblxm);				// 出厂年份							 chu_chang_nian_cgqX
                    	CC_Y = getFloatValue(SBBM, "chu_chang_yue_cgq" + yblxm);				// 出厂月份							 chu_chang_yue_cgqX
                    	CC_R = getFloatValue(SBBM, "chu_chang_ri_cgq" + yblxm);					// 出厂日							 chu_chang_ri_cgqX
                    	TJLC = getFloatValue(SBBM, "tiao_jiao_liang_cheng_cgq" + yblxm);		// 调教量程							 tiao_jiao_liang_cheng_cgqX
                    	XIH1 = getFloatValue(SBBM, "xing_hao1_cgq" + yblxm);					// 产品序列号 -- 型号1				 	 xing_hao1_cgqX
                    	XIH2 = getFloatValue(SBBM, "xing_hao2_cgq" + yblxm);					// 产品序列号 -- 型号2					 xing_hao2_cgqX
                    	XIH3 = getFloatValue(SBBM, "xing_hao3_cgq" + yblxm);					// 产品序列号 -- 型号3					 xing_hao3_cgqX
                    	XUH1 = getFloatValue(SBBM, "xu_hao1_cgq" + yblxm);						// 产品序列号 -- 序号1					 xu_hao1_cgqX
                    	XUH2 = getFloatValue(SBBM, "xu_hao2_cgq" + yblxm);						// 产品序列号 -- 序号2					 xu_hao2_cgqX
                    	SYDL = getFloatValue(SBBM, "cgq_remained_dianliang_cgq" + yblxm);		// 电池剩余电量百分比					 cgq_remained_dianliang_cgqX
                    	YXSJ_G = null;															// 运行时间（高字）					 cgq_rtu_time_cgqX
                    	YXSJ_D = getFloatValue(SBBM, "cgq_rtu_time_cgq" + yblxm) + "";			// 运行时间（低字）					  暂定写到低字
                    	
                    	String ddlbStr = realtimeDataService.getEndTagVarInfo(SBBM, dianChiBiaoShiArray[yblxm-1]);	// 电池低电量标示:1代表低电量，0代表高电量。实时库Key:详见数组
                    	if ( ddlbStr == null ) {
                    		DDLBS = null;
                    	} else {
                    		DDLBS = (float) (ddlbStr.equals("true")?1:0);
                    	}
                    	
                    	// 需要特殊处理的传感器参数(1.量程处理)
                    	if ( yblxm != 15 ) {													// 智能电表无量程
                        	ZDLC1 = getFloatValue(SBBM, "zui_da_liang_cheng1_cgq" + yblxm);		// 最大量程1，默认只有一个参数采集的，只采最大、最小量程。 实时库Key: zui_da_liang_cheng1_cgqX
                        	ZXLC1 = getFloatValue(SBBM, "zui_xiao_liang_cheng1_cgq" + yblxm);	// 最小量程1，默认只有一个参数采集的，只采最大、最小量程。 实时库Key: zui_xiao_liang_cheng1_cgqX
                        	XSWS1 = getFloatValue(SBBM, "xiao_shu_wei_shu1_cgq" + yblxm);		// 小数位数1，默认只有一个参数采集的，只采最大、最小量程。 实时库Key: xiao_shu_wei_shu1_cgqX
                        	// System.out.println("ZDLC1: " + ZDLC1 + "  , " + yblxm);
                        	
                        	if ( yblxm == 16 || yblxm == 13 ) {										// 温压一体、掺稀配水  存在第二组参数 (暂时进入不到13的分支，现场无此设备)
                            	ZDLC2 = getFloatValue(SBBM, "zui_da_liang_cheng2_cgq" + yblxm);		// 最大量程2，对于温、压一体的两个量程，掺稀的三个量程：流量、温度、压力。  实时库Key: zui_da_liang_cheng2_cgqX
                            	ZXLC2 = getFloatValue(SBBM, "zui_xiao_liang_cheng2_cgq" + yblxm);	// 最小量程2
                            	XSWS2 = getFloatValue(SBBM, "xiao_shu_wei_shu2_cgq" + yblxm);		// 小数位数2
                            	// System.out.println("ZDLC2: " + ZDLC2 + "  , " + yblxm);
                            	
                        		if ( yblxm == 13 ) { 													// 掺稀配水 存在第三组参数 
                                	ZDLC3 = getFloatValue(SBBM, "zui_da_liang_cheng3_cgq" + yblxm);		// 最大量程3，对于温、压一体的两个量程，掺稀的三个量程：流量、温度、压力。 实时库Key: zui_da_liang_cheng3_cgqX
                                	ZXLC3 = getFloatValue(SBBM, "zui_xiao_liang_cheng3_cgq" + yblxm);	// 最小量程3
                                	XSWS3 = getFloatValue(SBBM, "xiao_shu_wei_shu3_cgq" + yblxm);		// 小数位数3
                                	// System.out.println("ZDLC3: " + ZDLC3 + "  , " + yblxm);
                            	}
                        	} 
                        	
                    	} else {
                    		// doNothing
                    	}
                    	
                    	// 需要特殊处理的传感器参数(2.变频器驱动代码处理)
                    	if ( yblxm == 14 ) {	
                        	QDDM = realtimeDataService.getEndTagVarInfo(SBBM, "qu_dong_dai_ma_cgq" + yblxm);	// 变频器驱动代码。实时库Key:qu_dong_dai_ma_cgqX
                        	// System.out.println("QDDM: " + QDDM + "  , " + yblxm);
                    	}
        			
                	} else if ( yblxm == 18 ) {	// RTU
                		
                		YBDM = getFloatValue(SBBM, "rtu_she_bei_dai_ma") + "";		// 仪表代码，即RTU中设备代码。	实时库Key:rtu_she_bei_dai_ma
                    	CJDM = getFloatValue(SBBM, "rtu_chang_jia_dai_ma") + "";	// 厂家代码，即RTU中厂家代码。	实时库Key:rtu_chang_jia_dai_ma
                    	CC_N = getFloatValue(SBBM, "rtu_chu_chang_nian");			// 出厂年份							 rtu_chu_chang_nian
                    	CC_Y = getFloatValue(SBBM, "rtu_chu_chang_yue");			// 出厂月份							 rtu_chu_chang_yue
                    	CC_R = getFloatValue(SBBM, "rtu_chu_chang_ri");				// 出厂日							 rtu_chu_chang_ri
                    	TJLC = getFloatValue(SBBM, "rtu_tiao_jiao_liang_cheng");	// 调教量程							 rtu_tiao_jiao_liang_cheng
                    	XIH1 = getFloatValue(SBBM, "rtu_xing_hao1");				// 产品序列号 -- 型号1				 	 rtu_xing_hao1
                    	XIH2 = getFloatValue(SBBM, "rtu_xing_hao2");				// 产品序列号 -- 型号2					 rtu_xing_hao2
                    	XIH3 = getFloatValue(SBBM, "rtu_xing_hao3");				// 产品序列号 -- 型号3					 rtu_xing_hao3
                    	XUH1 = getFloatValue(SBBM, "rtu_xu_hao1");					// 产品序列号 -- 序号1					 rtu_xu_hao1
                    	XUH2 = getFloatValue(SBBM, "rtu_xu_hao2");					// 产品序列号 -- 序号2					 rtu_xu_hao2
                    	
                	} else {
                		// doNothing
                	}
                	
            	} catch (Exception e11) {
            		e11.printStackTrace();
            		continue;
            	}
            	
            	try  {
            		 query.addParameter("SBBM", SBBM) 					
        			.addParameter("YBLX", YBLX)
        			.addParameter("CJSJ", new Date())
        			.addParameter("YBDM", YBDM)
        			.addParameter("CJDM", CJDM)
        			.addParameter("ZDLC1", ZDLC1)
        			.addParameter("ZXLC1", ZXLC1)
        			.addParameter("XSWS1", XSWS1)
        			.addParameter("ZDLC2", ZDLC2)
        			.addParameter("ZXLC2", ZXLC2)
        			.addParameter("XSWS2", XSWS2)
        			.addParameter("ZDLC3", ZDLC3)
        			.addParameter("ZXLC3", ZXLC3)
        			.addParameter("XSWS3", XSWS3)
        			.addParameter("CC_N", CC_N)
        			.addParameter("CC_Y", CC_Y)
        			.addParameter("CC_R", CC_R)
        			.addParameter("TJLC", TJLC)
        			.addParameter("XIH1", XIH1)
        			.addParameter("XIH2", XIH2)
        			.addParameter("XIH3", XIH3)
        			.addParameter("XUH1", XUH1)
        			.addParameter("XUH2", XUH2)
        			.addParameter("QDDM", QDDM)
        			.addParameter("SYDL", SYDL)
        			.addParameter("DDLBS", DDLBS)
        			.addParameter("YXSJ_G", YXSJ_G)
        			.addParameter("YXSJ_D", YXSJ_D)
        			.addToBatch();
            		
            	} catch (Exception e1) {
            		e1.printStackTrace();
            	}
            	
    		} // endInnerFor....
    		
    		try {
    			query.executeBatch();		// 执行
    		} catch(Exception e1) {
    			e1.printStackTrace();
    		}
    		
    		
//    		System.out.println("油井： " + (String)recordList.get(i).get("jlmc") + " 相关设备信息插入完毕！");
    	} // endOuterFor...
    	
    	try{
    		con2.commit();				// 批量提交
    	} catch(Exception e1) {
			e1.printStackTrace();
		}
    	    	
    	System.out.println("设备信息记录插入完成！" );
    }
    
    /**
     * 获得从实时库查询的值
     * @param code		- 节点Code
     * @param varName	- 变量名
     * @return
     */
    public Float getFloatValue (String code, String varName) {
    	Float temp = null;
    	String tempStr = realtimeDataService.getEndTagVarInfo(code, varName);
    	//System.out.println("实际值为： " + tempStr);
    
    	if ( tempStr !=null && isNumeric(tempStr) ) {
    		temp = Float.parseFloat(tempStr);
    	}
    	
    	return temp;
    }
    
    /**
     * 	判断字符串是否是数字-用正则表达式方法
     * @param str
     * @return
     */
    public static boolean isNumeric(String str){ 
    	Pattern pattern = Pattern.compile("^\\d+$|^\\d+\\.\\d+$|-\\d+$");
    	Matcher isNum = pattern.matcher(str);
    	if (!isNum.matches()) {
    		return false;
    	}
    	return true;  
    } 
    
    
}
