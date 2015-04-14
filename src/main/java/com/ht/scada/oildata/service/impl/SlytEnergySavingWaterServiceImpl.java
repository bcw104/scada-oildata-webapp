package com.ht.scada.oildata.service.impl;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.service.TagCfgTplService;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.data.service.RTUService;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.Scheduler;
import com.ht.scada.oildata.service.SlytEnergySavingWaterService;

/**
 * 胜利油田-孤岛采油厂 智能节电注水服务接口实现类 2015.4.7
 * @author PengWang
 */
@Transactional
@Service("slytEnergySavingWaterService")
public class SlytEnergySavingWaterServiceImpl implements SlytEnergySavingWaterService {
	
    private static final Logger log = LoggerFactory.getLogger(SlytEnergySavingWaterServiceImpl.class);
    @Inject
    protected Sql2o sql2o;
    @Autowired
    private RealtimeDataService realtimeDataService;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    @Autowired
    private TagCfgTplService tagCfgTplService ;								// 变量模板服务对象
    @Autowired
    private RTUService rtuService;											// RTU操作服务
    @Autowired
    private EndTagService endTagService;									// 节点操作服务
    
	@Override
	public void intelligentFlowControl() {
		
		Date currentTime = new Date();										// 获得当前时间
		log.info("\r\n开始执行智能配注定时任务" + "-执行时间   "+ CommonUtils.date2String(currentTime) );
		
		List<EndTag> waterWells = endTagService.getByType("ZHU_SHUI_JING");	// 获得系统内所有水井
		List<Map<String, Object>> intervalInfoList = getIntervalInfor();	// 获得间隔信息集合（包含表的全部信息）
		
		int [] ids = new int[intervalInfoList.size()];						// id集合
		Date [] intervalBegins = new Date[intervalInfoList.size()];			// 间隔开始时间集合
		Date [] intervalEnds = new Date[intervalInfoList.size()];			// 间隔结束时间集合
		Float [] intervalLongs = new Float[intervalInfoList.size()];		// 间隔长度集合
		Float [] cofficients = new Float[intervalInfoList.size()];			// 间隔内水量系数集合
		ArrayList<WaterWellInfoYT> waterWellInfoYTList = new ArrayList<WaterWellInfoYT>();		// 本阶段调水集合
		
		if (intervalInfoList != null) {
			int i=0;
			for (Map<String, Object> map : intervalInfoList) {
				ids[i] = Integer.parseInt(((BigDecimal) map.get("id")).toString());
				intervalBegins[i] = (Date) map.get("intervalbegin");
				intervalEnds[i] = (Date) map.get("intervalend");
				intervalLongs[i] = Float.parseFloat(((BigDecimal) map.get("intervallong")).toString());
				cofficients[i] = Float.parseFloat(((BigDecimal) map.get("cofficient")).toString());
				i++;
			}
		}

		List<Map<String, Object>> wellBasicSetList = getBasicSetting();		// 获得水井基本配置信息
		for ( int i=0; i<intervalInfoList.size(); i++ ) {
		
			// System.out.println(ids[i] + ", " + intervalBegins[i] + ", " + intervalEnds[i] + ", " + intervalLongs[i] + ", " + cofficients[i]);
			if ( (intervalBegins[i].getHours() == currentTime.getHours() && intervalBegins[i].getMinutes() == currentTime.getMinutes()) ){
					// || intervalBegins[i].getHours() == 10 ) {	// 为了测试使用
				
				log.info( "时间段：" + intervalBegins[i].toString().split(" ")[1] + " - " + intervalEnds[i].toString().split(" ")[1] + ", 时段长 " + intervalLongs[i] + ", 标准系数 " + cofficients[i]);
				
				// (1) 数据写入
				if ( i == 0 ) {	// 要插入的为最后一个时段
					detailDataAdd( intervalBegins[intervalBegins.length-1], intervalEnds[intervalBegins.length-1], intervalLongs[intervalBegins.length-1], i);
				} else {		// 要插入的为非最后一个时段
					detailDataAdd( intervalBegins[i-1], intervalEnds[i-1], intervalLongs[i-1], i);
				}   
				
				// (2) 智能调节
				int num = 1;
				for ( int w=0; w<wellBasicSetList.size(); w++ ) {
					
					Map mapTemp = wellBasicSetList.get(w);				// 获得该行记录
					String code = (String) mapTemp.get("code");
					int used = Integer.parseInt( ((BigDecimal)mapTemp.get("used")).toString() );
					String model = (String) mapTemp.get("model");
					String detail = (String) mapTemp.get("detail");
					// System.out.println("当前查询的井Code为：" + code + ", 是否启用： " + used + ", 模式： " + model + ",  详情： " + detail );
					
					if ( used == 1 ) {	// 是否启用智能配注 1-启用，0-未启用
						
						// 算法： 时段内配注量 = 日配注量/24*系数
						//（1）获得配注量
						Float RPZL = null;															// 源点获得的日配注水量
	                    Map<String, Object> map = findDataFromYdkByCode(code);	 					// 源头库数据
	                    if ( map != null ) {
	                        RPZL = Float.parseFloat(((BigDecimal) map.get("rpzsl")).toString());
	                    }
						// Float RPZL = 100f;														// 源点获得的日配注水量(离线测试)
	                    
	                    // （2）获得水量系数
	                    Float xs = null;
	                    if ( model.equals("标准模式") ) {
	                    	xs = cofficients[i];
	                    } else {
	                    	if ( detail == null || detail.equals("") ) {
	                    		// System.out.println("请先配置 " + code + " 水井的个性配注方式...");
	                    		continue;
	                    	} else {
	                    		String xsArrayTemp = detail.substring(1, detail.length()-1).split(",")[i];	// 过滤掉前后的括号 [] 
	                    		xs = Float.parseFloat(xsArrayTemp);
	                    	}
	                    }
	                    Float traditionZSOneHour = CommonUtils.string2Float( RPZL/24 + "", 2 );				// 获得传统配注实际应该注水量，每小时(保留2位小数)
	                    Float realZSOneHour = CommonUtils.string2Float( RPZL/24*xs + "", 2 );				// 获得智能配注实际应该注水量，每小时(保留2位小数)（即新的配注设定值）
	                  
	                    // （3）执行配注操作
	                    String [] rtucodeAndVarName = getCodeAndVarName(waterWells, code);					// 首先要获得该井对应的RTU Code和VarName
	                    String valueStr = realtimeDataService.getEndTagVarInfo(rtucodeAndVarName[0], rtucodeAndVarName[1]);					// 获得未调节前流量设定值
	                    boolean flag = flowControl(rtucodeAndVarName[0], rtucodeAndVarName[1], rtucodeAndVarName[2], realZSOneHour, Float.parseFloat(valueStr));	// 调节
	                    
	                    if ( flag == false ) {		// 若调节失败，延时100ms再次执行一次(允许一口水井至多执行两次遥调)
	                    	try {
								Thread.sleep(100);
								flag = flowControl(rtucodeAndVarName[0], rtucodeAndVarName[1], rtucodeAndVarName[2], realZSOneHour, Float.parseFloat(valueStr));	// 调节
								num ++;
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
	                    }
						
	                    log.info( code + " 日配注：" + RPZL + ", 下阶段‘智能vs传统’配注： " + realZSOneHour + "vs" + traditionZSOneHour + " m³/h, " +
	                    		(model.equals("标准模式")?"标准模式":"个性模式") + ", 系数： " + xs + "    详情：" + rtucodeAndVarName[0] + "^" + rtucodeAndVarName[1] + ": " +
	                    		Float.parseFloat(valueStr) + " -> " + realZSOneHour + " m³/h  " + ( flag == true ? "调节成功": "调节失败") + "-" + num );
	                    
	                    // （4）获得配注前水井相关信息
	                    WaterWellInfoYT waterWellInfoYTCurrent = new WaterWellInfoYT();		// 构造调水临时变量
	                    waterWellInfoYTCurrent.setWellCode(code);							// 设置水井井号
	                    waterWellInfoYTCurrent.setRtuCode(rtucodeAndVarName[0]);			// 设置RTUCode
	                    waterWellInfoYTCurrent.setVarNameLlsdz(rtucodeAndVarName[1]);		// 设置调节变量名_流量设定值
	                    waterWellInfoYTCurrent.setVarNameZSYLZDLC(rtucodeAndVarName[2]);	// 设置调节变量名_注水压力最大量程
	                    waterWellInfoYTCurrent.setXs(xs);									// 设置本阶段系数
	                    waterWellInfoYTCurrent.setLlsdzBefore(Float.parseFloat(valueStr));	// 设置流量设定值（调节前）
	                    waterWellInfoYTCurrent.setLlsdzAfter(realZSOneHour);				// 设定流量设定值（调节后）
	                    float xishu =tagCfgTplService.getXiShuByCodeAndVarname(rtucodeAndVarName[0], rtucodeAndVarName[1]);	// 获取流量设定值系数
	                    waterWellInfoYTCurrent.setLlsdzxs(xishu);							// 设置流量设定值系数
	                    waterWellInfoYTList.add(waterWellInfoYTCurrent);					// 添加入集合
	                    
					} else {
						// doNothing
					}
					
				}	// endInner for
				
			} // end if
			
		}	// endOutter for
		log.info("本阶段智能配注调节执行完毕, 10分钟后进行本轮调节检查...");
		
		successfulChecking( waterWellInfoYTList );	// 本阶段配注成功性检验程序
		
	}
	
	/**
	 * 智能阶段配注成功性检验程序 2015.4.14
	 * @author PengWang 
	 * @param waterWellInfoYTList
	 */
	private void successfulChecking ( ArrayList<WaterWellInfoYT> waterWellInfoYTList ) {
		
		try {
			Thread.sleep(600000);								// 10min延迟启动时间
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		log.info("调节检查开始...");
		int successNum = 0;									// 初次调节成功水井井数
		for ( int q=0; q<waterWellInfoYTList.size(); q++ ) {
			WaterWellInfoYT temp = waterWellInfoYTList.get(q);
					
			String valueCurrentStr = realtimeDataService.getEndTagVarInfo( temp.getRtuCode(), temp.getVarNameLlsdz() );	// 当前获得的流量设定值
			Float valueCurrent = CommonUtils.string2Float( valueCurrentStr , 1 );			// 当前值
			Float valueSet = CommonUtils.string2Float( temp.getLlsdzAfter() + "", 1 );		// 设定值
					
			if ( valueCurrent.floatValue() == valueSet.floatValue() ) {						// 调节成功了
				successNum ++ ;
				log.info( temp.getWellCode() + "  +" + successNum + " 配注成功（自检）");
				continue;
			} else {																		// 初次调节失败
				log.info( temp.getWellCode() + " 配注失败（自检）!  当前值/应该值" + valueCurrent + "/" + valueSet );
				// 启动调节配注（再次调节）
				
				int value =  (int) ( valueSet / temp.getLlsdzxs() );
//				try {
//					boolean flag = rtuService.yt( temp.getRtuCode(), temp.getVarNameLlsdz(), value );		// 执行调节动作
//					
//					if ( flag == true ) {
//						log.info( temp.getWellCode() + "流量设定值自检调节成功-1");
//					} else {
//						Thread.sleep(500);			// 等待0.5s
//						boolean flag3 = rtuService.yt( temp.getRtuCode(), temp.getVarNameLlsdz(), value );	// 执行调节动作
//						log.info( temp.getRtuCode() + " " + temp.getVarNameLlsdz() + "  " + ( (flag3 == true)?"流量设定值自检调节成功-2":"流量设定值自检调节失败-2" ));
//					}
//					
//					Thread.sleep(500);				// 0.5s后执行量程调节
//					boolean flag1 = rtuService.yt( temp.getRtuCode(), temp.getVarNameZSYLZDLC(), 160 );		// 调用遥调方法
//					if ( flag1 == false ) {
//						Thread.sleep(500);			// 等待0.5s
//						boolean flag2 =  rtuService.yt( temp.getRtuCode(), temp.getVarNameZSYLZDLC(), 160 );// 重置失败再次执行一次
//						log.info( temp.getRtuCode() + " " + temp.getVarNameZSYLZDLC() + "  " + ( (flag2 == true)?"量程设定值重置成功-2":"量程设定值重置失败-2" ));
//					} else {
//						log.info( temp.getRtuCode() + " " + temp.getVarNameZSYLZDLC() + "  " + "量程设定值重置成功-1");
//					}
//					
//					Thread.sleep(500);				// 0.5s后执行下以后井调节
//					
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			
			}
			// temp.infoPrint();
		}
		log.info("调节检查结束，初次调节成功 " + successNum + "/" + waterWellInfoYTList.size() + " 口水井, 未成功井请查看详情日志...\r\n");
	}
	
	/**
	 * 获得当前水井节点的RTU节点和遥调变量名
	 * @author PengWang 2015.4.8
	 * @param waterWells
	 * @param code
	 * @return 0-RTUCode, 1-注水流量设定值, 2-注水压力最大量程
	 */
	private String[] getCodeAndVarName(List<EndTag> waterWells, String code) {

		String[] result = new String[3];
		for (int k = 0; k < waterWells.size(); k++) {
			String wellCodeTemp = waterWells.get(k).getCode(); 												// 水井井号
			String extInfoTemp = waterWells.get(k).getExtConfigInfo(); 										// 扩展信息

			if (wellCodeTemp.equals(code)) { 																// 是当前处理的水井
				String[] framesLine = extInfoTemp.replaceAll("\\r", "").replaceAll(" ", "").split("\\n");	// 替换字符串

				String parentCode = framesLine[0].split("\\|")[2]; 											// 父节点Code
				String sensorName = framesLine[0].split("\\|")[3];										 	// 传感器字符串
																											// 示例：zky6_ssll
				String regEx = "[^0-9]"; 																	// 正则表达式，提取其中定数字
				Pattern p = Pattern.compile(regEx);
				Matcher m = p.matcher(sensorName);
				String addressNum = m.replaceAll("").trim();

				String varName = "zky" + addressNum + "yt_zsllsdz"; 	// zky1yt_zsllsdz
				String varNameForLiangcheng = "zky" + addressNum + "yt_zsylzdlc"; //zky1yt_zsylzdlc
				// System.out.println(wellCodeTemp + " RTU父节点为： " + parentCode + " 变量为：" + varName);
				
				result[0] = parentCode;
				result[1] = varName;
				result[2] = varNameForLiangcheng;
			}
		}
		return result;
	}
	
	/**
	 * 流量控制函数
	 * @author PengWang 2015.4.8
	 * @param code
	 * @param varName
	 * @param ytVal	   - 新值
	 * @param ytValOld - 旧值
	 * @return true -遥调成功 false-遥调失败
	 */
	private boolean flowControl(String code, String varName, String varNameForLiangcheng, float ytVal, float ytValOld) {

		boolean success = false;												// 初始化未成功
		float xishu =tagCfgTplService.getXiShuByCodeAndVarname(code, varName);
		int ytVal1 = (int) ( ytVal / xishu);									// 获得遥调实际值（扩大了的整数）

		try {
			boolean flag = rtuService.yt(code, varName, ytVal1 );			 	// 调用遥调方法
			// System.out.println("节点： " + code + ", 变量: " + varName );
			
			Thread.sleep(1000);
			log.info( code  + "将注水压力量程回归 160" );
			boolean flag1 = rtuService.yt(code, varNameForLiangcheng, 160 );	// 调用遥调方法
			if ( flag1 == false ) {
				log.info( code + " " + varNameForLiangcheng + "  " + "重置失败");
				boolean flag2 = rtuService.yt(code, varNameForLiangcheng, 160 );				// 重置失败再次执行一次
				log.info( code + " " + varNameForLiangcheng + "  " + ( (flag2 == true)?"再次重置成功":"再次重置失败" ));
			} else {
				log.info( code + " " + varNameForLiangcheng + "  " + "重置成功");
			}
			
			Thread.sleep(1000);
			log.info( code + " 正在执行配注 等待1s调节下一口...");
			
			if (flag) {															// 遥调成功
				// System.out.println( "	水井智能调参： " + code + " 成功!  流量 " + (int)(ytValOld /xishu) + " --> " + ytVal + " m³/h" );
				success = true;
			} else {															// 遥调失败
				//System.out.println( "	水井智能调参： " + code + " 失败! ");
				success = false;
			}
			// 此处可以补充调参日志记录至后台
			
		} catch (Exception e) {
			System.out.println( "采集程序异常! " );
			e.printStackTrace();
		}
		// System.out.println( "水井智能调参： " + code + " " + varName + " 流量 " + (int)(ytValOld /xishu) + " --> " + ytVal1 + " m³/h" );
		return success;
	}

	
	/**
	 * 获得电价设置基本间隔信息 2015.4.7
	 * @author PengWang
	 * @return
	 */
	private List<Map<String, Object>> getIntervalInfor() {
		// id, intervalbegin, intervalend, intervallong, cofficient, property, energy_cofficient, energy_price, standardprice
		String sql = "SELECT * FROM T_ENERGYSAVING_WATER_PRICE";

        List<Map<String, Object>> intervalInfoList = null;
        try (Connection con = sql2o.open()) {
            intervalInfoList = con.createQuery(sql).executeAndFetchTable().asList();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return intervalInfoList;
	}
	
	/**
	 * 获得水井智能配注基本配置信息 2015.4.8
	 * @author PengWang
	 * @return
	 */
	private List<Map<String, Object>> getBasicSetting() {
		// id, code, used, model, detail
		String sql = "SELECT * FROM T_ENERGYSAVING_WATER_SET";
		
        List<Map<String, Object>> wellBasicSetList = null;
        try (Connection con = sql2o.open()) {
            wellBasicSetList = con.createQuery(sql).executeAndFetchTable().asList();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return wellBasicSetList;
	}
    
	
	 /**
     * 从源头库查询数据 泵径、含水、泵深、气油比、地面原油密度、地层水密度、动液面、天然气相对密度
     *
     * @param code
     * @return
     */
    private Map<String, Object> findDataFromYdkByCode(String code) {
        String sql = "SELECT * FROM (SELECT rpzsl from ys_dba02@ydk where jh=:CODE ORDER BY RQ DESC ) WHERE rownum <= 1";

        List<Map<String, Object>> list;
        try (Connection con = sql2o.open()) {
            list = con.createQuery(sql)
                    .addParameter("CODE", code)
                    .executeAndFetchTable().asList();
        }
        if (list != null && !list.isEmpty()) {
            return list.get(0);
        }
        return null;
    }

    /**
     * 详情数据添加
     * @author PengWang 2015.4.9
     * @param intervalBegin
     * @param intervalEnd
     * @param intervalLong
     * @param intervalIndex - 时段索引，为1时，需要插入上一个时段，昨天
     */
	public void detailDataAdd(Date intervalBegin, Date intervalEnd,	float intervalLong, int intervalIndex) {
		
		// 将日期转化为当天
		Date today = new Date();				// 今天
		today.setHours(0);
		today.setMinutes(0);
		today.setSeconds(0);
		
		Calendar cal = Calendar.getInstance();	// 昨天
	    cal.add(Calendar.DATE, -1);
	    Date yesterday = cal.getTime();
	    yesterday.setHours(0);
	    yesterday.setMinutes(0);
	    yesterday.setSeconds(0);
	    
		if ( intervalIndex == 1 ) {				// 需要向前处理
		    intervalBegin.setYear(yesterday.getYear());
			intervalBegin.setMonth(yesterday.getMonth());
			intervalBegin.setDate(yesterday.getDate());
			// System.out.println("昨天=昨天");
		} else {
			intervalBegin.setYear(today.getYear());
			intervalBegin.setMonth(today.getMonth());
			intervalBegin.setDate(today.getDate());
			yesterday = today;
			// System.out.println("昨天=今天");
		}
		intervalEnd.setYear(today.getYear());
		intervalEnd.setMonth(today.getMonth());
		intervalEnd.setDate(today.getDate());

		log.info("	水井智能配注 阶段数据录入开始: " + CommonUtils.date2String(new Date()));
		if (Scheduler.shuiJingList != null && Scheduler.shuiJingList.size() > 0) {
			
			int exceptionNum = 0;	// 捕捉到的异常数目，允许出现一次异常
			//for (EndTag shuiJing : Scheduler.shuiJingList) {
			for ( int w=0; w< Scheduler.shuiJingList.size(); w++ ) {	// 后加
				EndTag shuiJing = Scheduler.shuiJingList.get(w);		// 后加
				try {
					String insertSql = "insert into T_ENERGYSAVING_WATER_DETAIL "
							+ "(code, date_time_begin, date_time_end, sdc, zsl, ljzsl, gy, yy, ssll, rpzl, ljll) "
							+ "values (:code, :date_time_begin, :date_time_end, :sdc, :zsl, :ljzsl, :gy, :yy, :ssll, :rpzl, :ljll)";

					String code = shuiJing.getCode(); 						// 水井井号
					Float rpzl = 0f; 										// 日配注量（源点）， 每次执行时获取
					Float gy = 0f; 											// 干线压力，末端瞬时
					Float yy = 0f; 											// 油压，末端瞬时
					Float ssll = 0f; 										// 瞬时流量， 末端瞬时
					Float ljll = 0f; 										// 累计流量，末端瞬时
					Float zsl = 0f; 										// 阶段内注水量，
					Float ljzsl = 0f; 										// 日累计注水量，每日7:00阶段开始

					// 获取日配注量
					Map<String, Object> map = findDataFromYdkByCode(code); // 源头库数据
					if (map != null) {
						rpzl = Float.parseFloat(((BigDecimal) map.get("rpzsl")).toString());
					}

					// 获取油压、累计流量、瞬时流量、干线压力
					try {
						String extConfigInfo = shuiJing.getExtConfigInfo(); 								// 获得扩展信息
						if ( extConfigInfo != null && !"".equals(extConfigInfo.trim()) ) {
							String[] framesLine = extConfigInfo.trim().replaceAll("\\r", "").split("\\n");	// 替换字符串
							for (String varName : framesLine) {							// yc|zsyl-注水压力|psj_z1-10-b|zky12_zsyl
								if (varName.contains("yx|")	|| varName.contains("yc|")) {
									String varNames[] = varName.trim().split("\\|");
									String varName1 = varNames[1];
									String codeName = varNames[2];
									String varNameStr = varNames[3];
									if (varName1.contains("zsyl-")) { 										// 注水压力
										String zsylValue = realtimeDataService.getEndTagVarInfo(codeName,varNameStr);
										if (zsylValue != null) {
											yy = CommonUtils.formatFloat(Float.parseFloat(zsylValue),2);
										}
									} else if (varName1.contains("ljll")) { 								// 累计流量
										String ljllValue = realtimeDataService.getEndTagVarInfo(codeName,varNameStr);
										if (ljllValue != null) {
											ljll = CommonUtils.formatFloat(Float.parseFloat(ljllValue),2);
										}
									} else if (varName1.contains("shll")) { 								// 瞬时流量
										String ssllValue = realtimeDataService.getEndTagVarInfo(codeName,varNameStr);
										if (ssllValue != null) {
											ssll = CommonUtils.formatFloat(	Float.parseFloat(ssllValue),2);
										}
									} else if (varName1.contains("gxyl")) { 								// 干线压力
										String gxylValue1 = realtimeDataService.getEndTagVarInfo(codeName,varNameStr);
										if (gxylValue1 != null) {
											gy = CommonUtils.formatFloat(Float.parseFloat(gxylValue1), 2); 	// 在单井上配置了干线压力帧
										}
									}
									
									if (gy == 0) {															// 若未在单井上配置干线压力帧（或干线压力为0），找配置RTU节点的干线压力帧
										String extConfigInfo1 = endTagService.getByCode(codeName).getExtConfigInfo();
										if (extConfigInfo1 != null && !"".equals(extConfigInfo1	.trim())) {
											String[] framesLine1 = extConfigInfo1.trim().replaceAll("\\r", "").split("\\n");// 替换字符串
											for (String varName3 : framesLine1) {							// yc|zsyl-注水压力|psj_z1-10-b|zky12_zsyl
												if (varName3.contains("yx|")|| varName3	.contains("yc|")) {
													String varNames1[] = varName3.trim().split("\\|");
													String varName2 = varNames1[1];
													String codeName1 = varNames1[2];
													String varNameStr1 = varNames1[3];
													if (varName2.contains("gxyl")) { 						// 注水压力
														String gxylValue = realtimeDataService.getEndTagVarInfo(codeName1,varNameStr1);
														if (gxylValue != null) {
															gy = CommonUtils.formatFloat(Float.parseFloat(gxylValue),2);
														}
													}
												}
											}
										}
									}
									
								}
							} // end For
						}
					} catch (Exception e) {
						log.info(code + ": 实时库值获取异常 " + e.toString());
						
						// 实时库捕捉到异常时，回退，重新处理该井
						exceptionNum ++;
						if ( exceptionNum < 2 ) {	// 异常小于2此，重新处理该井
							w-- ;
						} else {					// 略过该井处理其他井
							// doNothing
							exceptionNum = 0; 		// 归零，重新计数
						}
						continue;
					}

					Float frontLJLL = getFrontLJLL(code);							// 上一阶段开始时的累计流量
					zsl = (ljll - frontLJLL) < 0 ? 0f : (ljll - frontLJLL) ;		// 获得“阶段配注量”
					ljzsl = getTodayLJZSL(code, yesterday) + zsl;					// 获得“当日累计注水量” 从7：00 开始
					// System.out.println(code + " 上一阶段累计流量为： " + frontLJLL + "  当前累计流量为： " + ljll + ", 阶段配注： " + zsl + ", 当天累计流量为： " + ljzsl);

					try (Connection con = sql2o.open()) {
						con.createQuery(insertSql) 
								.addParameter("code", code) 						// 井号
								.addParameter("date_time_begin", intervalBegin)		// 时段开始时间
								.addParameter("date_time_end", intervalEnd)			// 时段结束时间
								.addParameter("sdc", intervalLong)					// 时段长度，小时
								.addParameter("rpzl", rpzl) 						// 日配注量
								.addParameter("zsl", zsl)							// 阶段内注水量
								.addParameter("ljzsl", ljzsl)						// 当日累积注水量
								.addParameter("gy", gy)								// 干压
								.addParameter("yy", yy)								// 油压（注水压力）
								.addParameter("ssll", ssll) 						// 瞬时流量
								.addParameter("ljll", ljll)							// 累计流量
								.executeUpdate();
					} catch (Exception e) {
						log.info("处理水井：" + code + "出现异常！" + e.toString());
						continue;
					}
					
				} catch (Exception e) {
					log.error(e.toString());
					continue;
				}
			}
			log.info("	水井智能配注 阶段数据录入结束：" + CommonUtils.date2String(new Date()));
		}
	}
	
	/**
	 * 获得上一时间段开始时该井的累计流量
	 * @author PengWang 2015.4.9
	 * @param code - 井号
	 * @return 水井累计流量值
	 */
	private float getFrontLJLL(String code) {
		float ljll = 0f;
		String sql = "SELECT * from T_ENERGYSAVING_WATER_DETAIL where date_time_begin  = (select max(date_time_begin) from t_energysaving_water_detail where code = :CODE  ) AND code = :CODE";

		List<Map<String, Object>> list;
		try (Connection con = sql2o.open()) {
			list = con.createQuery(sql).addParameter("CODE", code).executeAndFetchTable().asList();
		}
		if (list != null && !list.isEmpty()) {
			ljll = list.get(0).get("ljll") == null ? 0f : Float.parseFloat(((BigDecimal) list.get(0).get("ljll")).toString());
		}
		return ljll;
	}

	/**
	 * 获得该井当天（从7:00 开始 的注水流量）
	 * @author PengWang 2015.4.9
	 * @param code
	 * @param today
	 * @return
	 */
	private float getTodayLJZSL( String code , Date today ) {
		float ljzsl = 0f;
		String sql = " SELECT sum(zsl) as ljzslsum from T_ENERGYSAVING_WATER_DETAIL where code= :code and date_time_begin > :date_time_begin";
		
		List<Map<String, Object>> list;
		try (Connection con = sql2o.open()) {
			list = con.createQuery(sql)
					.addParameter("code", code)
					.addParameter("date_time_begin", today)
					.executeAndFetchTable().asList();
		}
		if (list != null && !list.isEmpty()) {
			ljzsl = list.get(0).get("ljzslsum") == null ? 0f : Float.parseFloat(((BigDecimal) list.get(0).get("ljzslsum")).toString());
		}
		return ljzsl;
	}
	
	/**
	 * 水井遥调内部信息类 2015.4.13
	 * @author PengWang 
	 */
	private class WaterWellInfoYT {
		
		private String wellCode;		// 水井井号
		private String rtuCode;			// rtu节点号
		private String varNameLlsdz;	// 变量名_流量设定值
		private String varNameZSYLZDLC;	// 变量名_注水压力最大量程（暂时未启用）
		
		private Float llsdzBefore;		// 调节前流量设定值
		private Float llsdzAfter;		// 调节后流量设定值
		private Float llsdzxs;			// 流量设定值系数
		private Float xs;				// 本阶段注水调节系数
		
		public void infoPrint() {
			System.out.println("当前井: " + this.wellCode + ", rtuCode: " + this.rtuCode + ", 流量设定值变量: " + this.varNameLlsdz + ", 注水压力最大量程变量: " + this.varNameZSYLZDLC
					+ ", 调节前: " + this.llsdzBefore + ", 调节后: " + this.llsdzAfter + ", 变量系数: " + this.llsdzxs + ", 注水调节系数: " + this.xs);
		}
		
		public Float getLlsdzxs() {
			return llsdzxs;
		}
		public void setLlsdzxs(Float llsdzxs) {
			this.llsdzxs = llsdzxs;
		}
		public String getWellCode() {
			return wellCode;
		}
		public void setWellCode(String wellCode) {
			this.wellCode = wellCode;
		}
		public String getRtuCode() {
			return rtuCode;
		}
		public void setRtuCode(String rtuCode) {
			this.rtuCode = rtuCode;
		}
		public String getVarNameLlsdz() {
			return varNameLlsdz;
		}
		public void setVarNameLlsdz(String varNameLlsdz) {
			this.varNameLlsdz = varNameLlsdz;
		}
		public String getVarNameZSYLZDLC() {
			return varNameZSYLZDLC;
		}
		public void setVarNameZSYLZDLC(String varNameZSYLZDLC) {
			this.varNameZSYLZDLC = varNameZSYLZDLC;
		}
		public Float getLlsdzBefore() {
			return llsdzBefore;
		}
		public void setLlsdzBefore(Float llsdzBefore) {
			this.llsdzBefore = llsdzBefore;
		}
		public Float getLlsdzAfter() {
			return llsdzAfter;
		}
		public void setLlsdzAfter(Float llsdzAfter) {
			this.llsdzAfter = llsdzAfter;
		}
		public Float getXs() {
			return xs;
		}
		public void setXs(Float xs) {
			this.xs = xs;
		}

	}
    
}
