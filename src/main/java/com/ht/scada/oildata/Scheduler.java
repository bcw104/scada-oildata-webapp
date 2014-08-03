package com.ht.scada.oildata;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.common.tag.util.RedisKeysEnum;
import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.entity.WetkSGT;
import com.ht.scada.oildata.service.ReportService;
import com.ht.scada.oildata.service.WellService;
import com.ht.scada.oildata.service.WetkSGTService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 定时任务
 *
 * @author 赵磊
 */
@Component
public class Scheduler {

    //@Autowired
    //@Qualifier("scheduledService1")
    //private ScheduledService scheduledService;
    @Autowired
    private EndTagService endTagService;
    @Autowired
    private ReportService reportService;
    @Autowired
    private RealtimeDataService realtimeDataService;
    @Autowired
    private WellService wellService;
    @Autowired
    private WetkSGTService wetkSGTService;

    private Map<String, String> dateMap = new HashMap<String, String>();;

    @Scheduled(cron="* 0/10 * * * ? ")
    public void hourlyTask() {
        System.out.println("现在时刻：" + CommonUtils.date2String(new Date()));
        
        // 功图id(32位随机数)
        String gtId;
        float CC,CC1,ZDZH,ZXZH;
         
        List<EndTag> youJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if (youJingList != null && youJingList.size() > 0) {
            for (EndTag youJing : youJingList) {
                String code = youJing.getCode();
                // 1.判断功图时间是否更新
                String newDateTime = realtimeDataService.getEndTagVarYcArray(code, RedisKeysEnum.GT_DATETIME.toString());
                if (dateMap.get(code) != null && dateMap.get(code).equals(newDateTime)) {
                    return;
                }
                dateMap.put(code, newDateTime);
                // 2.判断是否有功图
                if (getDianYCData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase()) > 0) { // 有功图才写进行持久化
                    WetkSGT wetkSGT = new WetkSGT();
                    gtId = CommonUtils.getCode();
                    CC = getDianYCData(code, VarSubTypeEnum.CHONG_CHENG.toString().toLowerCase());
                    CC1 = getDianYCData(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase());
                    ZDZH = getDianYCData(code, VarSubTypeEnum.ZUI_DA_ZAI_HE.toString().toLowerCase());
                    ZXZH = getDianYCData(code, VarSubTypeEnum.ZUI_XIAO_ZAI_HE.toString().toLowerCase());
                    wetkSGT.setID(gtId);
                    wetkSGT.setJH(code);
                    wetkSGT.setCJSJ(CommonUtils.string2Date(newDateTime));
                    wetkSGT.setCC(CC); // 冲程
                    wetkSGT.setCC1(CC1); // 冲次
                    wetkSGT.setSXCC1(getDianYCData(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase())); //todo 上行冲次，暂时与冲次值相同
                    wetkSGT.setXXCC1(getDianYCData(code, VarSubTypeEnum.CHONG_CI.toString().toLowerCase())); //todo 下行冲次，暂时与冲次值相同
                    wetkSGT.setWY(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.WEI_YI_ARRAY.toString().toLowerCase()), 3));
                    wetkSGT.setZH(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.ZAI_HE_ARRAY.toString().toLowerCase()), 3));
                    wetkSGT.setGL(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.GONG_LV_ARRAY.toString().toLowerCase()), 3));
                    wetkSGT.setDL(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.DIAN_LIU_ARRAY.toString().toLowerCase()), 3));
                    wetkSGT.setBPQSCGL(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.GONG_LV_YIN_SHU_ARRAY.toString().toLowerCase()), 3));
                    wetkSGT.setZJ(CommonUtils.string2String(realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.DIAN_GONG_TU_ARRAY.toString().toLowerCase()), 3));
                    wetkSGT.setZDZH(ZDZH);// 最大载荷
                    wetkSGT.setZXZH(ZXZH); // 最小载荷
                    wetkSGT.setBZGT(null); // 暂时为空
                    wetkSGT.setGLYS(getDianYCData(code, VarSubTypeEnum.GV_GLYS.toString().toLowerCase()));
                    wetkSGT.setYGGL(getDianYCData(code, VarSubTypeEnum.GV_YG.toString().toLowerCase()));
                    wetkSGT.setWGGL(getDianYCData(code, VarSubTypeEnum.GV_WG.toString().toLowerCase()));

                    wetkSGTService.addOneRecord(wetkSGT); // 持久化

                    wetkSGTService.addOneGTFXRecord(gtId,code,CommonUtils.string2Date(newDateTime),CC,CC1,ZDZH,ZXZH); // 功图分析表持久化数据
                }
            }
        }
        System.out.println("现在时刻：" + CommonUtils.date2String(new Date()));
    }


    /**
    * 根据井号和变量名获取实时数据
    * @param code
    * @param varName
    * @return
    */
    private float getDianYCData(String code, String varName) {
        String value = realtimeDataService.getEndTagVarInfo(code, varName);
        if (value != null && !value.isEmpty()) {
            return  CommonUtils.string2Float(value, 4); // 保留四位小数
        } else {
            return 0;
        }
    }
}
