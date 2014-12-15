/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.entity.EndTag;
import com.ht.scada.common.tag.service.EndTagService;
import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.common.tag.util.EndTagSubTypeEnum;
import com.ht.scada.common.tag.util.EndTagTypeEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.service.QkOilWellRecordService;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

/**
 *
 * @author 赵磊 2014-12-7
 */
@Transactional
@Service("qkOilWellRecordService")
public class QkOilWellRecordServiceImpl implements QkOilWellRecordService {

    private static final Logger log = LoggerFactory.getLogger(QkOilWellRecordServiceImpl.class);
    @Autowired
    private RealtimeDataService realtimeDataService;
    @Inject
    protected Sql2o sql2o;
    @Autowired
    private EndTagService endTagService;
    public List<EndTag> qiJingList;

    public Sql2o getSql2o() {
        return sql2o;
    }

    public void setSql2o(Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    @Override
    public void runRiBaoTask() {
        log.info("日报录入开始——现在时刻：" + CommonUtils.date2String(new Date()));
        Date date = new Date();

        qiJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if (qiJingList != null && qiJingList.size() > 0) {
            for (EndTag youJing : qiJingList) {
                try {
                    String code = youJing.getCode();
                    String sql = "Insert into T_RECORD_YJRB "
                            + "(ID,PJ_XGDY,PJ_XDY,PJ_XDL,PINLV,I_A,I_B,I_C,U_3XBPH_DIAN_YA,I_3XBPH_DIAN_LIU,U_A,U_B,U_C,U_AB,"
                            + "U_BC,U_CA,GV_YG,GV_WG,GV_SZ,GV_GLYS,DL_ZX_Z,KJ_TIME,HUI_YA,TAO_YA,BENG_YA,GUAN_WEN,GUAN_YA,CHONG_CHENG,"
                            + "CHONG_CI,ZUI_DA_ZAI_HE,ZUI_XIAO_ZAI_HE,WEN_DU,CODE,DATETIME) "
                            + "values (:ID,:PJ_XGDY,:PJ_XDY,:PJ_XDL,:PINLV,:I_A,:I_B,:I_C,:U_3XBPH_DIAN_YA,:I_3XBPH_DIAN_LIU,:U_A,:U_B,:U_C,:U_AB,"
                            + ":U_BC,:U_CA,:GV_YG,:GV_WG,:GV_SZ,:GV_GLYS,:DL_ZX_Z,:KJ_TIME,:HUI_YA,:TAO_YA,:BENG_YA,:GUAN_WEN,:GUAN_YA,:CHONG_CHENG,"
                            + ":CHONG_CI,:ZUI_DA_ZAI_HE,:ZUI_XIAO_ZAI_HE,:WEN_DU,:CODE,:DATETIME)";

                    Float PJ_XGDY = null, PJ_XDY = null, PJ_XDL = null, PINLV = null, I_A = null, I_B = null, I_C = null, U_3XBPH_DIAN_YA = null, I_3XBPH_DIAN_LIU = null, U_A = null, U_B = null, U_C = null, U_AB = null,
                            U_BC = null, U_CA = null, GV_YG = null, GV_WG = null, GV_SZ = null, GV_GLYS = null, DL_ZX_Z = null, KJ_TIME = null, HUI_YA = null, TAO_YA = null, BENG_YA = null, GUAN_WEN = null, GUAN_YA = null, CHONG_CHENG = null,
                            CHONG_CI = null, ZUI_DA_ZAI_HE = null, ZUI_XIAO_ZAI_HE = null, WEN_DU = null;

                    PJ_XGDY = getRealData(code, "PJ_XGDY".toLowerCase());
                    PJ_XDY = getRealData(code, "PJ_XDY".toLowerCase());
                    PJ_XDL = getRealData(code, "PJ_XDL".toLowerCase());
                    PINLV = getRealData(code, "PINLV".toLowerCase());
                    I_A = getRealData(code, "I_A".toLowerCase());
                    I_B = getRealData(code, "I_B".toLowerCase());
                    I_C = getRealData(code, "I_C".toLowerCase());
                    U_3XBPH_DIAN_YA = getRealData(code, "U_3XBPH_DIAN_YA".toLowerCase());
                    I_3XBPH_DIAN_LIU = getRealData(code, "I_3XBPH_DIAN_LIU".toLowerCase());
                    U_A = getRealData(code, "U_A".toLowerCase());
                    U_B = getRealData(code, "U_B".toLowerCase());
                    U_C = getRealData(code, "U_C".toLowerCase());
                    U_AB = getRealData(code, "U_AB".toLowerCase());
                    U_BC = getRealData(code, "U_BC".toLowerCase());
                    U_CA = getRealData(code, "U_CA".toLowerCase());
                    GV_YG = getRealData(code, "GV_YG".toLowerCase());
                    GV_WG = getRealData(code, "GV_WG".toLowerCase());
                    GV_SZ = getRealData(code, "GV_SZ".toLowerCase());
                    GV_GLYS = getRealData(code, "GV_GLYS".toLowerCase());
                    DL_ZX_Z = getRealData(code, "DL_ZX_Z".toLowerCase());
                    KJ_TIME = getRealData(code, "KJ_TIME".toLowerCase());

                    HUI_YA = getRealData(code, "HUI_YA".toLowerCase());
                    TAO_YA = getRealData(code, "TAO_YA".toLowerCase());
                    BENG_YA = getRealData(code, "BENG_YA".toLowerCase());

                    if (youJing.getSubType() != null && youJing.getSubType().equals(EndTagSubTypeEnum.GU_lI_JING.toString())) {
                        GUAN_WEN = getRealData(code, "GUAN_WEN".toLowerCase());
                        GUAN_YA = getRealData(code, "GUAN_YA".toLowerCase());
                    }

                    String extConfigInfo = youJing.getExtConfigInfo();
                    try {
                        if (extConfigInfo != null && !"".equals(extConfigInfo.trim())) {
                            String[] framesLine = extConfigInfo.trim().replaceAll("\\r", "").split("\\n");// 替换字符串									
                            for (String varName : framesLine) {
                                //yc|zsyl-注水压力|psj_z1-10-b|zky12_zsyl 
                                if (varName.contains("yc|")) {
                                    String varNames[] = varName.trim().split("\\|");
                                    String varName1 = varNames[1];
                                    String codeName = varNames[2];
                                    String varNameStr = varNames[3];
                                    if (varName1.contains("wen_du-")) { // 温度
                                        WEN_DU = getRealData(codeName, varNameStr);
                                    } else if (varName1.contains("hui_ya-")) {
                                        HUI_YA = getRealData(codeName, varNameStr);
                                    }
                                }
                            }
                        }

                    } catch (Exception e) {
                        log.info(code + ":" + e.toString());
                    }

//                    WEN_DU = getRealData(code, "WEN_DU".toLowerCase());

                    CHONG_CHENG = getRealData(code, "CHONG_CHENG".toLowerCase());
                    CHONG_CI = getRealData(code, "CHONG_CI".toLowerCase());
                    ZUI_DA_ZAI_HE = getRealData(code, "ZUI_DA_ZAI_HE".toLowerCase());
                    ZUI_XIAO_ZAI_HE = getRealData(code, "ZUI_XIAO_ZAI_HE".toLowerCase());

                    try (Connection con = sql2o.open()) {
                        con.createQuery(sql)
                                .addParameter("ID", UUID.randomUUID().toString().replace("-", ""))
                                .addParameter("CODE", code) //井号
                                .addParameter("DATETIME", date) //日期
                                .addParameter("PJ_XGDY", PJ_XGDY)//最小载荷
                                .addParameter("PJ_XDY", PJ_XDY)//最大载荷
                                .addParameter("PJ_XDL", PJ_XDL)//位移
                                .addParameter("PINLV", PINLV)//载荷
                                .addParameter("I_A", I_A)//平衡率
                                .addParameter("I_B", I_B)//电流平衡率
                                .addParameter("I_C", I_C)//耗电量
                                .addParameter("U_3XBPH_DIAN_YA", U_3XBPH_DIAN_YA)//产液量
                                .addParameter("I_3XBPH_DIAN_LIU", I_3XBPH_DIAN_LIU)//油量
                                .addParameter("U_A", U_A)//运行时间
                                .addParameter("U_B", U_B)//回压
                                .addParameter("U_C", U_C)//套压
                                .addParameter("U_AB", U_AB)//温度
                                .addParameter("U_BC", U_BC)//平均电流
                                .addParameter("U_CA", U_CA)//平均电压
                                .addParameter("GV_YG", GV_YG)//上行电流
                                .addParameter("GV_WG", GV_WG)//下行电流
                                .addParameter("GV_SZ", GV_SZ)//上行能耗
                                .addParameter("GV_GLYS", GV_GLYS)//下行能耗
                                .addParameter("DL_ZX_Z", DL_ZX_Z)//上行功率
                                .addParameter("KJ_TIME", KJ_TIME)//下行功率
                                .addParameter("HUI_YA", HUI_YA)//频率
                                .addParameter("TAO_YA", TAO_YA)//系数
                                .addParameter("BENG_YA", BENG_YA)//系数
                                .addParameter("GUAN_WEN", GUAN_WEN)//系数
                                .addParameter("GUAN_YA", GUAN_YA)//系数
                                .addParameter("CHONG_CHENG", CHONG_CHENG)//系数
                                .addParameter("CHONG_CI", CHONG_CI)//系数
                                .addParameter("ZUI_DA_ZAI_HE", ZUI_DA_ZAI_HE)//系数
                                .addParameter("ZUI_XIAO_ZAI_HE", ZUI_XIAO_ZAI_HE)//系数
                                .addParameter("WEN_DU", WEN_DU)//系数
                                .executeUpdate();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
                log.info(youJing.getCode() + "日报计算结束！");
            }
        }
        log.info("日报录入结束——现在时刻：" + CommonUtils.date2String(new Date()));
    }

    private Float getRealData(String code, String varName) {
        String value = realtimeDataService.getEndTagVarInfo(code, varName);
        if (value != null && !value.isEmpty()) {
            return CommonUtils.string2Float(value, 2); // 保留四位小数
        } else {
            return null;
        }
    }

    @Override
    public void runQjRiBaoTask() {
        log.info("气井日报录入开始——现在时刻：" + CommonUtils.date2String(new Date()));
        Date date = new Date();
        qiJingList = endTagService.getByType(EndTagTypeEnum.TIAN_RAN_QI_JING.toString());
        if (qiJingList != null && qiJingList.size() > 0) {
            for (EndTag youJing : qiJingList) {
                try {
                    String code = youJing.getCode();

                    String sql = "Insert into T_RECORD_QJRB "
                            + "(ID,YOU_YA,JRLWSYL,QJYL,JRLCKWD,TAO_YA,JRLJKJL,CODE,DATETIME) "
                            + "values (:ID,:YOU_YA,:JRLWSYL,:QJYL,:JRLCKWD,:TAO_YA,:JRLJKJL,:CODE,:DATETIME)";

                    Float YOU_YA = null, JRLWSYL = null, QJYL = null, JRLCKWD = null, TAO_YA = null, JRLJKJL = null;

                    YOU_YA = getRealData(code, "YOU_YA".toLowerCase());
                    JRLWSYL = getRealData(code, "JRLWSYL".toLowerCase());
                    QJYL = getRealData(code, "QJYL".toLowerCase());
                    JRLCKWD = getRealData(code, "JRLCKWD".toLowerCase());
                    TAO_YA = getRealData(code, "TAO_YA".toLowerCase());
                    JRLJKJL = getRealData(code, "JRLJKJL".toLowerCase());

                    String extConfigInfo = youJing.getExtConfigInfo();
                    try {
                        if (extConfigInfo != null && !"".equals(extConfigInfo.trim())) {
                            String[] framesLine = extConfigInfo.trim().replaceAll("\\r", "").split("\\n");// 替换字符串									
                            for (String varName : framesLine) {
                                //yc|zsyl-注水压力|psj_z1-10-b|zky12_zsyl 
                                if (varName.contains("yc|")) {
                                    String varNames[] = varName.trim().split("\\|");
                                    String varName1 = varNames[1];
                                    String codeName = varNames[2];
                                    String varNameStr = varNames[3];
                                    if (varName1.contains("you_ya-")) { //回压
                                        YOU_YA = getRealData(codeName, varNameStr);
                                    } else if (varName1.contains("tao_ya-")) {  //套压
                                        TAO_YA = getRealData(codeName, varNameStr);
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.info(code + ":" + e.toString());
                    }
                    try (Connection con = sql2o.open()) {
                        con.createQuery(sql)
                                .addParameter("ID", UUID.randomUUID().toString().replace("-", ""))
                                .addParameter("CODE", code) //井号
                                .addParameter("DATETIME", date) //日期
                                .addParameter("YOU_YA", YOU_YA)//最小载荷
                                .addParameter("JRLWSYL", JRLWSYL)//最大载荷
                                .addParameter("QJYL", QJYL)//位移
                                .addParameter("JRLCKWD", JRLCKWD)//载荷
                                .addParameter("TAO_YA", TAO_YA)//平衡率
                                .addParameter("JRLJKJL", JRLJKJL)//电流平衡率
                                .executeUpdate();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
                log.info(youJing.getCode() + "日报计算结束！");
            }
        }
        log.info("气井日报录入结束——现在时刻：" + CommonUtils.date2String(new Date()));
    }

    @Override
    public void runSjRiBaoTask() {
        log.info("水井日报录入开始——现在时刻：" + CommonUtils.date2String(new Date()));
        Date date = new Date();

        List<Map<String, Object>> list;
        try (Connection con = sql2o.open()) {
            list = con.createQuery("select * from T_SJ_GL").executeAndFetchTable().asList();
        }

        if (list != null) {
            for (Map<String, Object> map : list) {
                try {
                    String code = (String) map.get("code");
                    String realCode = (String) map.get("real_code");
                    String glCode = (String) map.get("gl_code");
                    String glVarZsyl = (String) map.get("gl_var_zsyl");

                    String sql = "Insert into T_RECORD_SJRB "
                            + "(ID,ZS_YL,CODE,FJ_CODE,DATETIME) "
                            + "values (:ID,:ZS_YL,:CODE,:FJ_CODE,:DATETIME)";

                    Float ZS_YL = getRealData(glCode, glVarZsyl);

                    try (Connection con = sql2o.open()) {
                        con.createQuery(sql)
                                .addParameter("ID", UUID.randomUUID().toString().replace("-", ""))
                                .addParameter("CODE", realCode) //井号
                                .addParameter("DATETIME", date) //日期
                                .addParameter("ZS_YL", ZS_YL)
                                .addParameter("FJ_CODE", glCode)
                                .executeUpdate();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } catch (Exception e) {
                    continue;
                }
            }
        }
        log.info("水井日报录入结束——现在时刻：" + CommonUtils.date2String(new Date()));
    }
}