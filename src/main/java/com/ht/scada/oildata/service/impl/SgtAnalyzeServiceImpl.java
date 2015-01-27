/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.util.VarSubTypeEnum;
import com.ht.scada.data.service.RealtimeDataService;
import com.ht.scada.oildata.webapp.entity.WellInfoWrapper;
import com.ht.scada.oildata.service.SgtAnalyzeService;
import com.ht.scada.oildata.util.String2FloatArrayUtil;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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

/**
 *
 * @author 赵磊 2014-12-18 21:09:12
 */
@Transactional
@Service("sgtAnalyzeService")
public class SgtAnalyzeServiceImpl implements SgtAnalyzeService {

    private static final Logger log = LoggerFactory.getLogger(SgtAnalyzeServiceImpl.class);
    @Inject
    protected Sql2o sql2o;
    @Autowired
    private RealtimeDataService realtimeDataService;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void sgtAnalyze() {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH) - 1);
        String sql = "SELECT code, datetime, chong_ci"
                + " FROM T_SGT_HISTORY  WHERE datetime>:TIME and chong_cheng>0 and chong_ci>0 and zui_xiao_zai_he>0 and jssj is null order by datetime";

        List<Map<String, Object>> sgtDataList = null;
        try (Connection con = sql2o.open()) {
            sgtDataList = con.createQuery(sql).addParameter("TIME", c.getTime()).executeAndFetchTable().asList();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (sgtDataList != null) {
            for (Map<String, Object> map : sgtDataList) {
                String code = (String) map.get("code");
                Date date = (Date) map.get("datetime");
                Float chongCi = Float.parseFloat(((BigDecimal) map.get("chong_ci")).toString());
                if (chongCi == 0) {
                    continue;
                }

                String sqlSgt = "select wei_yi_array, zai_he_array from T_SGT_HISTORY "
                        + "where code=:code and datetime=:date";
                List<Map<String, Object>> dataList = null;
                try (Connection con = sql2o.open()) {
                    dataList = con.createQuery(sqlSgt)
                            .addParameter("code", code)
                            .addParameter("date", date)
                            .executeAndFetchTable().asList();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (dataList == null) {
                    continue;
                }

                String weiyi = (String) dataList.get(0).get("wei_yi_array");
                String zaihe = (String) dataList.get(0).get("zai_he_array");
                if (weiyi == null || zaihe == null) {
                    continue;
                }

                String time = LocalDateTime.fromDateFields(date).toString("yyyy-MM-dd HH:mm:ss");
                try {
                    log.info("计算功图：" + code + " " + time);
                    SgtCalcService scs = new SgtCalcService();
                    scs.setRealtimeDataService(realtimeDataService);
                    scs.setSql2o(sql2o);
                    String powerStr = realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.GONG_LV_ARRAY.toString().toLowerCase());
                    String dlStr = realtimeDataService.getEndTagVarYcArray(code, VarSubTypeEnum.DIAN_LIU_ARRAY.toString().toLowerCase());
                    scs.handleData(code, sdf.parse(time), parseStringToFloatArray(weiyi), parseStringToFloatArray(zaihe), chongCi, powerStr, dlStr);
                } catch (Exception e) {
                    log.error(e.getMessage());
                    continue;
                }
            }
        }
    }

    private float[] parseStringToFloatArray(String str) {
        if (str == null || "".equals(str)) {
            return null;
        } else {
            String strs[] = str.split(",");
            float f[] = new float[strs.length];
            for (int i = 0; i < strs.length; i++) {
                f[i] = Float.valueOf(strs[i]);
            }
            return f;
        }
    }
}
