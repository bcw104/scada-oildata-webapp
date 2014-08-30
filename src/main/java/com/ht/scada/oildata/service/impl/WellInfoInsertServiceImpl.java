/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ht.scada.oildata.service.impl;

import com.ht.scada.common.tag.util.CommonUtils;
import com.ht.scada.oildata.service.WellInfoInsertService;
import com.ht.scada.oildata.service.WellInfoService;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.joda.time.LocalDateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 *
 * @author 赵磊
 */
@Transactional
@Service("wellInfoInsertService")
public class WellInfoInsertServiceImpl implements WellInfoInsertService {

    @Autowired
    private WellInfoService wellInfoService;

    /**
     * 井基本数据录入任务
     *
     * @author 王蓬
     */
    @Override
    public void wellInfoSaveTask() {
        System.out.println("开始录入井信息：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));

        List<Map<String, Object>> allEndtagsCode = wellInfoService.findAllEndtagsCode();
        for (int i = 0; i < allEndtagsCode.size(); i++) {
            try {
                String endTagCode = (String) allEndtagsCode.get(i).get("code");
                Map<String, Object> basicInforOfthisEndtag = wellInfoService.findBasicCalculateInforsByCode(endTagCode);

                if (basicInforOfthisEndtag != null) {		// 不为空
                    float bengJing = Float.parseFloat(((BigDecimal) basicInforOfthisEndtag.get("bj")).toString());
                    float hanShui = Float.parseFloat(((BigDecimal) basicInforOfthisEndtag.get("hs")).toString());
                    float yymd = Float.parseFloat(((BigDecimal) basicInforOfthisEndtag.get("dmyymd")).toString());
                    Date lrqiDate = (Date) basicInforOfthisEndtag.get("rq");
                    // 此处调用写入函数
                    wellInfoService.addOneTWellInforRecord(
                            CommonUtils.getCode(),
                            endTagCode,
                            bengJing,
                            hanShui,
                            1.0f, // 水密度 默认1.0
                            yymd,
                            lrqiDate);
                }
            } catch (Exception e) {
            }
        }
        System.out.println("完成录入井信息：" + LocalDateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
    }
}
