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
import java.util.Date;
import java.util.List;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.sql2o.Sql2o;

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
        System.out.println("开始更新电表读数——现在时刻：" + CommonUtils.date2String(new Date()));
        List<EndTag> youJingList = endTagService.getByType(EndTagTypeEnum.YOU_JING.toString());
        if (youJingList != null && youJingList.size() > 0) {
            for (EndTag youJing : youJingList) {
                String code = youJing.getCode();
                String num = realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.DL_ZX_Z.toString().toLowerCase()) == null ? "0"
                        : realtimeDataService.getEndTagVarInfo(code, VarSubTypeEnum.DL_ZX_Z.toString().toLowerCase());
                realtimeDataService.putValue(code, RedisKeysEnum.RI_LINGSHI_DBDS.toString(), num);
            }
        }
        System.out.println("结束更新电表读数——现在时刻：" + CommonUtils.date2String(new Date()));
    }

}