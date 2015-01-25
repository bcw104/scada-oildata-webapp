/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.ht.scada.oildata.dr;

import com.ht.scada.data.service.RealtimeDataService;
import java.util.List;
import java.util.Map;
import org.sql2o.Connection;

/**
 *  油井设备档案实时数据转储
 */
public class YouJingSbdazc implements Runnable {
    private Connection con2;
    private List<Map<String, Object>> recordList;
    private RealtimeDataService realtimeDataService;

    public YouJingSbdazc(Connection con2, List<Map<String, Object>> recordList, RealtimeDataService realtimeDataService) {
        this.con2 = con2;
        this.recordList = recordList;
        this.realtimeDataService = realtimeDataService;
    }

    @Override
    public void run() {
        System.out.println(con2.createQuery("select count(*) from qysczh.scy_ss_yj").executeScalar(Integer.class));
        System.out.println(realtimeDataService.getEndTagVarInfo("GD1-12-511", "i_c"));
    }
}
