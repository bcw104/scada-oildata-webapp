/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.ht.scada.oildata.service.impl;

import com.ht.scada.oildata.service.ScdtService;
import java.util.Date;
import javax.inject.Inject;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

/**
 *
 * @author 赵磊
 * 2014-8-14
 * 23:49:22
 */
@Transactional
@Service("scdtService")
public class ScdtServiceImpl implements ScdtService {
    @Inject
    protected Sql2o sql2o;

    public Sql2o getSql2o() {
        return sql2o;
    }

    public void setSql2o(Sql2o sql2o) {
        this.sql2o = sql2o;
    }
    
    public void insertIntoYjrbRecord(String id, String code, float bengJing,
            float hanShui, float SMD, float YYMD, Date lrqi) {
        String sql = "Insert into F_SCDT_YJRB "
                + "(ID, code, BENG_JING, HAN_SHUI, SMD, YYMD, LRSJ) "
                + "values (:ID, :CODE, :BENG_JING, :HAN_SHUI, :SMD, :YYMD, :LRSY)";

        try (Connection con = sql2o.open()) {  			//
            con.createQuery(sql) //
                    .addParameter("ID", id) //
                    .addParameter("CODE", code) //
                    .addParameter("BENG_JING", bengJing)//
                    .addParameter("HAN_SHUI", hanShui) //
                    .addParameter("SMD", SMD) //
                    .addParameter("YYMD", YYMD) //
                    .addParameter("LRSY", lrqi) //
                    .executeUpdate();//
        } catch (Exception e) {
            System.out.println("e:" + e.getMessage());//
        }

    }

}
