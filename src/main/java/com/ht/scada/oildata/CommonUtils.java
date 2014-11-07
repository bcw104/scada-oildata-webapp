package com.ht.scada.oildata;

import java.util.Calendar;
import java.util.Date;

public class CommonUtils {
     /*
     * 当前时间较 0点时间占一天比重
     */
    public static float timeProportion(Date date) {
        Calendar cal = Calendar.getInstance();		// 当前时间
        cal.setTime(date);
        return (float) (cal.get(Calendar.HOUR_OF_DAY) * 60 + cal.get(Calendar.MINUTE)) / (24 * 60);
    }

}
