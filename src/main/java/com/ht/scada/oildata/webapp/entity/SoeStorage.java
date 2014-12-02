package com.ht.scada.oildata.webapp.entity;

/**
 * SOE存儲器，不能与遥信变位存储器与故障存储器共存
 *
 * @author 赵磊
 */
public class SoeStorage {

    public String name;
    public boolean flag;	// 有效状态
    public String onInfo;	// 合消息
    public String offInfo;	// 分消息
    public boolean pushWnd;// 推画面
    public int[] startAdress;//值地址
    public int[] shresholdAdress;//门限地址

    SoeStorage(String name, boolean flag, String onInfo, String offInfo,
               boolean pushWnd, int[] startAdress, int[] shresholdAdress) {
        this.name = name;
        this.flag = flag;
        this.onInfo = onInfo;
        this.offInfo = offInfo;
        this.pushWnd = pushWnd;
        this.startAdress = startAdress;
        this.shresholdAdress = shresholdAdress;
    }
}
