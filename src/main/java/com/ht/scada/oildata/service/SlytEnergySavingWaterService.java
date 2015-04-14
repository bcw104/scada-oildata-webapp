package com.ht.scada.oildata.service;

/**
 * 胜利油田-孤岛采油厂 智能节电注水服务接口
 * @author PengWang 2015.4.7
 *
 */
public interface SlytEnergySavingWaterService {

	/**
	 * 智能节电配注
	 * （1）按阶段录入数据
	 * （2）按阶段自动调节注水
	 * @author PengWang 2015.4.8
	 */
	void intelligentFlowControl() ;
	
}
