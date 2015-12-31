package com.zshuiquan.demo;

import java.util.ResourceBundle;
import org.apache.log4j.Logger;
import com.kugou.tianmu.handlers.Topology;

/**
 * 
 * @author josephzeng
 * 
 */
public class AnalysisClient {

	/**
	 * 获取 resources config.properties
	 */
	public static ResourceBundle resourceBundle = ResourceBundle.getBundle("config");
	
	/**
	 * 
	 */
	public static Logger log4j = Logger.getLogger(Topology.class.getClass());
	
	public static void main(String[] args) {
		Topology topology = new Topology(resourceBundle);
		topology.publishTopology();
	}

}
