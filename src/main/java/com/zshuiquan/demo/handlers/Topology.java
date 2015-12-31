package com.zshuiquan.demo.handlers;

import java.util.HashMap;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
/**
 * 
 * @author josephzeng
 *
 */
public class Topology {
	
	/**
	 * 
	 */
	private static ResourceBundle resourceBundle;
	
	/**
	 * 
	 */
	public static Logger log4j = Logger.getLogger(Topology.class.getClass());

	/**
	 * 
	 */
	private static HashMap<String, String> configHashMap = new HashMap<String, String>();
	

	/**
	 * 
	 * @param myResourceBundle
	 */
	public Topology(ResourceBundle myResourceBundle) {
		resourceBundle = myResourceBundle;
		String redisStr = resourceBundle.getString("redis_host");
		configHashMap.put("redis_host_default", redisStr.split(":")[0].trim());
		//...
		//set configHashMap
		//...
	}
	
	/**
	 * 
	 */
	public void publishTopology() {
		BrokerHosts brokerHosts = new ZkHosts(resourceBundle.getString("kafka_broker_zk_str"));
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, resourceBundle.getString("kafka_topic"), 
				resourceBundle.getString("kafka_zk_root"), resourceBundle.getString("kafka_id"));
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	    TopologyBuilder builder = new TopologyBuilder();
	    
	    Config conf = new Config();
	    conf.setDebug(true);
	    conf.setNumWorkers(Integer.parseInt(resourceBundle.getString("topology_num_workers")));
	    conf.setMaxSpoutPending(Integer.parseInt(resourceBundle.getString("topology_max_spout_pending")));
	    conf.setNumAckers(Integer.parseInt(resourceBundle.getString("topology_num_ackers")));

	    /**
	     * 
	     */
	    builder.setSpout(resourceBundle.getString("builder_kafka_spout_name"), new KafkaSpout(spoutConfig), 
	    		Integer.parseInt(resourceBundle.getString("builder_kafka_spout_num")));
	    /**
	     * bolt filter
	     */
	    builder.setBolt(resourceBundle.getString("builder_bolt_filter_name"), new FilterBolt(), 
	    		Integer.parseInt(resourceBundle.getString("builder_bolt_filter_num"))).shuffleGrouping(resourceBundle.getString("builder_kafka_spout_name"));
	    /**
	     * bolt 1
	     */
	    builder.setBolt(resourceBundle.getString("builder_bolt_blacklist_name"), 
	    		new BlackListBolt(configHashMap), 
	    		Integer.parseInt(resourceBundle.getString("builder_bolt_blacklist_ipnums"))).shuffleGrouping(resourceBundle.getString("builder_bolt_filter_name"));
	    /**
	     * bolt 2
	     */
	    builder.setBolt(resourceBundle.getString("builder_bolt_qps_name"), 
	    		new QpsBolt(configHashMap), 
	    		Integer.parseInt(resourceBundle.getString("builder_bolt_qps_nums"))).shuffleGrouping(resourceBundle.getString("builder_bolt_filter_name"));
		
		//...
		//更多的 bolt
		//...
	    try
	    {
	    	/**
	    	 * 提交确认
	    	 */
	    	StormSubmitter.submitTopology(Topology.class.getSimpleName(), conf, builder.createTopology());
	    } catch (AlreadyAliveException e) {
	    	log4j.error("产生AlreadyAliveException异常" + e.getMessage());
	    } catch (InvalidTopologyException e) {
	    	log4j.error("产生InvalidTopologyException异常" + e.getMessage());
	    }	
	}

}
