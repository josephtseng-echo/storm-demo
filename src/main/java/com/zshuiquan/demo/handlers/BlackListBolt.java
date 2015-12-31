package com.zshuiquan.demo.handlers;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.gson.Gson;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class BlackListBolt extends BaseRichBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 */
	private static Logger log4j = Logger.getLogger(BlackListBolt.class.getClass());
	
	/**
	 * 
	 */
	private  Jedis jedis = null;
	
	/**
	 * 
	 */
	private OutputCollector m_collector;
	
	/**
	 * 
	 */
	private HashMap<String, String> configHashMap  = null;
	
	/**
	 * 
	 */
	private int rediskeyExpire = 300;
	
	/**
	 * 
	 */
	private String redisAlarmQueueName = "test";
	
	/**
	 * 
	 * @param myResourceBundle
	 */
	public BlackListBolt(HashMap<String, String> myConfigHashMap) {
		this.configHashMap = myConfigHashMap;
		this.redisAlarmQueueName = this.configHashMap.get("redis_alarm_queue_name_base");
		this.rediskeyExpire = Integer.parseInt(this.configHashMap.get("redis_key_expire"));	
	}	

	@Override
	public void execute(Tuple input) {
		String line = input.getStringByField("line");
	    if ((line.isEmpty()) || (line.equals("{}")))
	    	return;
	    String[] fields = SplitUtilHandler.getStringSplitByStr(line, this.configHashMap.get("line_split_fields_str"));
	    //...
		//...
		try{
			//TODO
		}catch (Exception e){
			log4j.error(e.getMessage());
	    } finally {
	    	this.m_collector.ack(input);
	    }
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			/**/
		 	this.m_collector = collector;
		 	this.jedis = new Jedis(this.configHashMap.get("redis_host_default"), Integer.parseInt(this.configHashMap.get("redis_port_default")), 5000);		 	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public void cleanup(){
	  this.jedis.close();
	  this.jedis.disconnect();
	}
}
