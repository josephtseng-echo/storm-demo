package com.zshuiquan.demo.handlers;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FilterBolt extends BaseRichBolt{
	
	/**
	 * 
	 */
	private OutputCollector m_collector;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * 
	 */
	public static Logger log4j = Logger.getLogger(FilterBolt.class.getClass());
	
	/**
	 * 
	 * @param myResourceBundle
	 */
	public FilterBolt() {
		/**/
	}

	@Override
	public void execute(Tuple input) {
		String line = input.getString(0);
	    if ((line.isEmpty()) || (line.equals("{}")))
	      return;
	    try{
	    	this.m_collector.emit(input, new Values(line));
	    }catch (Exception e){
	    	log4j.error(e.getMessage());
	    }finally {
	    	this.m_collector.ack(input);
	    }
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
	    this.m_collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));		
	}

}
