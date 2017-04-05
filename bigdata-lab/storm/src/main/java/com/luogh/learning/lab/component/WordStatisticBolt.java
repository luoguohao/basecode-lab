package com.luogh.learning.lab.component;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.luogh.learning.lab.utils.LatecySimulator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Kaola on 2015/8/20.
 */
public class WordStatisticBolt extends BaseRichBolt {

    private static final Log LOG = LogFactory.getLog(WordStatisticBolt.class);
    private OutputCollector collector;
    private Map<String,AtomicLong> wordStatMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        wordStatMap = new HashMap<String,AtomicLong>();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,60);  // use tick tuple to persist the map to disk
        return conf;

    }

    @Override
    public void execute(Tuple input) {

        if(checkIsTickTuple(input)){
            LOG.info("<#############################current statistic data as follows :\n"
                    +this.wordStatMap.toString()+
                    "\n#############################################>");
            this.wordStatMap.clear();
        } else {
        	
        	 // simulate some time-cost operations in this bolt, using LatecySimulator 
            new LatecySimulator(20,10).simulating();
            
            String word = input.getStringByField("splittedWord");
            LOG.debug("current tuple is :"+word);
            if(wordStatMap.containsKey(word)){
                wordStatMap.get(word).incrementAndGet();
            } else {
                wordStatMap.put(word,new AtomicLong(1L));
            }
        }

        this.collector.ack(input);

    }

    /**
     * check current tuple is a tick tuple which used to a singal
     * @param tuple
     * @return
     */
    private boolean checkIsTickTuple(Tuple tuple){
        return (Constants.SYSTEM_TICK_STREAM_ID.equals(tuple.getSourceStreamId()) && Constants.SYSTEM_COMPONENT_ID
                .equals(tuple.getSourceComponent())) ? true : false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}


