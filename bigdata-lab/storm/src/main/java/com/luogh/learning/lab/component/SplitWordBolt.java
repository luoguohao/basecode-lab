package com.luogh.learning.lab.component;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.luogh.learning.lab.utils.LatecySimulator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by Kaola on 2015/8/20.
 */
public class SplitWordBolt extends BaseRichBolt{

    private static final Log LOG = LogFactory.getLog(SplitWordBolt.class);

    private OutputCollector collector ;
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("splittedWord"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String sentence = input.getStringByField("sentence");
        LOG.debug("current split tuple is :"+ sentence);
        
        // simulate some time-cost operations in this bolt, using LatecySimulator 
        new LatecySimulator(50,5).simulating();
        
        StringTokenizer sTokenizer = new StringTokenizer(sentence);
        while(sTokenizer.hasMoreTokens()) {
            this.collector.emit(input,new Values(sTokenizer.nextToken()));
        }

        this.collector.ack(input);

    }
}
