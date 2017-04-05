package com.luogh.learning.lab.component;

import java.util.Map;
import java.util.UUID;

import com.luogh.learning.lab.utils.LatecySimulator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


/**
 * Created by Kaola on 2015/8/20.
 */
public class WordSpout extends BaseRichSpout {

    private static final Log LOG = LogFactory.getLog(WordSpout.class);

    private SpoutOutputCollector collector = null;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LOG.info("initial the spout");
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        LOG.debug("begin to emit new tuple");
        StringBuilder sentence = new StringBuilder();

        String messageId = UUID.randomUUID().toString();
        int randSeed = new LatecySimulator(20,2).simulating() % 5 ;

        switch(randSeed) {
            case 0 :
                sentence.append("this is a test");
                break;
            case 1 :
                sentence.append("we hope you are correct ");
                break;
            case 2 :
                sentence.append("You do not have permission to get URL from this com.luogh.learning.lab.server");
                break;
            case 3 :
                sentence.append("storm just as what you say");
                break;
            case 4 :
                sentence.append("hadoop you have a lot to learn");
                break;
        }
        LOG.debug("emit new tuple : "+sentence.toString());
        this.collector.emit(new Values(sentence.toString()),messageId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
    
    @Override
    public void ack(Object msgId) {
    	LOG.info("the msgId has been acked success."+ msgId.toString());
    }

    @Override
    public void fail(Object msgId) {
    	LOG.info("the msgId has been acked failed."+ msgId.toString());
    }
    

}
