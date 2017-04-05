package com.luogh.learning.lab.topology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.luogh.learning.lab.component.SplitWordBolt;
import com.luogh.learning.lab.component.WordSpout;
import com.luogh.learning.lab.component.WordStatisticBolt;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.logging.FileHandler;

/**
 * Created by Kaola on 2015/8/20.
 *
 *
 *
 * StormStart Topology definition stream graph is depict as follow:
 *  +------------+     +-----------------+     +---------------------+
 * | WordSpout | --> | SplitWordBolt | --> | WordStatisticBolt |
 * +------------+     +-----------------+     +---------------------+
 */
public class StormStartTopology {
    private static final Log LOG = LogFactory.getLog(StormStartTopology.class);


    public static TopologyBuilder buildTopology(){
        LOG.info("building the topology...");

        String spoutName = WordSpout.class.getSimpleName();
        String splitWordBolt = SplitWordBolt.class.getSimpleName();
        String wordStatisticBolt = WordStatisticBolt.class.getSimpleName();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(spoutName,new WordSpout(),1).setNumTasks(1);
        builder.setBolt(splitWordBolt,new SplitWordBolt(),1).setNumTasks(1).shuffleGrouping(spoutName);
        builder.setBolt(wordStatisticBolt,new WordStatisticBolt(),1).setNumTasks(1).fieldsGrouping(splitWordBolt,new Fields("splittedWord"));
        LOG.info("finish building the topology ");
        return builder;
    }


    public static void main(String[] args) throws Exception {
    	
	    TopologyBuilder builder = buildTopology();
	    
        Config stormConf = new Config();
        
        
    	Configuration extenConfig = new PropertiesConfiguration("config.properties");
        String key = null;
        Iterator<String> iter = extenConfig.getKeys();
        while(iter.hasNext()){
            key = iter.next().toString();

            LOG.info("the key value is :"+key +" the value is :"+extenConfig.getProperty(key));
            stormConf.put(key,extenConfig.getProperty(key));
        }

        String name = StormStartTopology.class.getSimpleName();

        // production use
        if (args != null && args.length > 0) {
        	
            name = args[0];
            stormConf.setMessageTimeoutSecs(80);
            stormConf.setMaxSpoutPending(400);
            stormConf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(name, stormConf, builder.createTopology());
            
        } else {
            // debug using local cluster
            stormConf.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, stormConf, builder.createTopology());
            int sleep = 60 * 60 * 1000;
            Thread.sleep(sleep);
            cluster.killTopology(name);
            cluster.shutdown();
        }


    }

}
