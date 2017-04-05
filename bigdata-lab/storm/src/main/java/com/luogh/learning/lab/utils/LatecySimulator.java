package com.luogh.learning.lab.utils;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LatecySimulator {
		private static Log LOG = LogFactory.getLog(LatecySimulator.class);
        public  static final Integer MAX_LATECY_MIL_SECOND = 1000;
        public  static final Integer MIN_LATECY_MIL_SECOND = 0;
        public  static final Integer  EXTEME_LATECY_MIL_SECOND = 1000*6;

        private Integer maxLatecySecond;
        private Integer minLatecySencond;

        public LatecySimulator(){
            this.maxLatecySecond = MAX_LATECY_MIL_SECOND;
            this.minLatecySencond = MIN_LATECY_MIL_SECOND;
        }

        public LatecySimulator(Integer maxLatecySecond,Integer minLatecySencond) {
            if(maxLatecySecond < minLatecySencond || maxLatecySecond <0 || minLatecySencond <0){
                LOG.error("invaild latecySecond parameters! and the current maxLatecySecond is :"+maxLatecySecond +" and "
                            +"the current minLatecySecond is :"+minLatecySencond);
                throw new RuntimeException("invaild latecySecond parameters");
            }
            this.maxLatecySecond = maxLatecySecond;
            this.minLatecySencond = minLatecySencond;
        }


        public int simulating() {
            Random rand = new Random();
            int randNum = 0;
            do {
                randNum = rand.nextInt(this.maxLatecySecond);
            } while (randNum < this.minLatecySencond);
            
            //we also simulate the spike per 10 minute 
            double spike = Math.random();
            LOG.debug("current spike is :"+spike );
            
            if(spike >0.995||spike<0.03){
            	randNum = (int) (spike*EXTEME_LATECY_MIL_SECOND);
            }
            LOG.info("current latecy mil-second is :"+randNum);
            try {
                Thread.sleep(randNum);
            } catch(InterruptedException e){
                LOG.error(e.getMessage());
            }
            return randNum;
        }
        
        
        
        public static void main(String[] args) {
        	while(true) {
        		new LatecySimulator().simulating();
        	}
        	
		}
    }