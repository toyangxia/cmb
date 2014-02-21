package com.comcast.cqs.controller;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.Row;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.persistence.CassandraPersistence;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.model.CQSAPIStats;


public class CQSActiveActiveController {
    private static Logger logger = Logger.getLogger(CQSActiveActiveController.class);
    
    private static CQSActiveActiveController instance = new CQSActiveActiveController ();
    //thread pool for send poll message ID request
    public ExecutorService executor;
	
   
	//thread for check overall switch
    private CheckActiveActiveSwitchThread switchThread;
    private boolean switchFlag = false;
    
    //thread for check other data center url
    private CheckOtherDcURLThread dcThread;
    private Map <String, String> otherDcURLMap = null;
    private List <String> otherDcURLs = null;
    
    public static CQSActiveActiveController getInstance() {
    	return instance;
    }
    
  
    public void init() {
    	executor = Executors.newFixedThreadPool(CMBProperties.getInstance().getActiveActivePoolSize());
    	switchThread = new CheckActiveActiveSwitchThread();
    	switchThread.start();
    	dcThread = new CheckOtherDcURLThread ();
    	dcThread.start();
    }
    
   
    private class CheckActiveActiveSwitchThread extends Thread {
    	 public void run() {
    	while(true){
    		try{		
    			//set flag
    			getActiveActiveSwitchRealTime ();
    		}catch (Exception ex) {
        		logger.error("event=check_active_swtich_fail", ex);
        	}

        	// sleep for 1 minute
        	try { 
    			Thread.sleep(CMBProperties.getInstance().getActiveSwitchCheckFrequencySeconds()*1000); 
    		} catch (InterruptedException ex) {	
    			logger.error("event=thread_interrupted", ex);
    		}	
    	}
    	 }
    }
    
    public boolean getActiveActiveSwitch (){
    	return switchFlag;
    }
    
    public boolean getActiveActiveSwitchRealTime (){
		CassandraPersistence cassandraHandler = new CassandraPersistence(CMBProperties.getInstance().getCMBKeyspace());
		String switchFlagString=cassandraHandler.readColumnString("GlobalConfig", "activeactive", 10, CMBProperties.getInstance().getReadConsistencyLevel(), "switch");

		if(switchFlagString!=null&&(switchFlagString.equals("y"))){
			switchFlag=true;
		}else {
			switchFlag=false;
		}
		return switchFlag;
    }
    
    //the value is y or n
    public static void setActiveActiveSwitchRealTime (String value){
    	if(value==null){
    		return;
    	}
		CassandraPersistence cassandraHandler = new CassandraPersistence(CMBProperties.getInstance().getCMBKeyspace());
		Map<String, String> values = new HashMap<String, String>();
    	values.put("switch", value);
		cassandraHandler.insertOrUpdateRow("activeactive", "GlobalConfig", values, CMBProperties.getInstance().getWriteConsistencyLevel());

    }
    
    
    private class CheckOtherDcURLThread extends Thread {
    	 public void run() {
    	while(true){
    		try{
    			otherDcURLMap = new HashMap <String, String>();
    			
    			CassandraPersistence cassandraHandler = new CassandraPersistence(CMBProperties.getInstance().getCQSKeyspace());
    			
    			List<Row<String, String, String>> rows = cassandraHandler.readNextNNonEmptyRows("CQSAPIServers", null, 1000, 10, new StringSerializer(), new StringSerializer(), new StringSerializer(), CMBProperties.getInstance().getReadConsistencyLevel());
    			
    			if (rows != null) {
    				
    				for (Row<String, String, String> row : rows) {
    					
    					CQSAPIStats stats = new CQSAPIStats();
    					stats.setIpAddress(row.getKey());
    					if (row.getColumnSlice().getColumnByName("dataCenter") != null) {
    						stats.setDataCenter(row.getColumnSlice().getColumnByName("dataCenter").getValue());
    						//if it is current data center, ignore it.
    						if(stats.getDataCenter()!=null && stats.getDataCenter().equals(CMBProperties.getInstance().getCMBDataCenter())){
    							continue;
    						}
    					}
    					
    					if (row.getColumnSlice().getColumnByName("serviceUrl") != null) {
    						stats.setServiceUrl(row.getColumnSlice().getColumnByName("serviceUrl").getValue());
    					}
    					
    					if (stats.getIpAddress().contains(":")) {
    						otherDcURLMap.put(stats.getDataCenter(), stats.getServiceUrl());
    					}
    				}
    			}

    			//set otherDcURLs
    			otherDcURLs = new ArrayList <String>();
    			for(Entry <String, String> entity:otherDcURLMap.entrySet()){
    				otherDcURLs.add(entity.getValue());
    			}
    		}catch (Exception ex) {
        		logger.error("event=check_datacenter_url_fail", ex);
        	}

        	// sleep for 1 minute
        	try { 
    			Thread.sleep(60*1000); 
    		} catch (InterruptedException ex) {	
    			logger.error("event=thread_interrupted", ex);
    		}	
    	}
    	 }
    }   
    
    public List <String> getOtherDcURLs (){
    	return otherDcURLs;
    }
}
