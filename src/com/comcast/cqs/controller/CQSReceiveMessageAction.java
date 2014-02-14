/**
 * Copyright 2012 Comcast Corporation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.comcast.cqs.controller;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;

import com.amazonaws.Request;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.transform.CreateQueueRequestMarshaller;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.XmlUtil;
import com.comcast.cqs.io.CQSMessagePopulator;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.RedisCachedCassandraPersistence;
import com.comcast.cqs.util.CQSConstants;

/**
 * Receive message
 * @author aseem, baosen, bwolf, vvenkatraman
 *
 */
public class CQSReceiveMessageAction extends CQSAction {
	
    private static Logger logger = Logger.getLogger(CQSReceiveMessageAction.class);
    
	public CQSReceiveMessageAction() {
		super("ReceiveMessage");
	}
	
	public CQSReceiveMessageAction(String actionName) {
	    super(actionName);
	}
	
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        CQSHttpServletRequest request = (CQSHttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
        
    	CQSQueue queue = CQSCache.getCachedQueue(user, request);
        
        Map<String, String[]> requestParams = request.getParameterMap();
        List<String> filterAttributes = new ArrayList<String>();
        
        for (String k: requestParams.keySet()) {
        	if (k.contains(CQSConstants.ATTRIBUTE_NAME)) {
        		filterAttributes.add(requestParams.get(k)[0]);
        	}
        }
    	
    	request.setFilterAttributes(filterAttributes);
    	request.setQueue(queue);
    	
        HashMap<String, String> msgParam = new HashMap<String, String>();
        
        if (request.getParameter(CQSConstants.MAX_NUMBER_OF_MESSAGES) != null) {
            
        	int maxNumberOfMessages = Integer.parseInt(request.getParameter(CQSConstants.MAX_NUMBER_OF_MESSAGES));
            
        	if (maxNumberOfMessages < 1 || maxNumberOfMessages > CMBProperties.getInstance().getCQSMaxReceiveMessageCount()) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, "The value for MaxNumberOfMessages is not valid (must be from 1 to " + CMBProperties.getInstance().getCQSMaxReceiveMessageCount() + ").");
            }
        	
            msgParam.put(CQSConstants.MAX_NUMBER_OF_MESSAGES, "" + maxNumberOfMessages);
        }

        if (request.getParameter(CQSConstants.VISIBILITY_TIMEOUT) != null) {
        	msgParam.put(CQSConstants.VISIBILITY_TIMEOUT, request.getParameter(CQSConstants.VISIBILITY_TIMEOUT));
        }
        
        // receive timeout overrides queue default timeout if present 
        
        int waitTimeSeconds = queue.getReceiveMessageWaitTimeSeconds();
        
        if (request.getParameter(CQSConstants.WAIT_TIME_SECONDS) != null) {
        	try {
        		waitTimeSeconds = Integer.parseInt(request.getParameter(CQSConstants.WAIT_TIME_SECONDS));
        	} catch (NumberFormatException ex) {
                throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.WAIT_TIME_SECONDS + " must be an integer number.");
        	}
        }
        	
    	if (waitTimeSeconds < 0 || waitTimeSeconds > 20) {
            throw new CMBException(CMBErrorCodes.InvalidParameterValue, CQSConstants.WAIT_TIME_SECONDS + " must be an integer number between 0 and 20.");
    	}

    	// we are already setting wait time in main controller servlet, we are just doing
    	// this here again to throw appropriate error messages for invalid parameters
    	
    	if (!CMBProperties.getInstance().isCQSLongPollEnabled()) {
            //throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Long polling not enabled.");
    		waitTimeSeconds = 0;
    		logger.warn("event=invalid_parameter param=wait_time_seconds reason=long_polling_disabled action=force_to_zero");
    	}

    	//asyncContext.setTimeout(waitTimeSeconds * 1000);
        //request.setWaitTime(waitTimeSeconds * 1000);

        List<CQSMessage> messageList = PersistenceFactory.getCQSMessagePersistence().receiveMessage(queue, msgParam);
        request.setReceiveAttributes(msgParam);
        
        // wait for long poll if desired
        
        if (messageList.size() == 0 && waitTimeSeconds > 0) {
        	
        	// put context on async queue to wait for long poll
        	
        	logger.debug("event=queueing_context queue_arn=" + queue.getArn() + " wait_time_sec=" + waitTimeSeconds);
        	
        	CQSLongPollReceiver.contextQueues.putIfAbsent(queue.getArn(), new ConcurrentLinkedQueue<AsyncContext>());
			ConcurrentLinkedQueue<AsyncContext> contextQueue = CQSLongPollReceiver.contextQueues.get(queue.getArn());
			
			if (contextQueue.offer(asyncContext)) {
	            request.setIsQueuedForProcessing(true);
			}
			
        } else {

            CQSMonitor.getInstance().addNumberOfMessagesReturned(queue.getRelativeUrl(), messageList.size());
            List<String> receiptHandles = new ArrayList<String>();
            
            for (CQSMessage message : messageList) {
            	receiptHandles.add(message.getReceiptHandle());
            }
            
            request.setReceiptHandles(receiptHandles);
            String out = CQSMessagePopulator.getReceiveMessageResponseAfterSerializing(messageList, filterAttributes);
            writeResponse(out, response);
        }
        
		// for ActiveActive, if previous pollMessageIds call time stamp is older
		// than n seconds, kick async call to poll message ID from other Data
		// center
		try {
			if (CQSActiveActiveController.getInstance().getActiveActiveSwitch()) {
				int activeActiveFrequencySeconds = CMBProperties.getInstance()
						.getActiveActiveFrequencySeconds();
				Calendar calendarMinusFrequencySec = Calendar.getInstance();
				calendarMinusFrequencySec.add(Calendar.SECOND,
						0 - activeActiveFrequencySeconds);
				Calendar lastPollMessageIdTimestamp = CQSActiveActiveController
						.getInstance().getPollMessageIdTimeStamp(
								queue.getRelativeUrl());
				if ((lastPollMessageIdTimestamp == null)
						|| ((queue.isActiveActive()) && (lastPollMessageIdTimestamp
								.before(calendarMinusFrequencySec)))) {
					// kick async calls
					CQSActiveActiveController.getInstance().executor
							.submit(new PollMessageIdsRunnable(queue, user));
					CQSActiveActiveController.getInstance()
							.setPollMessageIdTimeStamp(queue.getRelativeUrl(),
									Calendar.getInstance());
				}
			}
		} catch (Exception e) {
			logger.error("event=active_active_exception " + e);
		}

        return messageList != null && messageList.size() > 0 ? true : false;
    }
	

	private class PollMessageIdsRunnable implements Runnable {
		private final CQSQueue queue;
		private final User user;
		public PollMessageIdsRunnable(CQSQueue queue, User user){
			this.queue=queue;
			this.user=user;
		}
	    public void run() {
	        //get max number, local queue depth
	    	int maxNumIds = CMBProperties.getInstance().getPollMessageIdsMaxNum();
	    	try{
	    		int localQueueDepth = (int)RedisCachedCassandraPersistence.getInstance().getQueueMessageCount(queue.getRelativeUrl(), true);

	    		//kick the call
		    		for(String cqsRemoteDatacenterUrl:CQSActiveActiveController.getInstance().getOtherDcURLs()){
						String pollMessageIdsRequestUrl = cqsRemoteDatacenterUrl + queue.getRelativeUrl()+ "?Action=PollMessageIds&MaxNumIds="+maxNumIds+"&LocalQueueDepth="+localQueueDepth+"&AWSAccessKeyId=" + user.getAccessKey();
						AWSCredentials awsCredentials=new BasicAWSCredentials(user.getAccessKey(),user.getAccessSecret());
						String pollMessageIdXml = httpPOST(cqsRemoteDatacenterUrl, pollMessageIdsRequestUrl,awsCredentials);
						Element root = XmlUtil.buildDoc(pollMessageIdXml);
						List<Element> messageIdElements = XmlUtil.getCurrentLevelChildNodes(XmlUtil.getCurrentLevelChildNodes(root, "PollMessageIdsResult").get(0), "Message");
						List <String> messageIdList= new LinkedList<String>();
						for (Element messageIdElement : messageIdElements) {
							messageIdList.add(XmlUtil.getCurrentLevelTextValue(messageIdElement, "MessageId"));
						}

						StringBuffer sb=new StringBuffer();
						for (String messageId: messageIdList){
							sb.append(messageId+ ",");
						}
						logger.info("event=pollmessageid_returned result:"+sb);
						//if not null, add to Redis
						if (messageIdList.size()>0){
							RedisCachedCassandraPersistence.getInstance().setMessageIds(queue, messageIdList);
						}
		    		}
	    	} catch(Exception e){
	    		logger.error(e);
	    	}
	    	//call remote Data center load balancer
	    	//add result to current Redis
	    }

	    protected String httpPOST(String baseUrl, String urlString, AWSCredentials awsCredentials) {
	        
	    	URL url;
	    	HttpURLConnection conn;
	    	BufferedReader br;
	    	String line;
	    	String doc = "";

	    	try {

	    		String urlPost=urlString.substring(0,urlString.indexOf("?"));
	    		url =new URL(urlPost);
	    		conn = (HttpURLConnection)url.openConnection();
	    		conn.setRequestMethod("POST");
	    		
	    		CreateQueueRequest createQueueRequest = new CreateQueueRequest("test");
	    		Request<CreateQueueRequest> request = new CreateQueueRequestMarshaller().marshall(createQueueRequest);
	    		//set parameters from url
	    		String parameterString= urlString.substring(urlString.indexOf("?")+1);
	    		String []parameterArray=parameterString.split("&");
	    		Map <String, String> requestParameters=new HashMap<String, String>();
	    		for(int i=0; i<parameterArray.length;i++){
	    			requestParameters.put(parameterArray[i].substring(0,parameterArray[i].indexOf("=")), 
	    					parameterArray[i].substring(parameterArray[i].indexOf("=")+1));
	    		}
	    		request.setParameters(requestParameters);
	    		//get endpoint from url
	    		URI uri = new URI(baseUrl);
	    		request.setEndpoint(uri);
	    		String resourcePath=urlString.substring(baseUrl.length(), urlString.indexOf("?"));
	    		request.setResourcePath(resourcePath);
	    		
	    		AWS4Signer aws4Signer=new AWS4Signer();
	    		String host = uri.getHost();
	    		aws4Signer.setServiceName(host);
	    		aws4Signer.sign(request, awsCredentials);
	    		
	    		//set headers for real request
	    		for (Entry <String, String>entry:request.getHeaders().entrySet()){
	    			conn.setRequestProperty(entry.getKey(),entry.getValue());	
	    		}
	    		
	    		// Send post request
	    		conn.setDoOutput(true);
	    		DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
	    		StringBuffer bodyStringBuffer=new StringBuffer();
	    		for(Entry <String, String> entry:requestParameters.entrySet()){
	    			bodyStringBuffer.append(entry.getKey()+"="+entry.getValue()+"&");
	    		}
	    		String bodyString="";
	    		if(bodyStringBuffer.length()>0){
	    			bodyString=bodyStringBuffer.substring(0, bodyStringBuffer.length()-1);
	    		}
	    		wr.writeBytes(bodyString);
	    		wr.flush();
	    		wr.close();
	    		
	    		br = new BufferedReader(new InputStreamReader(conn.getInputStream()));

	    		while ((line = br.readLine()) != null) {
	    			doc += line;
	    		}

	    		br.close();

	    		logger.info("event=http_get url=" + urlString);

	    	} catch (Exception ex) {
	    		logger.error("event=http_get url=" + urlString, ex);
	    	}

	    	return doc;
	    }
	}

}
