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

import java.util.Calendar;
import java.util.List;
import java.util.Random;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.io.CQSQueuePopulator;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.persistence.RedisCachedCassandraPersistence;

/**
 * Poll message id from remote data center
 * @author Jane
 *
 */
public class CQSPollMessageIdsAction extends CQSAction {
    
	private static Logger logger = Logger.getLogger(CQSPollMessageIdsAction.class);
	
	public CQSPollMessageIdsAction() {
		super("PollMessageIds");
	}

	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {
		
        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
        
        CQSQueue queue = CQSCache.getCachedQueue(user, request);
        int maxNumIds = Integer.parseInt(request.getParameter("MaxNumIds"));
        int remoteQueueDepth = Integer.parseInt(request.getParameter("LocalQueueDepth"));
        List<String> messageIds=RedisCachedCassandraPersistence.getInstance().pollMessageIds(queue, maxNumIds, remoteQueueDepth);
        //generate response 
        String out = CQSQueuePopulator.pollMessageIdsResponse(messageIds);
        writeResponse(out, response);
		StringBuffer sb=new StringBuffer();
		for (String messageId: messageIds){
			sb.append(messageId+ ",");
		}
		logger.info("event=pollmessageid_send result:"+sb);
        if(messageIds==null || messageIds.isEmpty()){
        	return false;
        }else {
        	//This is for active active queue. if this data center just send out some message id, 
        	//it will not send poll message id request for n seconds.
        	Calendar currentCalander=Calendar.getInstance();
        	currentCalander.add(Calendar.SECOND, CMBProperties.getInstance().getActiveActiveFrequencySeconds()*10);
        	CQSActiveActiveController.getInstance().setPollMessageIdTimeStamp(queue.getRelativeUrl(), currentCalander);
        	return true;
        }
	}
	
}
