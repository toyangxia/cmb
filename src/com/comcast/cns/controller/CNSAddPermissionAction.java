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
package com.comcast.plaxo.cns.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.comcast.plaxo.cmb.common.model.CMBPolicy;
import com.comcast.plaxo.cmb.common.model.User;
import com.comcast.plaxo.cmb.common.persistence.IUserPersistence;
import com.comcast.plaxo.cmb.common.persistence.PersistenceFactory;
import com.comcast.plaxo.cmb.common.util.CMBErrorCodes;
import com.comcast.plaxo.cmb.common.util.CMBException;
import com.comcast.plaxo.cmb.common.util.CMBProperties;
import com.comcast.plaxo.cns.io.CNSAttributePopulator;
import com.comcast.plaxo.cns.model.CNSTopic;
import com.comcast.plaxo.cns.model.CNSTopicAttributes;
import com.comcast.plaxo.cns.util.CNSErrorCodes;
import com.comcast.plaxo.cqs.util.CQSConstants;
import com.comcast.plaxo.cqs.util.CQSErrorCodes;
import com.comcast.plaxo.cqs.util.Util;

/**
 * Add permission action
 * @author bwolf
 */
public class CNSAddPermissionAction extends CNSAction {
	
	private static Logger logger = Logger.getLogger(CNSAddPermissionAction.class);
	
	public CNSAddPermissionAction() {
		super("AddPermission");
	}

	@Override
	public boolean doAction(User user, HttpServletRequest request,	HttpServletResponse response) throws Exception {

    	String topicArn = request.getParameter("TopicArn");
    	
    	if ((topicArn == null) ) {
    		logger.error("action=cns_add_permission status=failure event=missing_parameter_topic_arn");
			throw new CMBException(CNSErrorCodes.CNS_InvalidParameter,"Missing parameter TopicArn");
    	}
    	
    	CNSTopic topic = PersistenceFactory.getTopicPersistence().getTopic(topicArn);
    	
    	if (topic == null) {
    		logger.error("action=cns_add_permission status=failure event=invalid_parameter_topic_arn");
			throw new CMBException(CNSErrorCodes.CNS_NotFound,"Resource not found.");
    	}
    	
        String label = request.getParameter(CQSConstants.LABEL);
        
        if (label == null) {
        	throw new CMBException(CMBErrorCodes.ValidationError, "Validation error detected: Value null at 'label' failed to satisfy constraint: Member must not be null");
        }

        if (!Util.isValidId(label)) {
            throw new CMBException(CQSErrorCodes.InvalidBatchEntryId, "Label " + label + " is invalid. Only alphanumeric, hyphen, and underscore are allowed. It can be at most " + CMBProperties.getInstance().getMaxMessageSuppliedIdLength() + " letters long.");
        }
        
        List<String> userList = new ArrayList<String>();
        int index = 1;
        
        String userId = request.getParameter(CQSConstants.AWS_ACCOUNT_ID + ".member." + index);

        IUserPersistence userHandler = PersistenceFactory.getUserPersistence();
        
        while (userId != null) {

        	if (userId.equals("*") || userHandler.getUserById(userId) != null) { // only add user if they exist
                userList.add(userId);
            }
            
        	index++;
            userId = request.getParameter(CQSConstants.AWS_ACCOUNT_ID + ".member." + index);
        }
        
        if (userList.size() == 0) {
            throw new CMBException(CMBErrorCodes.NotFound, "AWSAccountId is required");
        }

        List<String> actionList = new ArrayList<String>();
        index = 1;
        String action = request.getParameter(CQSConstants.ACTION_NAME + ".member." + index);

        while (action != null) {
        	actionList.add(action);
            index++;
            action = request.getParameter(CQSConstants.ACTION_NAME + ".member." + index);
        }
        
        if (actionList.size() == 0) {
            throw new CMBException(CMBErrorCodes.NotFound, "ActionName is required");
        }
        
        CMBPolicy policy = null;
		CNSTopicAttributes attributes = CNSCache.getTopicAttributes(topicArn);

        // validate policy string
        
        if (attributes.getPolicy() != null) {
        	policy = new CMBPolicy(attributes.getPolicy());
        } else {
        	policy = new CMBPolicy();
        }

        if (policy.addStatement(CMBPolicy.SERVICE.CNS, label, "Allow", userList, actionList, topicArn, null)) {
        	attributes.setPolicy(policy.toString());
        	PersistenceFactory.getCNSAttributePersistence().setTopicAttributes(attributes, topicArn);
        } else {
        	throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Value " + label + " for parameter Label is invalid. Reason: Already exists.");
        }

        response.getWriter().print(CNSAttributePopulator.getAddPermissionResponse());	
        return true;
	}
}