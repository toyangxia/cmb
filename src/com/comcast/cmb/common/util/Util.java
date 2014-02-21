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
package com.comcast.cmb.common.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.amazonaws.Request;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.transform.CreateQueueRequestMarshaller;

/**
 * Utility functions 
 * Class has no state
 * @author bwolf, aseem, vvenkatraman, baosen
 */
public class Util {
	
    private static volatile boolean log4jInitialized = false;
    private static Logger logger = Logger.getLogger(Util.class.getName());
	
    public static void initLog4j(String defaultFile) throws Exception {
		
        if (!log4jInitialized) {
		
            String log4jPropertiesFileName = null;
			
            if (System.getProperty("cmb.log4j.propertyFile") != null) {
                log4jPropertiesFileName = System.getProperty("cmb.log4j.propertyFile");
            } else if (System.getProperty("log4j.propertyFile") != null) {
                log4jPropertiesFileName = System.getProperty("log4j.propertyFile");
            } else if (new File("config/"+defaultFile).exists()) {
                log4jPropertiesFileName = "config/"+defaultFile;
            } else if (new File(defaultFile).exists()) {
                log4jPropertiesFileName = defaultFile;
            } else {
                throw new IllegalArgumentException("Missing VM parameter cmb.log4j.propertyFile");
            }
			
            PropertyConfigurator.configure(log4jPropertiesFileName);
			
            logger.info("event=init_log4j file=" + log4jPropertiesFileName);
			
            log4jInitialized = true;
        }
    }

    public static void initLog4j() throws Exception {
        initLog4j("log4j.properties");
    }

    public static void initLog4jTest() throws Exception {
        initLog4j("test.log4j.properties");
    }

    public static boolean isEqual(Object f1, Object f2) {
        if ((f1 == null && f2 != null) || (f1 != null && f2 == null) || (f1 != null && !f1.equals(f2))) { 
            return false;
        }
        return true;
    }
    
    /**
     * Compare two collections. Method should be called form equals() method impls
     * Note: The order of collections does not matter
     * @param f1 Collection
     * @param f2 Collection
     * @return true if collections have the same size and each element is present in the other
     */
    public static boolean isCollectionsEqual(Collection f1, Collection f2) {
        if ((f1 == null && f2 != null) || (f1 != null && f2 == null)) {
        	return false;
        }
        if (f1 != null) {
            if ((f1.size() != f2.size()) || !isC2InC1(f1, f2) || !isC2InC1(f2, f1)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Compare two maps to see if they are equals. i.r, all the key,value pairs in
     * first exist in the second and nothing else in second. Method should be called
     * from equals
     * Note: assumed V are not collections
     * @param <K>
     * @param <V>
     * @param m1
     * @param m2
     * @return truw if two maps are the same. false otherwise
     */
    public static <K,V> boolean isMapsEquals(Map<K, V> m1, Map<K, V> m2) {
        if (m1 == m2) {
        	return true;
        }
        if ((m1 == null && m2 != null) || (m1 != null && m2 == null)) {
        	return false;        
        }
        if (m1.size() != m2.size()) {
        	return false;
        }
        for (Map.Entry<K, V> entry : m1.entrySet()) {
            K key = entry.getKey();
            V val = entry.getValue();
            V val2 = m2.get(key);
            if (val2 == null) {
            	return false;
            }
            if (!val.equals(val2)) {
                return false;
            }
        }
        return true;        
    }
    
    /**
     * return true if all elements of c2 are in c1
     * @param <T>
     * @param c1
     * @param c2
     * @return
     */
    public static <T> boolean isC2InC1(Collection<T> c1, Collection<T> c2) {
        for (T e1 : c1) {
            boolean e1found = false;        
            for (T theirPlayer : c2) {
                if (e1.equals(theirPlayer)) {
                    e1found = true;
                    break;
                }
            }
            if (!e1found) {
                return false;            
            }
        }
        return true;
    }
    
    public static boolean isValidUnicode(String msg) {
    	//[\u0020-\uD7FF\uE000-\uFFFD\uD800\uDC00-\uDBFF\uDFFF\r\n\t]
        char[] chs = msg.toCharArray();
        for (int i = 0; i < chs.length; i++) {
            if (chs[i] == '\n' || chs[i] == '\t' || chs[i] == '\r' || (chs[i] >= '\u0020' && chs[i] <= '\uD7FF') || (chs[i] >= '\uE000' && chs[i] <= '\uFFFD')) {
                continue;    			
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * Split a passed in list into multiple lists of a given size
     * @param <T>
     * @param list The list to split
     * @param count size of each split list
     * @return list of lists
     */
    public static <T> List<List<T>> splitList(List<T> list, int count) {
        List<List<T>> lofl = new LinkedList<List<T>>();
        for (int i = 0; i < list.size(); i += count) {
            int toIndex = (i + count < list.size()) ? (i + count) : list.size();
            List<T> subList = list.subList(i, toIndex);
            lofl.add(subList);
        }
        return lofl;        
    }
    
    public static String httpGet(String urlString) {
        
    	URL url;
    	HttpURLConnection conn;
    	BufferedReader br;
    	String line;
    	String doc = "";

    	try {

    		url = new URL(urlString);
    		conn = (HttpURLConnection)url.openConnection();
    		conn.setRequestMethod("GET");
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

public static String httpPOST(String baseUrl, String urlString, AWSCredentials awsCredentials) {
    
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
		if(CMBProperties.getInstance().getEnableSignatureAuth()){
			AWS4Signer aws4Signer=new AWS4Signer();
			String host = uri.getHost();
			aws4Signer.setServiceName(host);
			aws4Signer.sign(request, awsCredentials);
		}
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
