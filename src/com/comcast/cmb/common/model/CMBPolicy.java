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
package com.comcast.cmb.common.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;

/**
 * Class used to represent a policy for CNS and CQS. Each Policy has a list of Statements.
 * @author bwolf, vvenkatraman, baosen, tina
 */
public class CMBPolicy {
	
    public static final List<String> POLICY_ATTRIBUTES = Arrays.asList("Version", "Id", "Statement");
	public static final List<String> STATEMENT_ATTRIBUTES = Arrays.asList("Sid", "Effect", "Principal", "Action", "Resource", "Condition");
	
	public enum SERVICE {
        CQS,  CNS
    };

    public static final List<String> CQS_ACTIONS = Arrays.asList("AddPermission", "ChangeMessageVisibility", "ChangeMessageVisibilityBatch", "CreateQueue", "DeleteMessage", "DeleteMessageBatch", "DeleteQueue", "GetQueueAttributes", "GetQueueUrl", "ListQueues", "ReceiveMessage", "RemovePermission", "SendMessage", "SendMessageBatch", "SetQueueAttributes");
    public static final List<String> CNS_ACTIONS = Arrays.asList("AddPermission", "ConfirmSubscription", "CreateTopic", "DeleteTopic", "GetSubscriptionAttributes", "GetTopicAttributes", "ListSubscriptions", "ListSubscriptionsByTopic", "ListTopics", "Publish", "RemovePermission", "SetSubscriptionAttributes", "SetTopicAttributes", "Subscribe", "Unsubscribe");
	
    protected List<CMBStatement> statements = null;
    
    protected String id;
    protected String version;

    /**
     * construct a new policy
     */
    public CMBPolicy() {
    	
        this.id = UUID.randomUUID().toString();
        this.version = "2012-09-13";
        this.statements = new ArrayList<CMBStatement>();
    }
    
    /**
     * parse the policy string to fill statements
     * @param policy  json encoded string of policy
     * @throws Exception 
     */
    public CMBPolicy(String policyString) throws Exception {
        this();
        fromString(policyString);
    }
    
    /**
     * Add a statement to this policy identified by sid. One cannot override an existing statement
     * @param service
     * @param sid
     * @param effect
     * @param userList
     * @param actionList
     * @param resource
     * @param condition
     * @return true if statement was added, false otherwise
     * @throws CMBException
     */
    public boolean addStatement(SERVICE service, String sid, String effect, List<String> userList, List<String> actionList, String resource, CMBCondition condition) throws CMBException {
        
    	if (this.statements.size() > 0) { // no duplicate label can be set
        
    		for (CMBStatement stmt : this.statements) {
            
    			if (stmt.getSid().equals(sid)) {
                    return false;
                }
            }
        }       
    	
    	List<String> normalizedActionList = new ArrayList<String>();
    	
    	for (String action : actionList) {
    		
    		if (action.equals("")) {
    			throw new CMBException(CMBErrorCodes.ValidationError, "Blank action parameter is invalid");
    		}
    		
    		if (!CNS_ACTIONS.contains(action) && !CQS_ACTIONS.contains(action) && !action.equals("*")) {
    			throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Invalid action parameter " + action);
    		}
    		
    		normalizedActionList.add(action.contains(":") ? action : service + ":" + action);
    	}
        
        this.statements.add(new CMBStatement(sid, effect, userList, normalizedActionList, resource, condition));        
        
        return true;
    }
    /**
     * 
     * @param sid
     * @return true if statement was removed/false otherwise
     */
    public boolean removeStatement(String sid) {
    	
        if (this.statements.size() > 0) {
            for (Iterator<CMBStatement> it = statements.iterator(); it.hasNext();) {
                CMBStatement stmt = it.next();
                if (stmt.getSid().equals(sid)) {
                    it.remove();
                    return true;
                }
            }
        }
        
        return false;
    }
    
    public List<CMBStatement> getStatements() {
        return this.statements;
    }

    /**
     * check all statements matching user/action, return false upon the first Deny effect 
     * or no Allow effect; otherwise return true.
     * @param user
     * @param action
     * @return
     */
    public boolean isAllowed(User user, String action) {
    	
        if (statements == null) {
            return false;
        }
        
        boolean allow = false;
        String actionPrefix = action.substring(0, action.lastIndexOf(':') + 1);
        
        for (CMBStatement stmt : statements) {
        	
            if (stmt.getAction().contains(action) || stmt.getAction().contains(actionPrefix + "*")) {
            	
                if (stmt.getPrincipal().contains(user.getUserId()) || stmt.getPrincipal().contains("*")) {
                	
                    if (stmt.getEffect() == CMBStatement.EFFECT.Deny) {
                        return false;
                    } else {
                        allow = true;
                    }
                }
            }
        }
        
        return allow;
    }
    
    @Override
    public String toString() {
        
    	StringBuffer out = new StringBuffer();

        out.append("{\n").append("\"Version\": \"").append(this.version).append("\",\n");
        out.append("\"Id\": \"").append(this.id).append("\",\n");
        out.append("\"Statement\": [\n");
        int count = 0;

        for (CMBStatement stmt : this.statements) {
            out.append("{\n").append(stmt.toString()).append("}");
            count++;
            out.append(count != this.statements.size() ? ",\n" : "\n");
        }
        
        out.append("]\n").append("}");
        
        return out.toString();
    }
    
    /**
     * Parse and populate this object given policyString
     * @param policyString
     * @throws Exception
     */
	public void fromString(String policyString) throws Exception {
		
		if (policyString == null || policyString.isEmpty()) {
        	return;
        }
		
		// check if valid json
		
        JSONObject json = new JSONObject(policyString);
        	
        // validate semantics
        
		Iterator<String> policyIterator = json.keys();
		
		while (policyIterator.hasNext()) {
			
			String policyAttribute = policyIterator.next();
			
			if (!CMBPolicy.POLICY_ATTRIBUTES.contains(policyAttribute)) {
    			throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "Invalid value for the parameter Policy");
			}
			
		}
	
		JSONArray stmts = json.getJSONArray("Statement");
		
		if (stmts != null) {
			
			for (int i=0; i<stmts.length(); i++) {
				
				Iterator<String> stmtIterator = stmts.getJSONObject(i).keys();
				
				while (stmtIterator.hasNext()) {
					
					String statementAttribute = stmtIterator.next();
					
					if (!CMBPolicy.STATEMENT_ATTRIBUTES.contains(statementAttribute)) {
	        			throw new CMBException(CMBErrorCodes.InvalidAttributeValue, "Invalid value for the parameter Policy");
					}
				}
			}
		}
        
        // parse content
        
        this.statements = new ArrayList<CMBStatement>();
        
        if (json.has("Id")) {
        	id = json.getString("Id");
        }
        
        version = json.getString("Version");

        for (int i = 0; i < stmts.length(); i++) {
        	
            JSONObject obj = (JSONObject) stmts.get(i);
            CMBStatement statement = new CMBStatement();

            statement.setSid(obj.getString("Sid"));
            statement.setEffect(obj.getString("Effect"));

            if (obj.has("Condition")) {
            	statement.setCondition(new CMBCondition(obj.getString("Condition")));
            }
            
            String principal = obj.getJSONObject("Principal").getString(CMBStatement.PRINCIPAL_FIELD);

            if (principal.contains("[")) { 
                List<String> accessList = getStringList(obj.getJSONObject("Principal").getJSONArray(CMBStatement.PRINCIPAL_FIELD));
                statement.setPrincipal(accessList);
            } else {
                statement.setPrincipal(Arrays.asList(principal));
            }
            
            String action = obj.getString("Action");

            if (action.contains("[")) { 
                List<String> actionList = getStringList(obj.getJSONArray("Action"));
                statement.setAction(actionList);
            } else {
                statement.setAction(Arrays.asList(action));
            }
            
            if (obj.has("Resource")) {
            	statement.setResource(obj.getString("Resource"));
            }
            
            this.statements.add(statement);
        }
	}
	
    private List<String> getStringList(JSONArray jsonArr) throws JSONException {
    	
        List<String> list = new ArrayList<String>();

        for (int i = 0; i < jsonArr.length(); i++) {
            list.add((String) jsonArr.get(i));
        }
        
        return list;
    }
}
