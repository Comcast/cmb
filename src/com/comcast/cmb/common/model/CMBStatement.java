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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class represents Statements in a policy. A Statement contains a condition, a list of actions, 
 *   a resource, an effect and a list of accessList representing userids
 * @author bwolf
 */
public class CMBStatement {
	
	public enum EFFECT {
        Allow,  Deny
    };
    
    public static final String PRINCIPAL_FIELD = "AWS";
    
	private String sid;
	private EFFECT effect;
	private Map<String, List<String>> principal = null;
	private List<String> action = null;
	private String resource;
	private CMBCondition condition;

	public CMBStatement() {
		principal = new HashMap<String, List<String>>();
		action = new ArrayList<String>();
	}

	/**
	 * Constructor to set all internal state of the Statement
	 * @param sid
	 * @param effect
	 * @param principalList
	 * @param actionList
	 * @param resource
	 * @param condition
	 */
	public CMBStatement(String sid, String effect, List<String> principalList, List<String> actionList, String resource, CMBCondition condition) {

		this.sid = sid;
		setEffect(effect);
		this.principal = new HashMap<String, List<String>>();
		this.principal.put(PRINCIPAL_FIELD, principalList);
		this.action = actionList;
		this.resource = resource;
		this.condition = condition;
	}

	public void setSid(String sid) {
		this.sid = sid;
	}

	public void setEffect(String effect) {

		if (effect == null) {
			return;
		}

		if (effect.toLowerCase().equals("deny")) {
			this.effect = EFFECT.Deny;
		} else if (effect.toLowerCase().equals("allow")) {
			this.effect = EFFECT.Allow;
		}
	}

	public void setPrincipal(List<String> accessList) {
		this.principal.put(PRINCIPAL_FIELD, accessList);
	}

	public void setAction(List<String> list) {
		this.action = list;
	}

	public void setResource(String resource) {
		this.resource = resource;
	}

	public String getSid() {
		return this.sid;
	}

	public EFFECT getEffect() {
		return this.effect;
	}

	public List<String> getPrincipal() {
		return this.principal.get(PRINCIPAL_FIELD);
	}

	public List<String> getAction() {
		return this.action;
	}

	public String getResource() {
		return this.resource;
	}
	
	public void setCondition(CMBCondition condition) {
		this.condition = condition;
	}
	
	public CMBCondition getCondition() {
		return this.condition;
	}

	public String toString() {

		StringBuffer out = new StringBuffer();

		out.append("\"Sid\": \"").append(this.sid).append("\",\n");
		out.append("\"Effect\": \"").append(this.effect.toString()).append("\",\n");
		out.append("\"Principal\": {\n\"").append(PRINCIPAL_FIELD).append("\": ");
		
		int count = 0;

		int principalSize = this.principal.get(PRINCIPAL_FIELD).size();

		if (principalSize > 1) {

			out.append("[\n");

			for (String uid : this.principal.get(PRINCIPAL_FIELD)) {
				out.append("\"").append(uid).append("\"");
				count++;
				out.append(count != principalSize ? ",\n" : "\n");
			}

			out.append("]\n");

		} else {
			out.append("\"").append(this.principal.get(PRINCIPAL_FIELD).get(0)).append("\"\n");
		}

		out.append("},\n");
		int actionSize = this.action.size();

		if (actionSize > 1) {

			out.append("\"Action\": [\n");
			count = 0;

			for (String each : this.action) {
				out.append("\"").append(each).append("\"");
				count++;
				out.append(count != actionSize ? ",\n" : "\n");
			}

			out.append("],\n");

		} else {
			out.append("\"Action\": \"").append(this.action.get(0))
			.append("\",\n");
		}

		out.append("\"Resource\": \"").append(this.resource).append("\"\n");

		if (condition != null) {
			out.append("\"Condition\": \"").append(this.condition).append("\"\n");
		}
		
		return out.toString();
	}
}
