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

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XmlUtil {
	
	public static Element buildDoc(String content) throws Exception {
		
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document dom = db.parse(new ByteArrayInputStream(content.getBytes("UTF-8")));
		Element docEle = dom.getDocumentElement();
		
		return docEle;
	}

	public static List<Element> getCurrentLevelChildNodes(Element ele, String tagName) {
		
		List<Element> elements = new ArrayList<Element>();
		
		if (ele == null) {
			return elements;
		}
		NodeList nl = ele.getChildNodes();
		
		if (nl != null && nl.getLength() > 0) {
			for (Integer i=0; i<nl.getLength(); i++) {
				Node node = nl.item(i);
				if (node instanceof Element && ((Element)node).getNodeName().equals(tagName)) {
					elements.add((Element)node);
				}
			}
		}
	
		return elements;
	}

	public static String getCurrentLevelTextValue(Element ele, String tagName) {
		
		String textVal = null;
		
		List<Element> nl = getCurrentLevelChildNodes(ele, tagName);
		
		if (nl != null && nl.size() == 1) {
			Element el = (Element)nl.get(0);
			textVal = el.getFirstChild().getNodeValue();
		}
	
		return textVal;
	}
	
	public static Integer getCurrentLevelIntValue(Element ele, String tagName) {
		String textToInt = getCurrentLevelTextValue(ele,tagName);
		Integer valueInteger = null;
		if (textToInt != null) {
			valueInteger = new Integer(textToInt);
		}
		return valueInteger;
	}
	
	public static Double getCurrentLevelDoubleValue(Element ele, String tagName) {
		String textToDouble = getCurrentLevelTextValue(ele,tagName);
		Double valueDouble = null;
		if (textToDouble != null) {
			valueDouble = new Double(textToDouble);
		}
		return valueDouble;
	}
	
	public static boolean getCurrentLevelBooleanValue(Element ele, String tagName) {
		return Boolean.parseBoolean(getCurrentLevelTextValue(ele, tagName));
	}

	public static String getCurrentLevelAttributeTextValue(Element ele, String attribute) {
		if (ele == null || attribute == null) {
			return null;
		} else {
			return ele.getAttribute(attribute);
		}	
	}
	
	public static Integer getCurrentLevelAttributeIntValue(Element ele, String attribute) {
		Integer attributeInt = null;
		if (ele == null || attribute == null) {
			return attributeInt;
		}
		String attributeString = ele.getAttribute(attribute);
		if (attributeString != null) {
			attributeInt = new Integer(attributeString);
		}
		return attributeInt;
	}
}
