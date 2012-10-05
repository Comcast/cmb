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
package com.comcast.cmb.test.tools;

import java.io.CharArrayWriter;
import java.util.jar.Attributes;
import org.xml.sax.SAXException;

public class ErrorParser  extends org.xml.sax.helpers.DefaultHandler {

	private String code;
	private String message;
	private CharArrayWriter content = new CharArrayWriter();

	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		content.reset();
	}

	public void endElement (String uri, String localName, String qName) {

		if (qName.equals("Code")) {
			code = content.toString();
			content.reset();
		} else  if (qName.equals("Message")) {
			message = content.toString();
			content.reset();
		} else {
			content.reset();
		}
	}

	public void characters( char[] ch, int start, int length ) {
		content.write( ch, start, length );
	}

	public String getCode() {
		return code;
	}

	public String getMessage() {
		return message;
	}
}

