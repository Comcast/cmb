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

import com.comcast.cmb.common.model.ReceiptModule;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.helpers.PatternConverter;
import org.apache.log4j.helpers.PatternParser;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Pattern definition for log4j
 * @author michael, baosen, vvenkatraman
 */
public class CMBPatternLayout extends PatternLayout {
	
	public CMBPatternLayout() {
		if (hostname != null) return;
		
		try {
			java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
			hostname = localMachine.getHostName();
		}
		catch (java.net.UnknownHostException uhe) { 
			hostname = null;
		}
	}

    protected PatternParser createPatternParser(String pattern) {
        return new CMBPatternParser(pattern);
    }

    static final char RECEIPT_CHAR = 'R';
    static final char HOST_NAME = 'H';
    
    private final CMBPatternConverter receiptConverter = new CMBPatternConverter(RECEIPT_CHAR);
    private final CMBPatternConverter hostnameConverter = new CMBPatternConverter(HOST_NAME);
    
    private static volatile String hostname = null;

    class CMBPatternParser extends PatternParser {

        CMBPatternParser(String pattern) {
            super(pattern);
        }

        protected void finalizeConverter(char c) {
            switch (c) {
                case RECEIPT_CHAR:
                    currentLiteral.setLength(0);
                    addConverter(receiptConverter);
                    break;
                	
                case HOST_NAME:
                    currentLiteral.setLength(0);
                    addConverter(hostnameConverter);
                    break;

                default:
                    super.finalizeConverter(c);
            }
        }
    }

    class CMBPatternConverter extends PatternConverter {
    	private char patternChar;
    	
    	CMBPatternConverter(char c) {
    		patternChar = c;
    	}

        protected String convert(LoggingEvent evt) {
        	if (patternChar == RECEIPT_CHAR)
        		return ReceiptModule.getReceiptId();
        	else if (patternChar == HOST_NAME)
        		return hostname;
        	return null;
        }
    }
}
