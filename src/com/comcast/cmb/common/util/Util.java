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

import java.io.File;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

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
            } else if (i<chs.length) { // check for 4 bytes unicode, 2 char (utf16)
                if ((chs[i] >= '\uD800' && chs[i+1] >= '\uDC00') && (chs[i] <= '\uDBFF' && chs[i+1] <= '\uDFFF')) {
                    i++; // skip the next char
                    continue;
                }
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
}
