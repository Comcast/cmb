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
package com.comcast.plaxo.cqs.util;

import java.util.Random;

/**
 * Utility class used to return n random numbers in the range [0..n] 
 * @author aseem
 */
public class RandomNumberCollection {
    final int totalNum;
    int []randoms;
    final Random r;
    int seenSoFar;
    /**
     * 
     * @param num the range between [0,num) that we will return
     */
    public RandomNumberCollection(int num) {
        totalNum = num;
        seenSoFar = 0;
        randoms = new int[totalNum];
        r = new Random(System.currentTimeMillis());
        for(int i = 0; i < totalNum; i++) {
            randoms[i] = i;
        }
    }
    
    /**
     * @return random number between [0, num] that has not been previously returned
     *   by this instance
     */
    public int getNext() {
        if (seenSoFar == totalNum) throw new IllegalStateException("Already called getNext() " + totalNum + " times");
        int idx = r.nextInt(totalNum - seenSoFar);
        int ret = randoms[idx];
        //swap ret with first unseen number at the end of array 
        seenSoFar++;
        randoms[idx] = randoms[totalNum - seenSoFar];
        randoms[totalNum - seenSoFar] = ret;
        return ret;
    }
    
}