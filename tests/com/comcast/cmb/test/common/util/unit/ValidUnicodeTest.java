package com.comcast.cmb.test.common.util.unit;

import com.comcast.cmb.common.util.Util;

import org.junit.Test;
import static org.junit.Assert.*;

public class ValidUnicodeTest {
	
    public static final String EMOJI_SAMPLE = "😀😁AA😉😯BB";

    public static final char HIGH = '\uD83D';
    public static final char LOW = '\uDE33';

    public static final String INTACT_NORMAL_SURROGATE = "A😀"+HIGH+LOW+"😯";
    public static final String WRONG_SINGLE_HIGH_SURROGATE = "A😀"+HIGH+"😯";
    public static final String WRONG_SINGLE_LOW_SURROGATE = "A😀"+LOW+"😯";
    public static final String WRONG_FLIPPED_SURROGATE = "A😀"+LOW+HIGH+"😯";

    @Test
    public void testIsValidUnicode() {
        assertTrue(Util.isValidUnicode(EMOJI_SAMPLE));
        assertTrue(Util.isValidUnicode(INTACT_NORMAL_SURROGATE));
        assertFalse(Util.isValidUnicode(WRONG_SINGLE_HIGH_SURROGATE));
        assertFalse(Util.isValidUnicode(INTACT_NORMAL_SURROGATE+HIGH));
        assertFalse(Util.isValidUnicode(WRONG_SINGLE_LOW_SURROGATE));
        assertFalse(Util.isValidUnicode(INTACT_NORMAL_SURROGATE+LOW));      
        assertFalse(Util.isValidUnicode(WRONG_FLIPPED_SURROGATE));
        assertFalse(Util.isValidUnicode(INTACT_NORMAL_SURROGATE+LOW+HIGH));
    }
}