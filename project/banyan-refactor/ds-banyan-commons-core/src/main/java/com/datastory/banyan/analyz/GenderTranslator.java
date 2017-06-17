package com.datastory.banyan.analyz;

import java.io.Serializable;

/**
 * com.datastory.banyan.analyz.GenderTranslator
 *  0->unkown, 1->male, 2->female
 * @author lhfcws
 * @since 16/12/8
 */

public class GenderTranslator implements Serializable {

    public static int translate(String s) {
        if (s != null) {
            if (s.equals("m") || s.equals("1"))
                return 1;
            else if (s.equals("f") || s.equals("2"))
                return 2;
        }
        return 0;
    }
}
