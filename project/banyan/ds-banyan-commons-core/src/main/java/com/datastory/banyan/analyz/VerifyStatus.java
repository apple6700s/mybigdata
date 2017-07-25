package com.datastory.banyan.analyz;

/**
 * com.datastory.banyan.analyz.VerifyStatus
 *
 * @author lhfcws
 * @since 16/12/12
 */

public enum VerifyStatus {
    UNVERIFY(-1), APPROVING(0), VERFIED(1),
    WX_VERIFY(2), WB_VERIFY(3), PHONE_VERIFY(4),
    MAIL_VERIFY(5), IDENTITY_VERIFY(6),
    EDU_VERIFY(7), COM_VERIFY(8)
    ;

    private int v;

    VerifyStatus(int v) {
        this.v = v;
    }

    public int getValue() {
        return v;
    }

    public static VerifyStatus fromValue(int v) {
        for (VerifyStatus vs : values()) {
            if (vs.getValue() == v)
                return vs;
        }
        return null;
    }

    public static VerifyStatus fromValueStr(String s) {
        try {
            int v = Integer.valueOf(s);
            return fromValue(v);
        } catch (Exception e) {
            return null;
        }
    }
}
