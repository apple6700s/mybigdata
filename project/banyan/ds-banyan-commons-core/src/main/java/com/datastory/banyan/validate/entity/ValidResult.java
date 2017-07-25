package com.datastory.banyan.validate.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by abel.chan on 17/7/5.
 */
public class ValidResult {

    //是否成功
    private boolean isSuccess = false;

    //是否属于出现异常就需要过滤doc的级别
    private boolean isErrorDelete = false;

    //是否属于出现异常将字段置空的级别
    private boolean isErrorIgnore = false;

    private boolean isCanNull = false;

    private int code;

    private String msg;

    private List<String> successFields = new ArrayList<String>();

    private List<String> errorFields = new ArrayList<String>();

    private List<String> nullFields = new ArrayList<String>();

    private List<String> zeroFields = new ArrayList<String>();

    public ValidResult() {
    }

    public void setErrorLevel(ErrorLevel errorLevel) {
        if (errorLevel == null || errorLevel.equals(ErrorLevel.ERROR_IGNORE_NULL)) {
            //说明可以被忽略
            isErrorIgnore = true;
            isSuccess = false;
        } else if (errorLevel.equals(ErrorLevel.ERROR_DELETE)) {
            //出现不可忽略的错误，需要将其删除。
            isErrorDelete = true;
            isSuccess = false;
        }
    }

    public boolean addErrorField(List<String> fields) {
        for (String field : fields) {
            boolean state = addErrorField(field);
            if (!state) {
                return state;
            }
        }
        return true;
    }

    public boolean addErrorField(String field) {
        if (!errorFields.contains(field)) {
            errorFields.add(field);
        }
        return true;
    }

    public boolean addNullField(List<String> fields) {
        for (String field : fields) {
            boolean state = addNullField(field);
            if (!state) {
                return state;
            }
        }
        return true;
    }

    public boolean addNullField(String field) {
        if (!nullFields.contains(field)) {
            nullFields.add(field);
        }
        return true;
    }

    public boolean addZeroField(List<String> fields) {
        for (String field : fields) {
            boolean state = addZeroField(field);
            if (!state) {
                return state;
            }
        }
        return true;
    }

    public boolean addZeroField(String field) {
        if (!zeroFields.contains(field)) {
            zeroFields.add(field);
        }
        return true;
    }

    public boolean addSuccessField(List<String> fields) {
        for (String field : fields) {
            boolean state = addSuccessField(field);
            if (!state) {
                return state;
            }
        }
        return true;
    }

    public boolean addSuccessField(String field) {
        if (!successFields.contains(field)) {
            successFields.add(field);
        }
        return true;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean success) {
        isSuccess = success;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<String> getSuccessFields() {
        return successFields;
    }

    public void setSuccessFields(List<String> successFields) {
        this.successFields = successFields;
    }

    public List<String> getErrorFields() {
        return errorFields;
    }

    public void setErrorFields(List<String> errorFields) {
        this.errorFields = errorFields;
    }

    public List<String> getNullFields() {
        return nullFields;
    }

    public void setNullFields(List<String> nullFields) {
        this.nullFields = nullFields;
    }

    public List<String> getZeroFields() {
        return zeroFields;
    }

    public void setZeroFields(List<String> zeroFields) {
        this.zeroFields = zeroFields;
    }

    public boolean isErrorDelete() {
        return isErrorDelete;
    }

    public void setErrorDelete(boolean errorDelete) {
        isErrorDelete = errorDelete;
    }

    public boolean isErrorIgnore() {
        return isErrorIgnore;
    }

    public void setErrorIgnore(boolean errorIgnore) {
        isErrorIgnore = errorIgnore;
    }

    public boolean isCanNull() {
        return isCanNull;
    }

    public void setCanNull(boolean canNull) {
        isCanNull = canNull;
    }

    @Override
    public String toString() {
        return "ValidResult{" +
                "isSuccess=" + isSuccess +
                ", isErrorDelete=" + isErrorDelete +
                ", isErrorIgnore=" + isErrorIgnore +
                ", isCanNull=" + isCanNull +
                ", code=" + code +
                ", msg='" + msg + '\'' +
                ", successFields=" + successFields +
                ", errorFields=" + errorFields +
                ", nullFields=" + nullFields +
                ", zeroFields=" + zeroFields +
                '}';
    }
}
