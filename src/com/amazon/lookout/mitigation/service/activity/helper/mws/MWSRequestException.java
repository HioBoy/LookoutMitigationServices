package com.amazon.lookout.mitigation.service.activity.helper.mws;

public class MWSRequestException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MWSRequestException(String msg) {
        super(msg);
    }

    public MWSRequestException(String msg, Throwable e) {
        super(msg, e);
    }
}