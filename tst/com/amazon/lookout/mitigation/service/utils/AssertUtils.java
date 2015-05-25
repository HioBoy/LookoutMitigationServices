package com.amazon.lookout.mitigation.service.utils;

import static org.junit.Assert.fail;

public class AssertUtils {
    @SuppressWarnings("unchecked")
    public static <T extends Throwable> T assertThrows(Class<T> expectedException, Runnable action) {
        try {
            action.run();
            fail(String.format("Exception expected but no exception thrown: %s.", expectedException));
            throw new RuntimeException("Making compiler happy about return value");
        } catch (Throwable error) {
            if (expectedException.isInstance(error)) {
                return (T) error;
            } else {
                throw error;
            }
        }
    }
}
