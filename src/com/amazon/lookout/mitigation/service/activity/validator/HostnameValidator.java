package com.amazon.lookout.mitigation.service.activity.validator;

import com.amazon.lookout.blackwatch.host.config.HostConfig;
import com.amazon.lookout.mitigation.service.RequestHostStatusChangeRequest;
import lombok.NonNull;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HostnameValidator {
    private static final Log LOG = LogFactory.getLog(RequestValidator.class);

    /**
     * Validate whether the given hostname is complete and valid.
     * Validate whether requested location matches the hostname location.
     * Throw IllegalArgumentException otherwise.
     * @param request An instance of RequestHostStatusChangeRequest representing the input to the RequestHostStatusChange API.
     */
    public void validateHostname(@NonNull final RequestHostStatusChangeRequest request) {
        String hostLocation = getLocationAndVerifyHostname(request.getHostName());
        verifyLocation(request, hostLocation);
    }

    /**
     * Verify the hostname is structurally completed with regex.
     * @param hostname the requested hostname whose status to be changed.
     * @return matched location based on the hostname.
     */
    private String getLocationAndVerifyHostname(@NonNull final String hostname) {
        try {
            return HostConfig.getLocationFromHostname(hostname);
        } catch (RuntimeException e) {
            String msg = String.format("Invalid hostname \"%s\" found.", hostname);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Verify whether the host belongs to the requested location.
     * @param request An instance of RequestHostStatusChangeRequest representing the input to the RequestHostStatusChange API.
     * @param hostLocation the location where host belongs to.
     */
    private void verifyLocation(@NonNull final RequestHostStatusChangeRequest request,
                                @NonNull final String hostLocation) {
        Pattern edgeLocationPattern = Pattern.compile(String.format("(e-)?%s", hostLocation), Pattern.CASE_INSENSITIVE);
        if (!edgeLocationPattern.matcher(request.getLocation()).matches()) {
            String msg = String.format("Unmatched hostname \"%s\" at requested location \"%s\", should be \"%s\"",
                    request.getHostName(), request.getLocation(), hostLocation);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
}
