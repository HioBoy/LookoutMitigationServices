package com.amazon.lookout.mitigation.service.workflow.helper;

import java.beans.ConstructorProperties;

import org.apache.commons.lang.Validate;

import lombok.NonNull;
import amazon.mws.query.MonitoringQueryClient;
import amazon.odin.awsauth.OdinAWSCredentialsProvider;

/**
 * A simple provider for providing the clients an instance of MonitoringQueryClient.
 *
 */
public class MonitoringQueryClientProvider {
    private final MonitoringQueryClient monitoringQueryClient;
    
    @ConstructorProperties({"odinAWSCredsProvider", "regionName"})
    public MonitoringQueryClientProvider(@NonNull OdinAWSCredentialsProvider credsProvider, @NonNull String regionName) {
        Validate.notEmpty(regionName);
        monitoringQueryClient = new MonitoringQueryClient(credsProvider, regionName);
    }
    
    public MonitoringQueryClient getClient() {
        return monitoringQueryClient;
    }
}
