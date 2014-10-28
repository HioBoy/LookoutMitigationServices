package com.amazon.lookout.mitigation.service.workflow.helper;

import java.beans.ConstructorProperties;

import amazon.mws.query.MonitoringQueryClient;
import amazon.odin.awsauth.OdinAWSCredentialsProvider;

/**
 * A simple provider for providing the clients an instance of MonitoringQueryClient.
 *
 */
public class MonitoringQueryClientProvider {
    private final MonitoringQueryClient monitoringQueryClient;
    
    @ConstructorProperties({"odinAWSCredsProvider"})
    public MonitoringQueryClientProvider(OdinAWSCredentialsProvider credsProvider) {
        monitoringQueryClient = new MonitoringQueryClient(credsProvider);
    }
    
    public MonitoringQueryClient getClient() {
        return monitoringQueryClient;
    }
}
