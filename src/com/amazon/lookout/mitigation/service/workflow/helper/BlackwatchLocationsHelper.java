package com.amazon.lookout.mitigation.service.workflow.helper;

import java.beans.ConstructorProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import amazon.mws.data.Datapoint;
import amazon.mws.data.MetricSchema;
import amazon.mws.data.Statistic;
import amazon.mws.data.StatisticSeries;
import amazon.mws.data.StringTimeRange;
import amazon.mws.query.MonitoringQueryClient;
import amazon.mws.request.GetMetricDataRequest;
import amazon.mws.response.GetMetricDataResponse;
import amazon.mws.response.ResponseException;
import amazon.mws.types.Stat;
import amazon.mws.types.StatPeriod;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.ldaputils.LdapProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Helper to identify which POPs are Blackwatch capable.
 */
public class BlackwatchLocationsHelper {
    private static final Log LOG = LogFactory.getLog(BlackwatchLocationsHelper.class);
    
    // Constants to help performing a LDAP poll. 
    private static final String BASE_DISTINGUISHED_NAME = "ou=systems,ou=infrastructure,o=amazon.com";
    private static final String HOSTCLASS_LDAP_NAME = "amznDiscoHostClass";
    
    // For fetching the agent names from the hostclasses via LDAP
    private static final String HOSTCLASS_START_STR = "AWS-EDGE-";
    private static final String HOSTCLASS_END_STR = "-BW";
    
    // Configurations for attempts on failures.
    private static final int MAX_QUERY_ATTEMPTS = 3;
    private static final int SLEEP_BETWEEN_ATTEMPTS_IN_MILLIS = 500;
    
    private static final String GAMMA_IAD_POP_NAME = "G-IAD";
    
    private final LdapProvider ldapProvider;
    // This flag exists solely to be used in gamma - to indicate that this helper should treat Gamma BW POP as a non-BW POP.
    // This flag is handy when testing out workflows in gamma, where we have only 1 router where we could apply mitigations, which also happens to be a Gamma BW POP.
    private final boolean overrideGammaBWPOPAsNonBW;
    
    private final ImmutableMap<String, String> mwsMetricBaseDimensions;
    
    // Constant keys related to the metric dimensions
    public static final String BLACKWATCH_METRIC_SERVICE_NAME = "LookoutBlackWatch";
    public static final String DATASET_KEY = "DataSet";
    public static final String HOST_GROUP_KEY = "HostGroup";
    public static final String HOST_KEY = "Host";
    public static final String SERVICE_NAME_KEY = "ServiceName";
    public static final String METHOD_NAME_KEY = "MethodName";
    public static final String CLIENT_NAME_KEY = "Client";
    public static final String METRIC_CLASS_KEY = "MetricClass";
    public static final String INSTANCE_KEY = "Instance";
    public static final String METRIC_KEY = "Metric";
    public static final String SERVICE_METRIC_SCHEMA_KEY = "Service";
    public static final String MARKETPLACE_KEY = "Marketplace";
    
    public static final String METRIC_QUERY_START = "10 minutes ago";
    public static final String METRIC_QUERY_END = "now";
    
    private static final String NUM_ATTEMPTS_METRIC_KEY = "NumAttempts";
    
    public static final String METRIC_NOT_FOUND_EXCEPTION_MESSAGE = "MetricNotFound: No metrics matched your request parameters";
    
    private final long blackwatchInlineThreshold;
    private final MonitoringQueryClient mwsQueryClient;
    
    @ConstructorProperties({"ldapProvider", "overrideGammaBWPOPAsNonBW", "mwsClientProvider", "mwsMetricDataset", "blackwatchMWSMetricName", "blackwatchInlineThreshold"})
    public BlackwatchLocationsHelper(@NonNull LdapProvider ldapProvider, boolean overrideGammaBWPOPAsNonBW, @NonNull MonitoringQueryClientProvider mwsClientProvider,
                                     @NonNull String mwsMetricDataset, @NonNull String blackwatchMWSMetricName, long blackwatchInlineThreshold) {
        this.ldapProvider = ldapProvider;
        this.overrideGammaBWPOPAsNonBW = overrideGammaBWPOPAsNonBW;
        this.mwsQueryClient = mwsClientProvider.getClient();
        
        ImmutableMap.Builder<String, String> dimensions = new ImmutableMap.Builder<>();
        dimensions.put(DATASET_KEY, mwsMetricDataset);
        dimensions.put(HOST_GROUP_KEY, "ALL");
        dimensions.put(HOST_KEY, "ALL");
        dimensions.put(SERVICE_NAME_KEY, BLACKWATCH_METRIC_SERVICE_NAME);
        dimensions.put(METHOD_NAME_KEY, "ALL");
        dimensions.put(CLIENT_NAME_KEY, "ALL");
        dimensions.put(METRIC_CLASS_KEY, "NONE");
        dimensions.put(INSTANCE_KEY, "NONE");
        dimensions.put(METRIC_KEY, blackwatchMWSMetricName);
        mwsMetricBaseDimensions = dimensions.build();
        
        this.blackwatchInlineThreshold = blackwatchInlineThreshold;
    }
    
    /**
     * Returns whether the given POP can be considered a Blackwatch POP.
     * A POP is considered a non-Blackwatch POP if it has no hosts built for that POP. 
     * If a POP has hosts built or we are unable to confirm if it has hosts built, then we query MWS for a BW metric to check if
     * it has a recent data point above a configured threshold. If it does, then we label such a POP as a BW POP, else non-BW otherwise.
     * @param popName POP which needs to be checked for being a Blackwatch POP.
     * @param metrics Instance of TSDMetrics.
     * @return true if this POP is deemed a Blackwatch POP using the logic above, false otherwise.
     */
    public boolean isBlackwatchPOP(@NonNull String popName, @NonNull TSDMetrics metrics) {
        try (TSDMetrics subMetrics = metrics.newSubMetrics("isBlackwatchPOP")) {
            Validate.notEmpty(popName);
            
            String hostclass = createBWHostclassForPOP(popName);
            
            int numAttempts = 0;
            TSDMetrics ldapQueryMetrics = metrics.newSubMetrics("isBlackwatchPOP.ldapQuery");
            try {
                while (numAttempts < MAX_QUERY_ATTEMPTS) {
                    ++numAttempts;
                    try {
                        List<Map<String, List<Object>>> ldapResult = ldapProvider.search(BASE_DISTINGUISHED_NAME, "(" + HOSTCLASS_LDAP_NAME + "=" + hostclass + 
                                                                                         ")", Integer.MAX_VALUE, ImmutableList.of("cn"));
                        List<String> serversList = ldapProvider.toSimpleList(ldapResult, "cn");
                        
                        // If we don't have any BW hosts build for this POP, return false.
                        if (serversList.isEmpty()) {
                            LOG.info("No BW hosts built for pop: " + popName + ", hence marking this POP as a non-BW POP.");
                            return false;
                        }
                        
                        break;
                    } catch (Exception ex) {
                        LOG.warn("Unable to poll LDAP for hostclass: " + hostclass + " for pop: " + popName + ", numAttempts: " + numAttempts, ex);
                        
                        if (numAttempts < MAX_QUERY_ATTEMPTS) {
                            try {
                                Thread.sleep(SLEEP_BETWEEN_ATTEMPTS_IN_MILLIS);
                            } catch (InterruptedException ignored) {}
                        } else {
                            LOG.warn("Unable to poll LDAP to find if Blackwatch has hosts built in pop: " + popName + " after: " + numAttempts + 
                                     " number of attempts. Giving up checking with LDAP and now checking BW's MWS metricfor this pop.");
                        }
                    }
                }
            } finally {
                ldapQueryMetrics.addCount(NUM_ATTEMPTS_METRIC_KEY, numAttempts);
                ldapQueryMetrics.end();
            }
            
            boolean hasRecentDataForBWMetric = hasRecentDataForBWMetric(popName, metrics);
            
            // Check if we need to force the gamma IAD POP to return as a non-BW POP.
            if (popName.toUpperCase().startsWith(GAMMA_IAD_POP_NAME) && overrideGammaBWPOPAsNonBW) {
                LOG.info("Overridding the BW status for pop: " + popName + " to be a non-BW POP by default.");
                return false;
            } else {
                LOG.info("Returning the BW status for pop: " + popName + " to be: " + hasRecentDataForBWMetric);
                return hasRecentDataForBWMetric;
            }
        }
    }
    
    /**
     * Checks if there is at least 1 recent data point for a BW metric for the given POP. If there is such a data point, we return true, false otherwise.
     * @param popName POP for whom we need to check if there is at least a single recent data point for a BW metric that is above the configured threshold.
     * @param metrics Instance of TSDMetrics.
     * @return true if there is at least a single recent data point for a BW metric that is above the configured threshold, false otherwise.
     */
    protected boolean hasRecentDataForBWMetric(@NonNull String popName, @NonNull TSDMetrics metrics) {
        int numAttempts = 0;
        TSDMetrics subMetrics = metrics.newSubMetrics("isBlackwatchPOP.hasRecentDataForBWMetric");
        try {
            Validate.notEmpty(popName);
            
            Map<String, String> dimensions = new HashMap<>(mwsMetricBaseDimensions);
            dimensions.put(MARKETPLACE_KEY, popName.toUpperCase());
            MetricSchema schema = new MetricSchema(SERVICE_METRIC_SCHEMA_KEY, dimensions);
            Statistic statistic = new Statistic(schema, StatPeriod.OneMinute, Stat.sum);
            
            StringTimeRange timeRange = new StringTimeRange(METRIC_QUERY_START, METRIC_QUERY_END);
            
            Exception lastException = null;
            while (numAttempts < MAX_QUERY_ATTEMPTS) {
                ++numAttempts;
                try {
                    GetMetricDataRequest getMetricDataRequest = new GetMetricDataRequest(statistic, timeRange);
                    GetMetricDataResponse response = (GetMetricDataResponse) mwsQueryClient.requestResponse(getMetricDataRequest);
                    return hasDataAboveThreshold(response);
                } catch (Exception ex) {
                    // If the exception is for the metric not being found, then that either implies that BW never published metrics for this POP or it has stopped for a long time (typically 30 days).
                    // In both case, we would return false since BW isn't actively running.
                    if ((ex instanceof ResponseException) && ((ResponseException) ex).getMessage().contains(METRIC_NOT_FOUND_EXCEPTION_MESSAGE)) {
                        LOG.info("Caught a ResponseException stating that the metric was not found for dimensions: " + 
                                 dimensions + " for POP: " + popName + ", hence returning false.", ex);
                        return false;
                    }
                    
                    lastException = ex;
                    LOG.warn("Unable to query MWS for metric dimensions:" + dimensions + " for POP: " + popName + ", numAttempts: " + numAttempts, ex);
                    
                    if (numAttempts < MAX_QUERY_ATTEMPTS) {
                        try {
                            Thread.sleep(SLEEP_BETWEEN_ATTEMPTS_IN_MILLIS);
                        } catch (InterruptedException ignored) {}
                    }
                }
            }
            String msg = "Unable to query MWS for metric dimensions:" + dimensions + " for POP: " + popName + " after: " + numAttempts + " number of attempts";
            LOG.warn(msg, lastException);
            throw new RuntimeException(msg, lastException);
        } finally {
            subMetrics.addCount(NUM_ATTEMPTS_METRIC_KEY, numAttempts);
            subMetrics.end();
        }
    }
    
    /**
     * Helper method which checks if the MWS response has at least 1 data point which is above configured threshold.
     * @param response Instance of GetMetricDataResponse returned by MWS to represent the metric data.
     * @return true if at least 1 data point is above configured threshold, false otherwise.
     */
    private boolean hasDataAboveThreshold(@NonNull GetMetricDataResponse response) {
        for (StatisticSeries statisticSeries : response.getStatisticSeries()) {
            for (Datapoint datapoint : statisticSeries.getDatapoints()) {
                if (datapoint.getValue() >= blackwatchInlineThreshold) {
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * Helper to create the Blackwatch hostclass name for the pop passed as input. Protected for unit tests.
     * @param popName POP for whom we need to create the Blackwatch hostclass name.
     * @return String representing the Blackwatch hostclass name for the input POP.
     */
    protected String createBWHostclassForPOP(String popName) {
        String hostclass = HOSTCLASS_START_STR + popName.toUpperCase() + HOSTCLASS_END_STR;
        
        // Include an edge-case for the BlackWatch gamma host class so we can grab the right agent hosts.
        if (popName.toUpperCase().equals(GAMMA_IAD_POP_NAME)) {
            hostclass = HOSTCLASS_START_STR + "G-IAD" + HOSTCLASS_END_STR;
        }
        return hostclass;
    }
}
