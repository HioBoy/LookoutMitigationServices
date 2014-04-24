package com.amazon.lookout.mitigation.service.workflow.helper;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.daas.control.DNSServer;
import com.amazon.daas.control.DaasControlAPIServiceV20100701Client;
import com.amazon.daas.control.ListDNSServersRequest;
import com.amazon.daas.control.ListDNSServersResponse;
import com.amazon.daas.control.impl.ListDNSServersCall;
import com.amazon.edge.service.EdgeOperatorServiceClient;
import com.amazon.edge.service.GetPOPsResult;
import com.amazon.edge.service.impl.GetPOPsCall;
import com.google.common.collect.Sets;

/**
 * Helper that helps in identifying the POP locations for deployment.
 *
 */
@ThreadSafe
public class EdgeLocationsHelper implements Runnable {
    private static final Log LOG = LogFactory.getLog(EdgeLocationsHelper.class);
    
    // Configurations for retrying on failures during refresh.
    private static final int MAX_POP_NAMES_REFRESH_ATTEMPTS = 3;

    private static final String ROUTE53_POP_NAMES_REFRESH_METRIC = "Route53Refresh";
    private static final String CLOUDFRONT_POP_NAMES_REFRESH_METRIC = "EdgeServicesRefresh";
    private static final String BLACKWATCH_POP_CHECK_METRIC = "BlackwatchCheck";
    
    // Maintain a flag that indicates that the refresh of POP names and the checks if they are Blackwatch capable have been completed at least once.
    private final AtomicBoolean locationsRefreshAtleastOnce = new AtomicBoolean(false);
    
    // Maintain a copy of all POPs known to this locations helper.
    private final CopyOnWriteArraySet<String> allPOPs = new CopyOnWriteArraySet<>();
    
    // Maintain a copy of all POPs known to this locations helper which have Blackwatch hosts installed.
    private final CopyOnWriteArraySet<String> blackwatchPOPs = new CopyOnWriteArraySet<>();
    
    private final EdgeOperatorServiceClient cloudfrontClient;
    private final DaasControlAPIServiceV20100701Client daasClient;
    private final BlackwatchLocationsHelper bwLocationsHelper;
    private final int sleepBetweenRetriesInMillis;
    private final MetricsFactory metricsFactory;

    public EdgeLocationsHelper(@Nonnull EdgeOperatorServiceClient cloudfrontClient, @Nonnull DaasControlAPIServiceV20100701Client daasClient,
                               @Nonnull BlackwatchLocationsHelper bwLocationsHelper, int sleepBetweenRetriesInMillis, @Nonnull MetricsFactory metricsFactory) {
        Validate.notNull(cloudfrontClient);
        this.cloudfrontClient = cloudfrontClient;
        
        Validate.notNull(daasClient);
        this.daasClient = daasClient;
        
        Validate.notNull(bwLocationsHelper);
        this.bwLocationsHelper = bwLocationsHelper;
        
        Validate.notNull(metricsFactory);
        this.metricsFactory = metricsFactory;
        
        Validate.isTrue(sleepBetweenRetriesInMillis > 0);
        this.sleepBetweenRetriesInMillis = sleepBetweenRetriesInMillis;
        
        refreshPOPLocations();
    }
    
    public Set<String> getAllPOPs() {
        if (!locationsRefreshAtleastOnce.get()) {
            refreshPOPLocations();
        }
        
        if (locationsRefreshAtleastOnce.get()) {
            return allPOPs;
        } else {
            String msg = "Unable to refresh the list of POPs for this getAllPOPs call";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
    }
    
    public Set<String> getBlackwatchPOPs() {
        if (!locationsRefreshAtleastOnce.get()) {
            refreshPOPLocations();
        }
        
        if (locationsRefreshAtleastOnce.get()) {
            return blackwatchPOPs;
        } else {
            String msg = "Unable to refresh the list of POPs for this getBlackwatchPOPs call";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
    }
    
    public Set<String> getAllNonBlackwatchPOPs() {
        if (!locationsRefreshAtleastOnce.get()) {
            refreshPOPLocations();
        }
        
        if (locationsRefreshAtleastOnce.get()) {
        	return (Sets.difference(allPOPs, blackwatchPOPs));
        } else {
            String msg = "Unable to refresh the list of POPs for this getAllNonBlackwatchPOPs call";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
    }
    
    public void run() {
        refreshPOPLocations();
    }
    
    /**
     * Refreshes the list of all pops and pops with/without blackwatch.
     * 
     * We also add a counter for CloudFront/Route53 with a value of 1 if the API calls to their respective customer API for refreshing the POP names succeed, 
     * else we populate a 0 - this will help us to see the trend of how often we fail/succeed the refresh the POP names from these services.
     * We publish a similar metric for the check indicating whether a POP has Blackwatch hosts or not using LDAP search.
     */
    private void refreshPOPLocations() {
        TSDMetrics metrics = new TSDMetrics(metricsFactory, "refreshPOPLocations");
        try {
            Set<String> refreshedPOPsList = new HashSet<>();
            
            // Maintain a flag indicating all the service calls are successful or not.
            boolean allActionsSuccessful = true;
            
            // First check with the CloudFront API to get the list of all POPs known to it. Note, we only add any new POPs, but not remove any POPs from the existing set.
            // We do so to err on the side of us trying to deploy mitigation to a location that no longer exists than to miss out on a valid POP due to some bug in
            // the edge customer APIs.
            try {
                refreshedPOPsList.addAll(getCloudFrontPOPs());
                metrics.addCount(CLOUDFRONT_POP_NAMES_REFRESH_METRIC, 1);
            } catch (Exception ex) {
                allActionsSuccessful = false;
                metrics.addCount(CLOUDFRONT_POP_NAMES_REFRESH_METRIC, 0);
                
                String msg = "Caught exception when getting back CloudFrontPOPs, not updating any CloudFrontPOPs at this point";
                LOG.warn(msg, ex);
            }
            
            // Also check with the Route53 API to get the list of all POPs known to it, incase some POPs don't have CloudFront, but only Route53. Note, we only add any new POPs, 
            // but not remove any POPs from the existing set. We do so to err on the side of us trying to deploy mitigation to a location that no longer exists than to miss out 
            // on a valid POP due to some bug in the edge customer APIs. 
            try {
                refreshedPOPsList.addAll(getRoute53POPs());
                metrics.addCount(ROUTE53_POP_NAMES_REFRESH_METRIC, 1);
            } catch (Exception ex) {
                allActionsSuccessful = false;
                metrics.addCount(ROUTE53_POP_NAMES_REFRESH_METRIC, 0);
                
                String msg = "Caught exception when getting back Route53POPs, not updating any Route53POPs at this point";
                LOG.warn(msg, ex);
            }
            
            boolean popsUpdated = false;
            
            // If the list of all pops has changed, then update.
            if (!refreshedPOPsList.equals(allPOPs)) {
                allPOPs.addAll(refreshedPOPsList);
                popsUpdated = true;
            }
            
            // Also refresh the list of BW POPs here.
            Set<String> refreshedBWPOPs = new HashSet<>();
            boolean allPOPsCheckedForBlackwatch = true;
            for (String popName : allPOPs) {
                try {
                    if (bwLocationsHelper.isBlackwatchPOP(popName)) {
                        refreshedBWPOPs.add(popName);
                    }
                } catch (Exception ex) {
                    allActionsSuccessful = false;
                    allPOPsCheckedForBlackwatch = false;
                    
                    String msg = "Caught exception when checking if pop: " + popName + " is Blackwatch capable, continuing checking the other POPs now." +
                                 " Current status for this POP having Blackwatch: " + blackwatchPOPs.contains(popName) + ". Leaving this status as is.";
                    LOG.warn(msg, ex);
                }
            }
            
            if (allPOPsCheckedForBlackwatch) {
                metrics.addCount(BLACKWATCH_POP_CHECK_METRIC, 1);
            } else {
                metrics.addCount(BLACKWATCH_POP_CHECK_METRIC, 0);
            }
            
            // If the list of Blackwatch pops has changed, then update.
            if (!refreshedBWPOPs.equals(blackwatchPOPs)) {
                blackwatchPOPs.addAll(refreshedBWPOPs);
                popsUpdated = true;
            }
            
            if (allActionsSuccessful) {
                locationsRefreshAtleastOnce.set(true);
            }
        } finally {
            metrics.end();
        }
    }
    
    /**
     * Helper to get the current set of POPs known to the EdgeOperatorService.
     * @return Set<String> representing POP names known to the EdgeOperatorService.
     */
    private Set<String> getCloudFrontPOPs() {
        Set<String> cloudfrontPOPs = new HashSet<>();
        
        GetPOPsCall getPOPsCall = cloudfrontClient.newGetPOPsCall();
        
        int numAttempts = 0;
        while (numAttempts++ < MAX_POP_NAMES_REFRESH_ATTEMPTS) {
            try {
                GetPOPsResult cfGetPOPsResult = getPOPsCall.call();
                for (String popName : cfGetPOPsResult.getPOPList()) {
                    cloudfrontPOPs.add(popName.toUpperCase());
                }
                break;
            } catch (Exception ex) {
                LOG.warn("Caught exception when refreshing POP names from EdgeOperatorService. NumAttempts: " + numAttempts, ex);
                
                if (numAttempts < MAX_POP_NAMES_REFRESH_ATTEMPTS) {
                    try {
                        Thread.sleep(sleepBetweenRetriesInMillis);
                    } catch (InterruptedException ignored) {}
                } else {
                    String msg = "Unable to refresh POP names from EdgeOperatorService after " + numAttempts;
                    LOG.warn(msg);
                    throw new RuntimeException(msg);
                }
            }
        }
        
        return cloudfrontPOPs;
    }
    
    /**
     * Helper to get the current set of POPs known to the DaasControlAPIServiceV20100701.
     * @return Set<String> representing POP names known to the DaasControlAPIServiceV20100701.
     */
    private Set<String> getRoute53POPs() {
        Set<String> route53POPs = new HashSet<>();
        
        ListDNSServersCall listDNSServersCall = daasClient.newListDNSServersCall();
        ListDNSServersRequest listDNSServersRequest = new ListDNSServersRequest();
        
        int numAttempts = 0;
        while (numAttempts++ < MAX_POP_NAMES_REFRESH_ATTEMPTS) {
            try {
                ListDNSServersResponse listDNSServersResponse = listDNSServersCall.call(listDNSServersRequest);
                for (DNSServer dnsServer : listDNSServersResponse.getResults()) {
                    route53POPs.add(dnsServer.getPOP().toUpperCase());
                }
                break;
            } catch (Exception ex) {
                LOG.warn("Caught exception when refreshing POP names from DaasControlAPIServiceV20100701. NumAttempts: " + numAttempts, ex);
                
                if (numAttempts < MAX_POP_NAMES_REFRESH_ATTEMPTS) {
                    try {
                        Thread.sleep(sleepBetweenRetriesInMillis);
                    } catch (InterruptedException ignored) {}
                } else {
                    String msg = "Unable to refresh POP names from DaasControlAPIServiceV20100701 after " + numAttempts;
                    LOG.warn(msg);
                    throw new RuntimeException(msg);
                }
            }
        }
        
        return route53POPs;
    }
    
}
