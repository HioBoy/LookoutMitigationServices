package com.amazon.lookout.mitigation.service.workflow.helper;

import java.beans.ConstructorProperties;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

import lombok.NonNull;

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
 * Helper that helps in identifying the POP locations for deployment. Currently (01/2015) this locations helper only exposes classic (non-Metro) POPs.
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
    
    // If any CF POP has a -M<some number> in its name, then it is a metro POP. Examples of class POP: SFO5, DFW3. Examples of metro POPs: SFO5-M1, SFO20-M2.
    // Pattern is thread-safe and hence using a static final instance.
    private static final Pattern CF_METRO_POP_PATTERN = Pattern.compile("^[A-Z0-9]+-M[1-9]+$");
    
    // Maintain a flag that indicates that the refresh of POP names and the checks if they are Blackwatch capable have been completed at least once.
    private final AtomicBoolean locationsRefreshAtleastOnce = new AtomicBoolean(false);
    
    // Maintain a copy of all classic (non-Metro) POPs known to this locations helper.
    private final CopyOnWriteArraySet<String> allClassicPOPs = new CopyOnWriteArraySet<>();
    
    // Maintain a copy of all classic POPs known to this locations helper which have Blackwatch hosts installed.
    private final CopyOnWriteArraySet<String> blackwatchClassicPOPs = new CopyOnWriteArraySet<>();
    
    private final EdgeOperatorServiceClient cloudfrontClient;
    private final DaasControlAPIServiceV20100701Client daasClient;
    private final BlackwatchLocationsHelper bwLocationsHelper;
    private final int sleepBetweenRetriesInMillis;
    private final MetricsFactory metricsFactory;

    @ConstructorProperties({"cloudfrontClient", "daasClient", "bwLocationsHelper", "millisToSleepBetweenRetries", "metricsFactory"})
    public EdgeLocationsHelper(@NonNull EdgeOperatorServiceClient cloudfrontClient, @NonNull DaasControlAPIServiceV20100701Client daasClient,
                               @NonNull BlackwatchLocationsHelper bwLocationsHelper, int sleepBetweenRetriesInMillis, @NonNull MetricsFactory metricsFactory) {
        this.cloudfrontClient = cloudfrontClient;
        this.daasClient = daasClient;
        this.bwLocationsHelper = bwLocationsHelper;
        
        Validate.isTrue(sleepBetweenRetriesInMillis > 0);
        this.sleepBetweenRetriesInMillis = sleepBetweenRetriesInMillis;
        
        this.metricsFactory = metricsFactory;
        
        refreshPOPLocations();
    }
    
    public Set<String> getAllClassicPOPs() {
        if (!locationsRefreshAtleastOnce.get()) {
            refreshPOPLocations();
        }

        // Return allClassicPOPs based on the current view. Even if locationsRefreshAtleastOnce isn't true, we might have partial results 
        // (from either of the edge service calls succeeding) hence, there is no need to fail the create request. Users will have visibility into the instances being worked upon.
        return allClassicPOPs;
    }
    
    public Set<String> getBlackwatchClassicPOPs() {
        if (!locationsRefreshAtleastOnce.get()) {
            refreshPOPLocations();
        }

        // Return blackwatchClassicPOPs based on the current view. Even if locationsRefreshAtleastOnce isn't true, we might have partial results 
        // (from either of the edge service calls succeeding) hence, there is no need to fail the create request. Users will have visibility into the instances being worked upon.
        return blackwatchClassicPOPs;
    }
    
    public Set<String> getAllNonBlackwatchClassicPOPs() {
        if (!locationsRefreshAtleastOnce.get()) {
            refreshPOPLocations();
        }
        
        // Return the different of allClassicPOPs and blackwatchClassicPOPs based on the current view. Even if locationsRefreshAtleastOnce isn't true, we might have partial results 
        // (from either of the edge service calls succeeding) hence, there is no need to fail the create request. Users will have visibility into the instances being worked upon.
        return (Sets.difference(allClassicPOPs, blackwatchClassicPOPs));
    }
    
    public void run() {
        refreshPOPLocations();
    }
    
    /**
     * Refreshes the list of all pops and pops with/without blackwatch. Currently (01/2015) skipping all the non-classic POPs and only keeping track of the classic (non-metro) POPs.
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
                refreshedPOPsList.addAll(getCloudFrontClassicPOPs());
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
            
            // If the list of all pops has changed, then update.
            if (!refreshedPOPsList.equals(allClassicPOPs)) {
                allClassicPOPs.addAll(refreshedPOPsList);
            }
            
            // Also refresh the list of BW POPs here.
            boolean allPOPsCheckedForBlackwatch = true;
            for (String popName : allClassicPOPs) {
                try {
                    if (bwLocationsHelper.isBlackwatchPOP(popName, metrics)) {
                        LOG.info("Adding POP: " + popName + " as a BW POP.");
                        blackwatchClassicPOPs.add(popName);
                    } else {
                        if (blackwatchClassicPOPs.contains(popName)) {
                            LOG.info("Marking POP: " + popName + " as non-BW, though it was previously marked as a BW POP.");
                            blackwatchClassicPOPs.remove(popName);
                        }
                    }
                } catch (Exception ex) {
                    allActionsSuccessful = false;
                    allPOPsCheckedForBlackwatch = false;
                    
                    String msg = "Caught exception when checking if pop: " + popName + " is Blackwatch capable, continuing checking the other POPs now." +
                                 " Current status for this POP having Blackwatch: " + blackwatchClassicPOPs.contains(popName) + ". Leaving this status as is.";
                    LOG.warn(msg, ex);
                }
            }
            
            if (allPOPsCheckedForBlackwatch) {
                metrics.addCount(BLACKWATCH_POP_CHECK_METRIC, 1);
            } else {
                metrics.addCount(BLACKWATCH_POP_CHECK_METRIC, 0);
            }
            
            if (allActionsSuccessful) {
                locationsRefreshAtleastOnce.set(true);
            }
        } finally {
            metrics.end();
        }
    }
    
    /**
     * Helper to get the current set of POPs known to the EdgeOperatorService. Protected for unit-tests.
     * @return Set<String> representing POP names known to the EdgeOperatorService.
     */
    protected Set<String> getCloudFrontClassicPOPs() {
        Set<String> cloudfrontPOPs = new HashSet<>();
        
        GetPOPsCall getPOPsCall = cloudfrontClient.newGetPOPsCall();
        
        int numAttempts = 0;
        while (numAttempts++ < MAX_POP_NAMES_REFRESH_ATTEMPTS) {
            try {
                GetPOPsResult cfGetPOPsResult = getPOPsCall.call();
                LOG.info("List of CloudFront POPs received: " + cfGetPOPsResult.getPOPList() + " after refreshing with EdgeOperatorService.");
                
                for (String popName : cfGetPOPsResult.getPOPList()) {
                    String popNameUpperCase = popName.toUpperCase().trim();
                    Matcher matcher = CF_METRO_POP_PATTERN.matcher(popNameUpperCase);
                    if (matcher.matches()) {
                        LOG.info("Skipping the pop: " + popName + " whose name represents a metro-POP satisfying the pattern: " + CF_METRO_POP_PATTERN.pattern());
                        continue;
                    }
                    cloudfrontPOPs.add(popNameUpperCase);
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
                    LOG.info("Received of Route53 POP: " + dnsServer.getPOP() + " after refreshing the listDNSServers with DaasControlAPIService.");
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
