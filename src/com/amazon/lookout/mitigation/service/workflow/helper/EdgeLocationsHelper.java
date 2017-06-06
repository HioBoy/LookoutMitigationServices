package com.amazon.lookout.mitigation.service.workflow.helper;

import java.beans.ConstructorProperties;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;

import com.amazon.edge.service.GetPOPsRequest;
import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.io.FileUtils;
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
 * This class also reads the list of POPs and their corresponding BW/non-BW flag from disk on startup, to accommodate for any failures to communicate
 * with any Route53/EdgeOperatorService APIs - allowing the MitigationService to continue working with the last known list of POPs.
 * In accordance with this, this helper also flushes the current list of POPs and their BW/non-BW status to disk when it is about to shutdown.
 * 
 */
@ThreadSafe
public class EdgeLocationsHelper implements Runnable {
    private static final Log LOG = LogFactory.getLog(EdgeLocationsHelper.class);
    
    // Configurations for retrying on failures during refresh.
    private static final int MAX_POP_NAMES_REFRESH_ATTEMPTS = 3;

    private static final String ROUTE53_POP_NAMES_REFRESH_METRIC = "Route53Refresh";
    private static final String ROUTE53_POP_NAMES_REFRESH_FAILED_METRIC = "Route53RefreshFailed";
    private static final String CLOUDFRONT_POP_NAMES_REFRESH_METRIC = "EdgeServicesRefresh";
    private static final String CLOUDFRONT_POP_NAMES_REFRESH_FAILED_METRIC = "EdgeServicesRefreshFailed";

    public static final String POPS_LIST_FILE_NAME = "popsList";
    private static final String FILE_NEW_SUFFIX = ".new";
    public static final String POPS_LIST_FILE_COMMENTS_KEY = "#";
    public static final String POPS_LIST_FILE_CHARSET = "UTF-8";
    
    private static final String POPS_LIST_READ_SUCCESSFULLY_KEY = "PopsListReadSuccessfully";
    private static final String POPS_LIST_FLUSHED_SUCCESSFULLY_KEY = "PopsListFlushedSuccessfully";
    
    // Maintain a flag that indicates that the refresh of POP names and the checks if they are Blackwatch capable have been completed at least once.
    private final AtomicBoolean locationsRefreshAtleastOnce = new AtomicBoolean(false);
    
    // Maintain a copy of all classic (non-Metro) POPs known to this locations helper.
    private final CopyOnWriteArraySet<String> allClassicPOPs = new CopyOnWriteArraySet<>();
    
    private final EdgeOperatorServiceClient cloudfrontClient;
    private final DaasControlAPIServiceV20100701Client daasClient;
    private final int sleepBetweenRetriesInMillis;
    private final String popsListDiskCacheDirName;
    private final MetricsFactory metricsFactory;
    private final List<String> fakeBlackWatchClassicLocations;

    @ConstructorProperties({"cloudfrontClient", "daasClient", "millisToSleepBetweenRetries", "popsListDir", "metricsFactory", "fakeBlackWatchClassicLocations"})
    public EdgeLocationsHelper(@NonNull EdgeOperatorServiceClient cloudfrontClient, @NonNull DaasControlAPIServiceV20100701Client daasClient,
                               int sleepBetweenRetriesInMillis, 
                               @NonNull String popsListDiskCacheDirName, @NonNull MetricsFactory metricsFactory,
                               @NonNull List<String> fakeBlackWatchClassicLocations) {
        this.cloudfrontClient = cloudfrontClient;
        this.daasClient = daasClient;
        
        Validate.isTrue(sleepBetweenRetriesInMillis > 0);
        this.sleepBetweenRetriesInMillis = sleepBetweenRetriesInMillis;
        
        Validate.notEmpty(popsListDiskCacheDirName);
        this.popsListDiskCacheDirName = popsListDiskCacheDirName;
        
        this.metricsFactory = metricsFactory;
        
        loadPOPsFromDiskOnStartup(popsListDiskCacheDirName);
        refreshPOPLocations();
        
        this.fakeBlackWatchClassicLocations = fakeBlackWatchClassicLocations;
    }
    
    public Set<String> getAllClassicPOPs() {
        if (!locationsRefreshAtleastOnce.get()) {
            refreshPOPLocations();
        }

        // Return allClassicPOPs based on the current view. Even if locationsRefreshAtleastOnce isn't true, we might have partial results 
        // (from either of the edge service calls succeeding) hence, there is no need to fail the create request. Users will have visibility into the instances being worked upon.
        return Stream.concat(allClassicPOPs.stream(), fakeBlackWatchClassicLocations.stream())
                .collect(Collectors.toSet());
    }

    public void run() {
        refreshPOPLocations();
    }
    
    /**
     * Refreshes the list of all classic pops.
     * 
     * We also add a counter for CloudFront/Route53 with a value of 1 if the API calls to their respective customer API for refreshing the POP names succeed, 
     * else we populate a 0 - this will help us to see the trend of how often we fail/succeed the refresh the POP names from these services.
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
                metrics.addOne(CLOUDFRONT_POP_NAMES_REFRESH_METRIC);
                metrics.addZero(CLOUDFRONT_POP_NAMES_REFRESH_FAILED_METRIC);
            } catch (Exception ex) {
                allActionsSuccessful = false;
                metrics.addZero(CLOUDFRONT_POP_NAMES_REFRESH_METRIC);
                metrics.addOne(CLOUDFRONT_POP_NAMES_REFRESH_FAILED_METRIC);
                
                String msg = "Caught exception when getting back CloudFrontPOPs, not updating any CloudFrontPOPs at this point";
                LOG.warn(msg, ex);
            }
            
            // Also check with the Route53 API to get the list of all POPs known to it, incase some POPs don't have CloudFront, but only Route53. Note, we only add any new POPs, 
            // but not remove any POPs from the existing set. We do so to err on the side of us trying to deploy mitigation to a location that no longer exists than to miss out 
            // on a valid POP due to some bug in the edge customer APIs. 
            try {
                refreshedPOPsList.addAll(getRoute53POPs());
                metrics.addOne(ROUTE53_POP_NAMES_REFRESH_METRIC);
                metrics.addZero(ROUTE53_POP_NAMES_REFRESH_FAILED_METRIC);
            } catch (Exception ex) {
                allActionsSuccessful = false;
                metrics.addZero(ROUTE53_POP_NAMES_REFRESH_METRIC);
                metrics.addOne(ROUTE53_POP_NAMES_REFRESH_FAILED_METRIC);
                
                String msg = "Caught exception when getting back Route53POPs, not updating any Route53POPs at this point";
                LOG.warn(msg, ex);
            }
            
            // If the list of all pops has changed, then update.
            if (!refreshedPOPsList.equals(allClassicPOPs)) {
                allClassicPOPs.addAll(refreshedPOPsList);
                
                // If all actions are successful above, then we have a complete list of POPs from Route53 and CloudFront, hence we only retain the list of 
                // POPs obtained from the respective calls, removing all other POPs which might not be relevant to either services anymore.
                if (allActionsSuccessful) {
                    allClassicPOPs.retainAll(refreshedPOPsList);
                }
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
        while (cloudfrontPOPs.isEmpty() && numAttempts++ < MAX_POP_NAMES_REFRESH_ATTEMPTS) {
            try {
                GetPOPsRequest getPOPsRequest = new GetPOPsRequest();
                getPOPsRequest.setType("pop");
                GetPOPsResult cfGetPOPsResult = getPOPsCall.call(getPOPsRequest);
                LOG.info("List of CloudFront POPs received: " + cfGetPOPsResult.getPOPList() + " after refreshing with EdgeOperatorService.");

                cloudfrontPOPs.addAll(cfGetPOPsResult.getPOPList());
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
    protected Set<String> getRoute53POPs() {
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
    
    /**
     * Helper to read the list of POPs and their isBW/non-BW status from a file on disk.
     * The file is expected to have each line define the POP name and a comma separated value of true/false indicating whether the POP has BW or not. (Eg line: ARN1,true)
     * The file could also have comments if the line starts with the POPS_LIST_FILE_COMMENTS_KEY character.
     * @param popsListDiskCacheDir Directory name which contains the POPS_LIST_FILE_NAME file containing the list of POPs and their isBW/non-BW status.
     */
    protected void loadPOPsFromDiskOnStartup(@NonNull String popsListDiskCacheDir) {
        try (TSDMetrics tsdMetrics = new TSDMetrics(metricsFactory, "loadPOPsFromDiskOnStartup")) {
            tsdMetrics.addZero(POPS_LIST_READ_SUCCESSFULLY_KEY);
            String pathToListOfPopsOnDisk = popsListDiskCacheDir + "/" + POPS_LIST_FILE_NAME;
            
            File popsListFile = new File(pathToListOfPopsOnDisk);
            if (!popsListFile.exists()) {
                LOG.warn("File " + pathToListOfPopsOnDisk + " for list of POPs doesn't exists on disk.");
                return;
            }
            
            LOG.info("Loading list of POPs from file " + pathToListOfPopsOnDisk);
            
            Set<String> popsReadFromFile = new HashSet<>();

            List<List<String>> lines = null;
            try {
                lines = FileUtils.readFile(pathToListOfPopsOnDisk, ",", POPS_LIST_FILE_COMMENTS_KEY, POPS_LIST_FILE_CHARSET);
            } catch (IOException ex) {
                LOG.error("Unable to read the file: " + pathToListOfPopsOnDisk + " containing list of POPs on disk.", ex);
                return;
            }
            
            for (List<String> line : lines) {
                if (line.size() < 1) {
                    LOG.error("[UNEXPECTED_POPS_LIST_FILE_ENTRY] Skipping line with more than 1 entry: " + line);
                    continue;
                }
                
                String popName = line.get(0).trim();
                popsReadFromFile.add(popName);
            }
            
            allClassicPOPs.addAll(popsReadFromFile);
            tsdMetrics.addOne(POPS_LIST_READ_SUCCESSFULLY_KEY);
        }
    }
    
    /**
     * Flushes the current set of BW POPs this helper knows about.
     */
    @PreDestroy
    public void flushCurrentListOfPOPsToDisk() {
        try (TSDMetrics tsdMetrics = new TSDMetrics(metricsFactory, "flushCurrentListOfPOPsToDisk")) {
            tsdMetrics.addZero(POPS_LIST_FLUSHED_SUCCESSFULLY_KEY);
            LOG.info("Saving list of POPs: " + allClassicPOPs + " to disk.");
    
            File popsListDiskCacheDir = new File(popsListDiskCacheDirName);
            if (!popsListDiskCacheDir.exists() && !popsListDiskCacheDir.mkdirs()) {
                LOG.warn("Unable to make new directory for storing current list of pops at: " + popsListDiskCacheDir.getAbsolutePath());
                return;
            }
            
            // If there is already a .new file, delete it and create a new one
            File newFile = new File(popsListDiskCacheDir, POPS_LIST_FILE_NAME + FILE_NEW_SUFFIX);
            if (newFile.exists()) {
                boolean deletedSuccessfully = newFile.delete();
                if (!deletedSuccessfully) {
                    LOG.error("Unable to delete file " + newFile.getAbsolutePath());
                    return;
                }

                try {
                    newFile.createNewFile();
                } catch (IOException e) {
                    LOG.error("Unable to create new file for storing current list of pops at: " + newFile.getAbsolutePath());
                    return;
                }
            }
    
            BufferedWriter writer = null;
            try {
                writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newFile), StandardCharsets.UTF_8));
                for (String popName : allClassicPOPs) {
                    writer.write(popName + "\n");
                }
            } catch (IOException e) {
                LOG.error("Unable to write file for pops list ids at: " + newFile.getAbsolutePath(), e);
                return;
            } finally {
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (IOException ignored) {}
                }
            }
    
            // move with replace current file
            File currentFile = new File(popsListDiskCacheDir, POPS_LIST_FILE_NAME);
            try {
                Files.move(newFile.toPath(), currentFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
                tsdMetrics.addOne(POPS_LIST_FLUSHED_SUCCESSFULLY_KEY);
            } catch (IOException ex) {
                LOG.warn("Failed renaming pops list file " + newFile.getAbsolutePath() + " to file " + currentFile.getAbsolutePath(), ex);
            }
        }
    }
}

