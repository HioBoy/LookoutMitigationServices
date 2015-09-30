package com.amazon.lookout.mitigation.service.workflow.helper;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.joda.time.DateTime;

import com.amazon.coral.google.common.collect.Sets;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.daas.control.DaasControlAPIServiceV20100701Client;
import com.amazon.edge.service.EdgeOperatorServiceClient;
import com.amazon.lookout.mitigation.router.FakeDevice;
import com.amazon.lookout.mitigation.router.FakeRouterConstants;
import com.amazon.lookoutfakerouterservice.RouterMetaDataStruct;

import lombok.NonNull;

import javax.annotation.Nonnull;

/**
 * Locations helper to interact with the Fake Router Service. Will return lists
 * of fake routers that exist in the service
 * 
 * @author bulgarec
 * 
 */
public class FakeLocationsHelper extends EdgeLocationsHelper {

    private static final int POP_LIST_EXPIRATION_SECONDS = 1;
    private volatile PopLists popLists = null;
    //set time of last update to an expired timestamp
    private volatile DateTime timeOfLastUpdate = new DateTime().minus(POP_LIST_EXPIRATION_SECONDS * 2);

    
    @ConstructorProperties({ "cloudfrontClient", "daasClient", "bwLocationsHelper", "millisToSleepBetweenRetries",
            "popsListDir", "metricsFactory", "fakeBlackWatchClassicLocations" })
    public FakeLocationsHelper(@Nonnull EdgeOperatorServiceClient cloudfrontClient, @Nonnull DaasControlAPIServiceV20100701Client daasClient,
            @Nonnull BlackwatchLocationsHelper bwLocationsHelper, int sleepBetweenRetriesInMillis, @Nonnull String popsListDiskCacheDirName,
            @Nonnull MetricsFactory metricsFactory, @Nonnull List<String> fakeBlackWatchClassicLocations) {
        super(cloudfrontClient, daasClient, bwLocationsHelper, sleepBetweenRetriesInMillis, popsListDiskCacheDirName,
                metricsFactory, fakeBlackWatchClassicLocations);
        refreshFakeRouters();
    }
    @Override
    public Set<String> getAllClassicPOPs() {
        updateListsIfNeeded();
        return popLists.getClassicPOPs();
    }
    
    @Override
    public Set<String> getBlackwatchClassicPOPs() {
        updateListsIfNeeded();
        return popLists.getBlackwatchClassicPOPs();
    }
    
    @Override
    public Set<String> getAllNonBlackwatchClassicPOPs() {
        updateListsIfNeeded();
        return popLists.getAllNonBlackwatchClassicPOPs();
    }
    
    @Override
    protected Set<String> getRoute53POPs() {
        return getAllClassicPOPs();
    }
    
    @Override
    protected Set<String> getCloudFrontClassicPOPs() {
        return getAllClassicPOPs();
    }

    @Override
    public void run() {
        updateListsIfNeeded();
    }

    private void updateListsIfNeeded() {
        if(timeOfLastUpdate.plusSeconds(POP_LIST_EXPIRATION_SECONDS).isBeforeNow()) {
            refreshFakeRouters();
        }
    }
    
    private synchronized void refreshFakeRouters() {
        if(!timeOfLastUpdate.plusSeconds(POP_LIST_EXPIRATION_SECONDS).isBeforeNow()) {
            //another thread already updated the lists
            return;
        }
        List<RouterMetaDataStruct> routerData = FakeDevice.listAllRouterMetaData();
        List<String> currentClassicPOPs = new ArrayList<String>();
        List<String> currentBlackwatchClassicPOPs = new ArrayList<String>();
        for (RouterMetaDataStruct router : routerData) {
            String routerName = router.getName();
            if (routerName.contains(FakeRouterConstants.FAKE_ROUTER_SUFFIX)) {
                String popName = routerName.split(FakeRouterConstants.FAKE_ROUTER_SUFFIX)[0];
                currentClassicPOPs.add(popName);
                if (router.isBlackwatch()) {
                    currentBlackwatchClassicPOPs.add(popName);
                }
            }
        }
        popLists = new PopLists(currentClassicPOPs, currentBlackwatchClassicPOPs);
        timeOfLastUpdate = new DateTime();
    }
    
    /**
     * The fake router lists are very dynamic and should never be read from
     * disk. This method is overwritten to ignore all calls to read from disk.
     */
    @Override
    protected void loadPOPsFromDiskOnStartup(@NonNull String popsListDiskCacheDir) {

    }

    /**
     * The fake router lists are very dynamic and should never be written to
     * disk. This method is overwritten to ignore all calls to write to disk.
     */
    @Override
    public void flushCurrentListOfPOPsToDisk() {

    }

    private static class PopLists {
        private final HashSet<String> allClassicPOPs;
        private final HashSet<String> blackwatchClassicPOPs;
        public PopLists(Collection<String> classicPOPs, Collection<String> blackwatchClassicPOPs) {
            this.allClassicPOPs = new HashSet<String>(classicPOPs);
            this.blackwatchClassicPOPs = new HashSet<String>(blackwatchClassicPOPs);
        }
        public Set<String> getClassicPOPs() {
            return allClassicPOPs;
        }
        public Set<String> getBlackwatchClassicPOPs() {
            return blackwatchClassicPOPs;
        }
        public Set<String> getAllNonBlackwatchClassicPOPs() {
            return Sets.difference(allClassicPOPs,blackwatchClassicPOPs);
        }
    }
}
