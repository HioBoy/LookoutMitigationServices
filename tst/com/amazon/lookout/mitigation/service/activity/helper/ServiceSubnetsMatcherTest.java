package com.amazon.lookout.mitigation.service.activity.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.amazon.aws158.commons.customer.CustomerSubnetsFetcher;
import com.amazon.coral.google.common.collect.Lists;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;

public class ServiceSubnetsMatcherTest {
    
    /**
     * Test when the subnets to check fall within the range of a single service's subnets.
     */
    @Test
    public void testPositiveMatches() {
        CustomerSubnetsFetcher subnetsFetcher1 = mock(CustomerSubnetsFetcher.class);
        when(subnetsFetcher1.getAllSubnets()).thenReturn(Lists.newArrayList("205.251.200.0/24"));
        when(subnetsFetcher1.getCustomerName()).thenReturn(ServiceName.Route53);
        
        CustomerSubnetsFetcher subnetsFetcher2 = mock(CustomerSubnetsFetcher.class);
        when(subnetsFetcher2.getAllSubnets()).thenReturn(Lists.newArrayList("216.137.50.0/25", "216.137.51.0/24", "216.137.50.131/32"));
        when(subnetsFetcher2.getCustomerName()).thenReturn("CloudFront");
        
        ServiceSubnetsMatcher matcher = new ServiceSubnetsMatcher(Lists.newArrayList(subnetsFetcher1, subnetsFetcher2));

        String subnetToCheck = "216.137.51.128/26";
        String matchedService = matcher.getServiceForSubnet(subnetToCheck);
        assertNotNull(matchedService);
        assertEquals(matchedService, "CloudFront");

        subnetToCheck = "205.251.200.28";
        matchedService = matcher.getServiceForSubnet(subnetToCheck);
        assertNotNull(matchedService);
        assertEquals(matchedService, ServiceName.Route53);
    }

    /**
     * Test when the subnets to check fall outside the range of a service's subnet.
     */
    @Test
    public void testNegativeMatches() {
        CustomerSubnetsFetcher subnetsFetcher1 = mock(CustomerSubnetsFetcher.class);
        when(subnetsFetcher1.getAllSubnets()).thenReturn(Lists.newArrayList("205.251.200.0/24"));
        when(subnetsFetcher1.getCustomerName()).thenReturn(ServiceName.Route53);
        
        CustomerSubnetsFetcher subnetsFetcher2 = mock(CustomerSubnetsFetcher.class);
        when(subnetsFetcher2.getAllSubnets()).thenReturn(Lists.newArrayList("216.137.50.0/25", "216.137.51.0/24", "216.137.50.131/32"));
        when(subnetsFetcher2.getCustomerName()).thenReturn("CloudFront");
        
        ServiceSubnetsMatcher matcher = new ServiceSubnetsMatcher(Lists.newArrayList(subnetsFetcher1, subnetsFetcher2));

        String subnetToCheck = "216.137.52.0/25";
        String matchedService = matcher.getServiceForSubnet(subnetToCheck);
        assertNull(matchedService);

        subnetToCheck = "205.251.201.28";
        matchedService = matcher.getServiceForSubnet(subnetToCheck);
        assertNull(matchedService);
    }
    
    /**
     * Test when the subnets to check fall outside the range of a service's subnet, but have an overlap in large portions of their subnet.
     */
    @Test
    public void testNegativeMatchesWithOverlappingAddresses() {
        CustomerSubnetsFetcher subnetsFetcher1 = mock(CustomerSubnetsFetcher.class);
        when(subnetsFetcher1.getAllSubnets()).thenReturn(Lists.newArrayList("205.251.200.0/24"));
        when(subnetsFetcher1.getCustomerName()).thenReturn(ServiceName.Route53);
        
        CustomerSubnetsFetcher subnetsFetcher2 = mock(CustomerSubnetsFetcher.class);
        when(subnetsFetcher2.getAllSubnets()).thenReturn(Lists.newArrayList("216.137.50.0/25", "216.137.51.0/24", "216.137.50.131/32"));
        when(subnetsFetcher2.getCustomerName()).thenReturn("CloudFront");
        
        ServiceSubnetsMatcher matcher = new ServiceSubnetsMatcher(Lists.newArrayList(subnetsFetcher1, subnetsFetcher2));

        String subnetToCheck = "216.137.50.128/26";
        String matchedService = matcher.getServiceForSubnet(subnetToCheck);
        assertNull(matchedService);

        subnetToCheck = "172.22.20.0/24";
        matchedService = matcher.getServiceForSubnet(subnetToCheck);
        assertNull(matchedService);
    }

    /**
     * Test the case if 2 different Services have overlapping subnets, then the service with the more
     * generic subnet must match instead of the service with the more specific mask.
     */
    @Test
    public void testServiceWithOverlappingSubnets() {
        CustomerSubnetsFetcher subnetsFetcher1 = mock(CustomerSubnetsFetcher.class);
        when(subnetsFetcher1.getAllSubnets()).thenReturn(Lists.newArrayList("205.251.200.0/24"));
        when(subnetsFetcher1.getCustomerName()).thenReturn(ServiceName.Route53);
        
        CustomerSubnetsFetcher subnetsFetcher2 = mock(CustomerSubnetsFetcher.class);
        when(subnetsFetcher2.getAllSubnets()).thenReturn(Lists.newArrayList("205.251.200.0/26"));
        when(subnetsFetcher2.getCustomerName()).thenReturn("CloudFront");
        
        ServiceSubnetsMatcher matcher = new ServiceSubnetsMatcher(Lists.newArrayList(subnetsFetcher1, subnetsFetcher2));
        
        String subnetToCheck = "205.251.200.0";
        String matchedService = matcher.getServiceForSubnet(subnetToCheck);
        assertNotNull(matchedService);
        assertEquals(matchedService, ServiceName.Route53);
    }
}
