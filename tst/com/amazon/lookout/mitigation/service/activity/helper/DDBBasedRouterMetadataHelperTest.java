package com.amazon.lookout.mitigation.service.activity.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.router.model.RouterFilterInfoWithMetadata;
import com.amazon.lookout.mitigation.service.ActionType;
import com.amazon.lookout.mitigation.service.CompositeAndConstraint;
import com.amazon.lookout.mitigation.service.CompositeOrConstraint;
import com.amazon.lookout.mitigation.service.Constraint;
import com.amazon.lookout.mitigation.service.CountAction;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatuses;
import com.amazon.lookout.mitigation.service.RateLimitAction;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.ListActiveMitigationsForServiceActivity;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.service.router.helper.RouterFilterInfoDeserializer;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class DDBBasedRouterMetadataHelperTest {
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("E MMMM dd HH:mm:ss z yyyy");
    
    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    @Test
    public void testGetDestSubnetsFromSimpleConstraint() {
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        
        List<String> subnets = Lists.newArrayList("205.251.200.1", "205.251.200.2", "205.251.200.0/24");
        constraint.setAttributeValues(subnets);
        
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        DDBBasedRouterMetadataHelper helper = new DDBBasedRouterMetadataHelper(dynamoDBClient, "test", serviceSubnetsMatcher);
        
        List<String> destSubnets = helper.getDestSubnetsFromRouterMitigationConstraint(constraint);
        assertEquals(destSubnets.size(), 3);
        assertEquals(destSubnets, subnets);
    }
    
    @Test
    public void testGetDestSubnetsFromSimpleConstraintWithNoDestSubnets() {
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName(PacketAttributesEnumMapping.SOURCE_IP.name());
        
        List<String> subnets = Lists.newArrayList("205.251.200.1", "205.251.200.2", "205.251.200.0/24");
        constraint.setAttributeValues(subnets);
        
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        DDBBasedRouterMetadataHelper helper = new DDBBasedRouterMetadataHelper(dynamoDBClient, "test", serviceSubnetsMatcher);
        
        List<String> destSubnets = helper.getDestSubnetsFromRouterMitigationConstraint(constraint);
        assertEquals(destSubnets.size(), 0);
    }
    
    @Test
    public void testGetDestSubnetsFromCompositeAndConstraint() {
        List<Constraint> constraints = new ArrayList<Constraint>();
        
        SimpleConstraint constraint1 = new SimpleConstraint();
        constraint1.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        
        List<String> subnets = Lists.newArrayList("205.251.200.1", "205.251.200.2", "205.251.200.0/24");
        constraint1.setAttributeValues(subnets);
        constraints.add(constraint1);
        
        SimpleConstraint constraint2 = new SimpleConstraint();
        constraint2.setAttributeName(PacketAttributesEnumMapping.SOURCE_IP.name());
        
        constraint2.setAttributeValues(Lists.newArrayList("1.2.2.3"));
        constraints.add(constraint2);
        
        CompositeAndConstraint compositeConstraint = new CompositeAndConstraint();
        compositeConstraint.setConstraints(constraints);
        
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        DDBBasedRouterMetadataHelper helper = new DDBBasedRouterMetadataHelper(dynamoDBClient, "test", serviceSubnetsMatcher);
        
        List<String> destSubnets = helper.getDestSubnetsFromRouterMitigationConstraint(constraint1);
        assertEquals(destSubnets.size(), 3);
        assertEquals(destSubnets, subnets);
    }
    
    @Test
    public void testGetDestSubnetsFromCompositeAndConstraintWithNoDestSubnets() {
        List<Constraint> constraints = new ArrayList<Constraint>();
        
        SimpleConstraint constraint1 = new SimpleConstraint();
        constraint1.setAttributeName(PacketAttributesEnumMapping.TOTAL_PACKET_LENGTH.name());
        
        List<String> subnets = Lists.newArrayList("5", "10", "15");
        constraint1.setAttributeValues(subnets);
        constraints.add(constraint1);
        
        SimpleConstraint constraint2 = new SimpleConstraint();
        constraint2.setAttributeName(PacketAttributesEnumMapping.SOURCE_IP.name());
        
        constraint2.setAttributeValues(Lists.newArrayList("1.2.2.3"));
        constraints.add(constraint2);
        
        CompositeAndConstraint compositeConstraint1 = new CompositeAndConstraint();
        compositeConstraint1.setConstraints(constraints);
        
        SimpleConstraint constraint3 = new SimpleConstraint();
        constraint3.setAttributeName(PacketAttributesEnumMapping.SOURCE_ASN.name());
        constraint3.setAttributeValues(Lists.newArrayList("1234"));
        
        constraints = new ArrayList<Constraint>();
        constraints.add(compositeConstraint1);
        constraints.add(constraint3);
        CompositeAndConstraint compositeConstraint = new CompositeAndConstraint();
        compositeConstraint.setConstraints(constraints);
        
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        DDBBasedRouterMetadataHelper helper = new DDBBasedRouterMetadataHelper(dynamoDBClient, "test", serviceSubnetsMatcher);
        
        List<String> destSubnets = helper.getDestSubnetsFromRouterMitigationConstraint(constraint1);
        assertEquals(destSubnets.size(), 0);
    }
    
    @Test
    public void testGetDestSubnetsFromCompositeOrConstraint() {
        List<Constraint> constraints = new ArrayList<Constraint>();
        
        SimpleConstraint constraint1 = new SimpleConstraint();
        constraint1.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        
        List<String> subnets = Lists.newArrayList("205.251.200.1", "205.251.200.2", "205.251.200.0/24");
        constraint1.setAttributeValues(subnets);
        constraints.add(constraint1);
        
        SimpleConstraint constraint2 = new SimpleConstraint();
        constraint2.setAttributeName(PacketAttributesEnumMapping.SOURCE_IP.name());
        
        constraint2.setAttributeValues(Lists.newArrayList("1.2.2.3"));
        constraints.add(constraint2);
        
        CompositeOrConstraint compositeConstraint = new CompositeOrConstraint();
        compositeConstraint.setConstraints(constraints);
        
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        DDBBasedRouterMetadataHelper helper = new DDBBasedRouterMetadataHelper(dynamoDBClient, "test", serviceSubnetsMatcher);
        
        List<String> destSubnets = helper.getDestSubnetsFromRouterMitigationConstraint(constraint1);
        assertEquals(destSubnets.size(), 3);
        assertEquals(destSubnets, subnets);
    }
    
    @Test
    public void testConvertToMitigationRequestDescription() throws JsonParseException, JsonMappingException, IOException {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        when(serviceSubnetsMatcher.getAllServicesForSubnets(Lists.newArrayList("205.251.200.5", "205.251.200.7"))).thenReturn(Sets.newHashSet("Route53", "CloudFront"));
        DDBBasedRouterMetadataHelper helper = new DDBBasedRouterMetadataHelper(dynamoDBClient, "test", serviceSubnetsMatcher);
        
        String filterInfoAsJSON = "{\"filterName\":\"NTP Amplification Drop\",\"description\":\"Test filter\",\"srcIps\":[],\"destIps\":[\"205.251.200.5\",\"205.251.200.7\"]," +
                                    "\"srcASNs\":[],\"srcCountryCodes\":[],\"protocols\":[17],\"synOnly\":false,\"action\":\"RATE_LIMIT\",\"bandwidthKBps\":1300,\"burstSizeK\":15," +
                                    "\"enabled\":true,\"jobId\":0,\"lastDatePushedToRouter\":\"Fri Sep 26 11:21:09 PDT 2014\",\"lastUserToPush\":\"testUser\"," +
                                    "\"customerRateLimit\":{\"customer\":\"Route53\",\"rateLimitInKiloBytesPerSecond\":1300,\"estimatedRPS\":20000,\"averageRequestSize\":65}," +
                                    "\"customerSubnet\":\"\",\"metadata\":{},\"modified\":true,\"new\":true,\"policerBandwidthValueLocked\":false,\"sourcePort\":[\"53\"]," +
                                    "\"destinationPort\":[],\"packetLength\":[],\"ttl\":[]}";
        
        MitigationRequestDescriptionWithStatuses requestDescWithStatuses = helper.convertToMitigationRequestDescription(filterInfoAsJSON, "tst1-en-tra-r1");
        
        assertNotNull(requestDescWithStatuses);
        assertEquals(requestDescWithStatuses.getInstancesStatusMap().size(), 1);
        assertTrue(requestDescWithStatuses.getInstancesStatusMap().containsKey("TST1"));
        assertEquals(requestDescWithStatuses.getInstancesStatusMap().get("TST1").getLocation(), "TST1");
        assertEquals(requestDescWithStatuses.getInstancesStatusMap().get("TST1").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        
        MitigationRequestDescription requestDesc = requestDescWithStatuses.getMitigationRequestDescription();
        assertNotNull(requestDesc);
        
        assertEquals(requestDesc.getDeviceName(), DeviceName.POP_ROUTER.name());
        assertEquals(requestDesc.getDeviceScope(), DeviceScope.GLOBAL.name());
        assertEquals(requestDesc.getJobId(), 0);
        assertEquals(requestDesc.getMitigationActionMetadata().getDescription(), "Test filter");
        assertNull(requestDesc.getMitigationActionMetadata().getRelatedTickets());
        assertEquals(requestDesc.getMitigationActionMetadata().getToolName(), DDBBasedRouterMetadataHelper.ROUTER_MITIGATION_UI);
        assertEquals(requestDesc.getMitigationActionMetadata().getUser(), "testUser");
        
        ObjectMapper mapper = new ObjectMapper();
        RouterFilterInfoWithMetadata filterInfo = mapper.readValue(filterInfoAsJSON, RouterFilterInfoWithMetadata.class);
        assertEquals(requestDesc.getMitigationDefinition(), RouterFilterInfoDeserializer.convertToMitigationDefinition(filterInfo));
        assertEquals(requestDesc.getMitigationName(), "NTP Amplification Drop");
        
        long ntpMitigationRequestDateInMillis = formatter.parseMillis("Fri Sep 26 11:21:09 PDT 2014");
        assertEquals(requestDesc.getRequestDate(), ntpMitigationRequestDateInMillis);
    }
    
    @Test
    public void testMergeMitigations() throws JsonParseException, JsonMappingException, IOException {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        when(serviceSubnetsMatcher.getAllServicesForSubnets(Lists.newArrayList("205.251.200.5", "205.251.200.7"))).thenReturn(Sets.newHashSet("Route53", "CloudFront"));
        when(serviceSubnetsMatcher.getAllServicesForSubnets(Lists.newArrayList("216.137.51.0/24"))).thenReturn(Sets.newHashSet("CloudFront"));
        DDBBasedRouterMetadataHelper helper = new DDBBasedRouterMetadataHelper(dynamoDBClient, "test", serviceSubnetsMatcher);
        
        String filterInfoAsJSON = "{\"filterName\":\"NTP Amplification Drop\",\"description\":\"Test filter\",\"srcIps\":[],\"destIps\":[\"205.251.200.5\",\"205.251.200.7\"]," +
                                    "\"srcASNs\":[],\"srcCountryCodes\":[],\"protocols\":[17],\"synOnly\":false,\"action\":\"RATE_LIMIT\",\"bandwidthKBps\":1300,\"burstSizeK\":15," +
                                    "\"enabled\":true,\"jobId\":0,\"lastDatePushedToRouter\":\"Fri Sep 21 11:21:09 PDT 2014\",\"lastUserToPush\":\"testUser\"," +
                                    "\"customerRateLimit\":{\"customer\":\"Route53\",\"rateLimitInKiloBytesPerSecond\":1300,\"estimatedRPS\":20000,\"averageRequestSize\":65}," +
                                    "\"customerSubnet\":\"\",\"metadata\":{},\"modified\":true,\"new\":true,\"policerBandwidthValueLocked\":false,\"sourcePort\":[\"53\"]," +
                                    "\"destinationPort\":[],\"packetLength\":[],\"ttl\":[]}";
        
        MitigationRequestDescriptionWithStatuses route53Mitigation1Desc = helper.convertToMitigationRequestDescription(filterInfoAsJSON, "tst2-en-tra-r1");
        
        filterInfoAsJSON = "{\"filterName\":\"NTP Amplification Drop\",\"description\":\"Test filter\",\"srcIps\":[],\"destIps\":[\"216.137.51.0/24\"]," +
                           "\"srcASNs\":[],\"srcCountryCodes\":[],\"protocols\":[17],\"synOnly\":false,\"action\":\"RATE_LIMIT\",\"bandwidthKBps\":1300,\"burstSizeK\":15," +
                           "\"enabled\":true,\"jobId\":0,\"lastDatePushedToRouter\":\"Fri Sep 21 11:21:09 PDT 2014\",\"lastUserToPush\":\"testUser\"," +
                           "\"customerRateLimit\":{\"customer\":\"Route53\",\"rateLimitInKiloBytesPerSecond\":1300,\"estimatedRPS\":20000,\"averageRequestSize\":65}," +
                           "\"customerSubnet\":\"\",\"metadata\":{},\"modified\":true,\"new\":true,\"policerBandwidthValueLocked\":false,\"sourcePort\":[\"53\"]," +
                           "\"destinationPort\":[],\"packetLength\":[],\"ttl\":[]}";
        MitigationRequestDescriptionWithStatuses cloudFrontMitigationDesc = helper.convertToMitigationRequestDescription(filterInfoAsJSON, "tst3-en-tra-r1");
        
        filterInfoAsJSON = "{\"filterName\":\"Testing1\",\"description\":\"Test filter\",\"srcIps\":[],\"destIps\":[\"205.251.200.5\",\"205.251.200.7\"]," +
                           "\"srcASNs\":[],\"srcCountryCodes\":[],\"protocols\":[17],\"synOnly\":false,\"action\":\"RATE_LIMIT\",\"bandwidthKBps\":500,\"burstSizeK\":15," +
                           "\"enabled\":true,\"jobId\":5,\"lastDatePushedToRouter\":\"Fri Sep 22 11:21:09 PDT 2014\",\"lastUserToPush\":\"testUser\"," +
                           "\"customerRateLimit\":{\"customer\":\"Route53\",\"rateLimitInKiloBytesPerSecond\":1300,\"estimatedRPS\":20000,\"averageRequestSize\":65}," +
                           "\"customerSubnet\":\"\",\"metadata\":{},\"modified\":true,\"new\":true,\"policerBandwidthValueLocked\":false,\"sourcePort\":[\"53\"]," +
                           "\"destinationPort\":[],\"packetLength\":[],\"ttl\":[]}";
        MitigationRequestDescriptionWithStatuses route53Mitigation2Desc = helper.convertToMitigationRequestDescription(filterInfoAsJSON, "tst1-en-tra-r1");
        
        filterInfoAsJSON = "{\"filterName\":\"NTP Amplification Drop\",\"description\":\"Test filter\",\"srcIps\":[],\"destIps\":[\"205.251.200.5\",\"205.251.200.7\"]," +
                           "\"srcASNs\":[],\"srcCountryCodes\":[],\"protocols\":[17],\"synOnly\":false,\"action\":\"RATE_LIMIT\",\"bandwidthKBps\":1300,\"burstSizeK\":15," +
                           "\"enabled\":true,\"jobId\":0,\"lastDatePushedToRouter\":\"Fri Sep 21 11:21:09 PDT 2014\",\"lastUserToPush\":\"testUser\"," +
                           "\"customerRateLimit\":{\"customer\":\"Route53\",\"rateLimitInKiloBytesPerSecond\":1300,\"estimatedRPS\":20000,\"averageRequestSize\":65}," +
                           "\"customerSubnet\":\"\",\"metadata\":{},\"modified\":true,\"new\":true,\"policerBandwidthValueLocked\":false,\"sourcePort\":[\"53\"]," +
                           "\"destinationPort\":[],\"packetLength\":[],\"ttl\":[]}";

        MitigationRequestDescriptionWithStatuses route53Mitigation3Desc = helper.convertToMitigationRequestDescription(filterInfoAsJSON, "tst4-en-tra-r1");
        
        List<MitigationRequestDescriptionWithStatuses> routerMitigations = Lists.newArrayList(route53Mitigation1Desc, cloudFrontMitigationDesc, route53Mitigation2Desc, route53Mitigation3Desc);
        
        filterInfoAsJSON = "{\"filterName\":\"Testing1\",\"description\":\"Test filter\",\"srcIps\":[],\"destIps\":[\"205.251.200.5\",\"205.251.200.7\"]," +
                   "\"srcASNs\":[],\"srcCountryCodes\":[],\"protocols\":[17],\"synOnly\":false,\"action\":\"RATE_LIMIT\",\"bandwidthKBps\":500,\"burstSizeK\":15," +
                   "\"enabled\":true,\"jobId\":5,\"lastDatePushedToRouter\":\"Fri Sep 22 11:21:09 PDT 2014\",\"lastUserToPush\":\"testUser\"," +
                   "\"customerRateLimit\":{\"customer\":\"Route53\",\"rateLimitInKiloBytesPerSecond\":1300,\"estimatedRPS\":20000,\"averageRequestSize\":65}," +
                   "\"customerSubnet\":\"\",\"metadata\":{},\"modified\":true,\"new\":true,\"policerBandwidthValueLocked\":false,\"sourcePort\":[\"53\"]," +
                   "\"destinationPort\":[],\"packetLength\":[],\"ttl\":[]}";
        MitigationRequestDescriptionWithStatuses route53MitigationInMitSvc = helper.convertToMitigationRequestDescription(filterInfoAsJSON, "tst2-en-tra-r1");
        
        Map<String, List<MitigationRequestDescriptionWithStatuses>> activeMitigationsFromMitSvc = new HashMap<>();
        activeMitigationsFromMitSvc.put(ListActiveMitigationsForServiceActivity.createDeviceAndMitigationNameKey(route53MitigationInMitSvc), Lists.newArrayList(route53MitigationInMitSvc));
        
        List<MitigationRequestDescriptionWithStatuses> mergedMitigations = helper.mergeMitigations(activeMitigationsFromMitSvc, routerMitigations, "Route53");
        assertEquals(mergedMitigations.size(), 2);
        
        MitigationRequestDescriptionWithStatuses mitigationDrivenByMitSvc = null;
        MitigationRequestDescriptionWithStatuses mitigationDrivenByRouterMitUI = null;
        for (MitigationRequestDescriptionWithStatuses mitigation : mergedMitigations) {
            if (mitigation.getMitigationRequestDescription().getMitigationName().equals("Testing1")) {
                mitigationDrivenByMitSvc = mitigation;
            } else {
                mitigationDrivenByRouterMitUI = mitigation;
            }
        }
        
        assertEquals(mitigationDrivenByMitSvc.getMitigationRequestDescription(), route53MitigationInMitSvc.getMitigationRequestDescription());
        assertEquals(mitigationDrivenByMitSvc.getInstancesStatusMap().size(), 2);
        assertTrue(mitigationDrivenByMitSvc.getInstancesStatusMap().containsKey("TST1"));
        assertEquals(mitigationDrivenByMitSvc.getInstancesStatusMap().get("TST1").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        assertTrue(mitigationDrivenByMitSvc.getInstancesStatusMap().containsKey("TST2"));
        assertEquals(mitigationDrivenByMitSvc.getInstancesStatusMap().get("TST2").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription().getMitigationTemplate(), DDBBasedRouterMetadataHelper.ROUTER_MITIGATION_DEFAULT_TEMPLATE);
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription().getMitigationVersion(), 1);
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription().getNumPostDeployChecks(), 0);
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription().getNumPreDeployChecks(), 0);
        
        long requestDateInMillis = formatter.parseMillis("Fri Sep 21 11:21:09 PDT 2014");
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription().getRequestDate(), requestDateInMillis);
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription().getRequestStatus(), WorkflowStatus.SUCCEEDED);
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription().getRequestType(), RequestType.CreateRequest.name());
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription().getServiceName(), "Route53");
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription().getUpdateJobId(), 0);
        
        route53Mitigation3Desc.getMitigationRequestDescription().setServiceName("Route53");
        route53Mitigation3Desc.getMitigationRequestDescription().setMitigationTemplate("None");
        route53Mitigation3Desc.getMitigationRequestDescription().setMitigationVersion(1);
        route53Mitigation3Desc.getMitigationRequestDescription().setUpdateJobId(0);
        route53Mitigation3Desc.getMitigationRequestDescription().setNumPreDeployChecks(0);
        route53Mitigation3Desc.getMitigationRequestDescription().setNumPostDeployChecks(0);
        route53Mitigation3Desc.getMitigationRequestDescription().setRequestStatus(WorkflowStatus.SUCCEEDED);
        route53Mitigation3Desc.getMitigationRequestDescription().setRequestType(RequestType.CreateRequest.name());
        
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription(), route53Mitigation3Desc.getMitigationRequestDescription());
        
        assertEquals(mitigationDrivenByRouterMitUI.getInstancesStatusMap().size(), 2);
        assertTrue(mitigationDrivenByRouterMitUI.getInstancesStatusMap().containsKey("TST2"));
        assertEquals(mitigationDrivenByRouterMitUI.getInstancesStatusMap().get("TST2").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        assertTrue(mitigationDrivenByRouterMitUI.getInstancesStatusMap().containsKey("TST4"));
        assertEquals(mitigationDrivenByRouterMitUI.getInstancesStatusMap().get("TST4").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
    }
    
    @Test
    public void testMergeMitigationsWithSameNameDiffDefinitions() throws JsonParseException, JsonMappingException, IOException {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        when(serviceSubnetsMatcher.getAllServicesForSubnets(Lists.newArrayList("205.251.200.5", "205.251.200.7"))).thenReturn(Sets.newHashSet("Route53", "CloudFront"));
        when(serviceSubnetsMatcher.getAllServicesForSubnets(Lists.newArrayList("216.137.51.0/24"))).thenReturn(Sets.newHashSet("CloudFront"));
        DDBBasedRouterMetadataHelper helper = new DDBBasedRouterMetadataHelper(dynamoDBClient, "test", serviceSubnetsMatcher);
        
        String filterInfoAsJSONBase = "{\"filterName\":\"Testing1\",\"description\":\"Test filter\",\"srcIps\":[],\"destIps\":[\"205.251.200.5\",\"205.251.200.7\"]," +
                                      "\"srcASNs\":[],\"srcCountryCodes\":[],\"protocols\":[17],\"synOnly\":false,\"action\":\"%s\",\"bandwidthKBps\":500,\"burstSizeK\":15," +
                                      "\"enabled\":true,\"jobId\":5,\"lastDatePushedToRouter\":\"Fri Sep 22 11:21:09 PDT 2014\",\"lastUserToPush\":\"testUser\"," +
                                      "\"customerRateLimit\":{\"customer\":\"Route53\",\"rateLimitInKiloBytesPerSecond\":1300,\"estimatedRPS\":20000,\"averageRequestSize\":65}," +
                                      "\"customerSubnet\":\"\",\"metadata\":{},\"modified\":true,\"new\":true,\"policerBandwidthValueLocked\":false,\"sourcePort\":[\"53\"]," +
                                      "\"destinationPort\":[],\"packetLength\":[],\"ttl\":[]}";
        String filterInfoAsJSON = String.format(filterInfoAsJSONBase, "COUNT");
        MitigationRequestDescriptionWithStatuses route53Mitigation1Desc = helper.convertToMitigationRequestDescription(filterInfoAsJSON, "tst1-en-tra-r1");
        
        List<MitigationRequestDescriptionWithStatuses> routerMitigations = Lists.newArrayList(route53Mitigation1Desc);
        
        filterInfoAsJSON = String.format(filterInfoAsJSONBase, "RATE_LIMIT");
        MitigationRequestDescriptionWithStatuses route53MitigationInMitSvc = helper.convertToMitigationRequestDescription(filterInfoAsJSON, "tst1-en-tra-r1");
        
        Map<String, List<MitigationRequestDescriptionWithStatuses>> activeMitigationsFromMitSvc = new HashMap<>();
        activeMitigationsFromMitSvc.put(ListActiveMitigationsForServiceActivity.createDeviceAndMitigationNameKey(route53MitigationInMitSvc), Lists.newArrayList(route53MitigationInMitSvc));
        
        List<MitigationRequestDescriptionWithStatuses> mergedMitigations = helper.mergeMitigations(activeMitigationsFromMitSvc, routerMitigations, "Route53");
        assertEquals(mergedMitigations.size(), 2);
        
        MitigationRequestDescriptionWithStatuses mitigationDrivenByMitSvc = null;
        MitigationRequestDescriptionWithStatuses mitigationDrivenByRouterMitUI = null;
        for (MitigationRequestDescriptionWithStatuses mitigation : mergedMitigations) {
            if (mitigation.getMitigationRequestDescription().getMitigationDefinition().getAction() instanceof RateLimitAction) {
                mitigationDrivenByMitSvc = mitigation;
            } else {
                mitigationDrivenByRouterMitUI = mitigation;
            }
        }
        
        assertEquals(mitigationDrivenByMitSvc.getMitigationRequestDescription(), route53MitigationInMitSvc.getMitigationRequestDescription());
        assertEquals(mitigationDrivenByMitSvc.getInstancesStatusMap().size(), 1);
        assertTrue(mitigationDrivenByMitSvc.getInstancesStatusMap().containsKey("TST1"));
        assertEquals(mitigationDrivenByMitSvc.getInstancesStatusMap().get("TST1").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription().getMitigationTemplate(), DDBBasedRouterMetadataHelper.ROUTER_MITIGATION_DEFAULT_TEMPLATE);
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription().getMitigationVersion(), 1);
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription().getNumPostDeployChecks(), 0);
        assertEquals(mitigationDrivenByRouterMitUI.getMitigationRequestDescription().getNumPreDeployChecks(), 0);
        
        assertEquals(mitigationDrivenByRouterMitUI.getInstancesStatusMap().size(), 1);
        assertTrue(mitigationDrivenByRouterMitUI.getInstancesStatusMap().containsKey("TST1"));
        assertEquals(mitigationDrivenByRouterMitUI.getInstancesStatusMap().get("TST1").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
    }
}
