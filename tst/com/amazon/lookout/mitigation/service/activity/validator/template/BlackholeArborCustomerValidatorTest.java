package com.amazon.lookout.mitigation.service.activity.validator.template;

import static com.amazon.lookout.test.common.util.AssertUtils.assertThrows;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.allOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.AlarmCheck;
import com.amazon.lookout.mitigation.service.ArborBlackholeConstraint;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.DropAction;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.BlackholeTestUtils;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.StandardLocations;
import com.amazon.lookout.mitigation.workers.helper.BlackholeMitigationHelper;
import com.amazon.lookout.test.common.util.TestUtils;
import com.google.common.collect.ImmutableList;

@RunWith(JUnitParamsRunner.class)
public class BlackholeArborCustomerValidatorTest {
    private static final String EXISTING_MITIGATION_NAME = "ExistingMitigation";
    
    private BlackholeMitigationHelper blackholeMitigationHelper;
    
    @BeforeClass
    public static void beforeClass() {
        TestUtils.configureLogging(Level.WARN);
    }
    
    @Before
    public void before() {
        blackholeMitigationHelper = BlackholeTestUtils.mockMitigationHelper();
    }
    
    @Test
    public void validRequest() throws Exception {
        validate(validCreateMitigationRequest());
        validate(validEditMitigationRequest());
        validate(validDeleteMitigationRequest());
    }
    
    @Test
    public void validSystestRequest() throws Exception {
        String testName = "SYSAUTO=LKT-TestName=" + UUID.randomUUID().toString();
        
        validate(withName(validCreateMitigationRequest(), testName));
        validate(withName(validEditMitigationRequest(), testName));
        validate(withName(validDeleteMitigationRequest(), testName));
    }
    
    @Test
    public void validDeleteRequestWithoutLKT() {
        String testName = "OldMitigationName";
        validate(withName(validDeleteMitigationRequest(), testName));
    }
    
    private static MitigationModificationRequest withName(MitigationModificationRequest request, String name) {
        request.setMitigationName(name);
        return request;
    }

    @Test
    public void invalidNotSupportedRequest() throws Exception {
        assertThat(
            validationMessage(
                notSupportedRequest(),
                request -> {}),
            containsString("not supported"));
    }

    @Test
    @Parameters({
        "",
        "LKT-Cannot contain both space and \"quote",
        "LKT-跑",
        "LKT-#",
        "MissingLKT-"
    })
    public void mitigationNameIsValidForCreateOrEdit(String invalidMitigationName) throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> request.setMitigationName(invalidMitigationName)),
            containsString("mitigationName"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> request.setMitigationName(invalidMitigationName)),
            containsString("mitigationName"));
    }
    
    // Delete allows mitigation names not starting with LKT- so we don't include the 
    // MissingLKT- value used for create and edit here
    @Test
    @Parameters({
        "",
        "LKT-Cannot contain both space and \"quote",
        "LKT-跑",
        "LKT-#",
    })
    public void mitigationNameIsValidForDelete(String invalidMitigationName) throws Exception {
        assertThat(
            validationMessage(
                validDeleteMitigationRequest(),
                request -> request.setMitigationName(invalidMitigationName)),
            containsString("mitigationName"));
    }

    @Test
    public void descriptionCanBeNotSpecified() {
        CreateMitigationRequest createRequest = validCreateMitigationRequest();
        createRequest.getMitigationActionMetadata().setDescription(null);
        validate(createRequest);


        EditMitigationRequest editRequest = validEditMitigationRequest();
        editRequest.getMitigationActionMetadata().setDescription(null);
        validate(editRequest);
    }

    @Test
    @Parameters({
        "Cannot contain both space and \"quote",
        "跑",
        "#"
    })
    public void descriptionIsValid(String invalidDescription) throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> request.getMitigationActionMetadata().setDescription(invalidDescription)),
            containsString("description"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> request.getMitigationActionMetadata().setDescription(invalidDescription)),
            containsString("description"));
    }

    @Test
    public void constraintIsSpecified() throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> request.getMitigationDefinition().setConstraint(null)),
            containsString("Constraint must not be null"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> request.getMitigationDefinition().setConstraint(null)),
            containsString("Constraint must not be null"));
    }

    @Test
    public void constraintHasValidType() throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> request.getMitigationDefinition().setConstraint(new SimpleConstraint())),
            containsString("ArborBlackholeConstraint"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> request.getMitigationDefinition().setConstraint(new SimpleConstraint())),
            containsString("ArborBlackholeConstraint"));
    }

    @Test
    public void ipIsSpecified() throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> constraint(request).setIp(null)),
            containsString("must not be blank"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> constraint(request).setIp(null)),
            containsString("must not be blank"));
    }
    
    @Test
    public void ipIsNotEmpty() throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> constraint(request).setIp("")),
            containsString("must not be blank"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> constraint(request).setIp("")),
            containsString("must not be blank"));
    }

    @Test
    @Parameters({
        "a",
        "幸"
    })
    public void ipIsValid(String invalidIp) throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> constraint(request).setIp(invalidIp)),
            containsString("not a valid CIDR"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> constraint(request).setIp(invalidIp)),
            containsString("not a valid CIDR"));
    }

    @Test
    @Parameters({
        "33",
        "3f"
    })
    public void netMaskIsValid(String invalidNetMask) throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> constraint(request).setIp(String.format("1.2.3.4/%s", invalidNetMask))),
            containsString(invalidNetMask));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> constraint(request).setIp(String.format("1.2.3.4/%s", invalidNetMask))),
            containsString(invalidNetMask));
    }

    @Test
    @Parameters({
        "16",
        "24",
        "31"
    })
    public void netMaskIs32(String invalidNetMask) throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> constraint(request).setIp(String.format("1.2.0.0/%s", invalidNetMask))),
            containsString("Blackholes can only work on /32s"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> constraint(request).setIp(String.format("1.2.0.0/%s", invalidNetMask))),
            containsString("Blackholes can only work on /32s"));
    }

    @Test
    @Parameters({
        "127.0.0.1/32", // loopback address
        "169.254.0.1/32" // link local address
    })
    public void specialIpsAreNotSupported(String invalidIp) throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> constraint(request).setIp(invalidIp)),
            containsString("not supported"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> constraint(request).setIp(invalidIp)),
            containsString("not supported"));
    }

    @Test
    public void ipV6IsNotSupported() throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> constraint(request).setIp("2001:4860:4860::8888/48")),
            containsString("is not a valid IP"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> constraint(request).setIp("2001:4860:4860::8888/48")),
            containsString("is not a valid IP"));
    }

    @Test
    public void ipIsInCidrFormat() throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> constraint(request).setIp("1.2.3.4")),
            containsString("not a valid CIDR"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> constraint(request).setIp("1.2.3.4")),
            containsString("not a valid CIDR"));
    }

    @Test
    public void noPreDeploymentChecks() throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> request.setPreDeploymentChecks(singletonList(new AlarmCheck()))),
            containsString("deployment checks"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> request.setPreDeploymentChecks(singletonList(new AlarmCheck()))),
            containsString("deployment checks"));
    }

    @Test
    public void noPostDeploymentChecks() throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> request.setPostDeploymentChecks(singletonList(new AlarmCheck()))),
            containsString("deployment checks"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> request.setPostDeploymentChecks(singletonList(new AlarmCheck()))),
            containsString("deployment checks"));
    }
    
    private static void setLocations(MitigationModificationRequest request, List<String> locations) {
        if (request instanceof CreateMitigationRequest) {
            ((CreateMitigationRequest) request).setLocations(locations);
        } else if (request instanceof EditMitigationRequest) {
            ((EditMitigationRequest) request).setLocation(locations);
        } else {
            throw new RuntimeException("Cannot set location for request " + request);
        }
    }
    
    @Test
    public void noLocations() throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> setLocations(request, Collections.singletonList(StandardLocations.ARBOR))),
            containsString("Expect no locations"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> setLocations(request, Collections.singletonList(StandardLocations.ARBOR))),
            containsString("Expect no locations"));
    }
    
    public Object[] validTransitProviderCombinations() {
        return new Object[] {
            ImmutableList.of(BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1),
            ImmutableList.of(
                    BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1, 
                    BlackholeTestUtils.VALID_UNSUPPORTED_TRANSIT_PROVIDER_ID),
            ImmutableList.of(
                    BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1, 
                    BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_2)
        };
    }
    
    @Parameters(method="validTransitProviderCombinations")
    @Test
    public void validTransitProvidersNoAdditionalCommunity(List<String> transitProviderIds) throws Exception {
        CreateMitigationRequest createRequest = validCreateMitigationRequest();
        constraint(createRequest).setTransitProviderIds(transitProviderIds);
        constraint(createRequest).setAdditionalCommunityString(null);
        validate(createRequest);
        
        EditMitigationRequest editRequest = validEditMitigationRequest();
        constraint(editRequest).setTransitProviderIds(transitProviderIds);
        constraint(createRequest).setAdditionalCommunityString(null);
        validate(editRequest);
    }
    
    @Parameters(method="validTransitProviderCombinations")
    @Test
    public void validTransitProvidersWithAdditionalCommunity(List<String> transitProviderIds) throws Exception {
        CreateMitigationRequest createRequest = validCreateMitigationRequest();
        constraint(createRequest).setTransitProviderIds(transitProviderIds);
        constraint(createRequest).setAdditionalCommunityString("16509:3 16509:20");
        validate(createRequest);
        
        EditMitigationRequest editRequest = validEditMitigationRequest();
        constraint(editRequest).setTransitProviderIds(transitProviderIds);
        constraint(createRequest).setAdditionalCommunityString("16509:3 16509:20");
        validate(editRequest);
    }
    
    @Parameters({
        BlackholeTestUtils.INVALID_TRANSIT_PROVIDER_ID + ", must be an url safe base64 encoded string", 
        BlackholeTestUtils.WELL_FORMATTED_BUT_INVALID_TRANSIT_PROVIDER_ID + ", Invalid transit provider id: " + 
                BlackholeTestUtils.WELL_FORMATTED_BUT_INVALID_TRANSIT_PROVIDER_ID})
    @Test
    public void invalidTransitProviders(String invalidTransitProviderId, String expected) throws Exception {
        assertThat(
                validationMessage(
                    validCreateMitigationRequest(),
                    request -> constraint(request).setTransitProviderIds(ImmutableList.of(invalidTransitProviderId))),
                containsString(expected));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> constraint(request).setTransitProviderIds(ImmutableList.of(invalidTransitProviderId))),
            containsString(expected));
    }
    
    @Test
    public void mismatchedTransitProviders() throws Exception {
        ImmutableList<String> mismatchedTransitProviders = ImmutableList.of(
                BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1, BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_12345_1);
        assertThat(
                validationMessage(
                    validCreateMitigationRequest(),
                    request -> constraint(request).setTransitProviderIds(mismatchedTransitProviders)),
                containsString("must share the same ASN"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> constraint(request).setTransitProviderIds(mismatchedTransitProviders)),
            containsString("must share the same ASN"));
    }
    
    @Test
    public void mismatchedTransitProvidersAndCommunityString() throws Exception {
        ImmutableList<String> transitProviders = ImmutableList.of(BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1);
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> {
                    constraint(request).setAdditionalCommunityString("1234:1 1234:10");
                    constraint(request).setTransitProviderIds(transitProviders);
                }),
            containsString("must share the same ASN"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> {
                    constraint(request).setAdditionalCommunityString("1234:1 1234:10");
                    constraint(request).setTransitProviderIds(transitProviders);
                }),
            containsString("must share the same ASN"));
    }
    
    @Test
    public void noTransitProviderOrCommunityString() throws Exception {
        assertThat(
                validationMessage(
                    validCreateMitigationRequest(),
                    request -> {
                        constraint(request).setTransitProviderIds(Collections.emptyList());
                        constraint(request).setAdditionalCommunityString("");
                    }),
                containsString("At least one transit provider or additionalCommunityString must be provided"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> {
                    constraint(request).setTransitProviderIds(Collections.emptyList());
                    constraint(request).setAdditionalCommunityString("");
                }),
            containsString("At least one transit provider or additionalCommunityString must be provided"));
    }
    
    @Test
    public void noActiveTransitProviderOrCommunityString() throws Exception {
        assertThat(
                validationMessage(
                    validCreateMitigationRequest(),
                    request -> {
                        constraint(request).setTransitProviderIds(ImmutableList.of(BlackholeTestUtils.VALID_UNSUPPORTED_TRANSIT_PROVIDER_ID));
                        constraint(request).setAdditionalCommunityString("");
                    }),
                containsString("None of the specified transit providers has a community string"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> {
                    constraint(request).setTransitProviderIds(ImmutableList.of(BlackholeTestUtils.VALID_UNSUPPORTED_TRANSIT_PROVIDER_ID));
                    constraint(request).setAdditionalCommunityString("");
                }),
            containsString("None of the specified transit providers has a community string"));
    }
    
    @Test
    public void invalidAdditionalCommunity() throws Exception {
        assertThat(
                validationMessage(
                    validCreateMitigationRequest(),
                    request -> constraint(request).setAdditionalCommunityString("Invalid")),
                containsString("The community string must be"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> constraint(request).setAdditionalCommunityString("Invalid")),
            containsString("The community string must be"));
        assertThat(
                validationMessage(
                    validEditMitigationRequest(),
                    request -> constraint(request).setAdditionalCommunityString("16509:720 1234:12")),
                containsString("All ASNs in a community string must match"));
    }
    
    private void validateConflict(MitigationDefinition newDefinition, MitigationDefinition existingDefinition) 
    {
        BlackholeArborCustomerValidator validator = new BlackholeArborCustomerValidator(blackholeMitigationHelper);
        validator.validateCoexistenceForTemplateAndDevice(
                MitigationTemplate.Blackhole_Mitigation_ArborCustomer, "NewMitigation", newDefinition,
                MitigationTemplate.Blackhole_Mitigation_ArborCustomer, EXISTING_MITIGATION_NAME, existingDefinition,
                mock(TSDMetrics.class));
    }
    
    @Test
    public void nonConflictingIPs() {
        MitigationDefinition existingDefinition = validMitigationDefinition();
        constraint(existingDefinition).setIp("1.2.3.4/32");
        
        MitigationDefinition newDefinition = validMitigationDefinition();
        constraint(newDefinition).setIp("1.2.3.5/32");
        
        validateConflict(newDefinition, existingDefinition);
    }
    
    @Test
    public void existingDisabled() {
        MitigationDefinition existingDefinition = validMitigationDefinition();
        constraint(existingDefinition).setEnabled(false);
        
        MitigationDefinition newDefinition = validMitigationDefinition();
        
        validateConflict(newDefinition, existingDefinition);
    }
    
    @Test
    public void newDisabled() {
        MitigationDefinition existingDefinition = validMitigationDefinition();
        
        MitigationDefinition newDefinition = validMitigationDefinition();
        constraint(newDefinition).setEnabled(false);
        
        String ip = constraint(existingDefinition).getIp();
        try {
            // New disabled mitigations should still fail even if they are disabled - we 
            // assume a new mitigation will soon be changed from disabled to enabled
            validateConflict(newDefinition, existingDefinition);
            fail("Mitigation should have conflicted");
        } catch (DuplicateDefinitionException400 e) {
            assertThat(e.getMessage(),
                    allOf(containsString(EXISTING_MITIGATION_NAME), containsString(ip)));
        }
    }
    
    @Test
    public void overlappingTransitProviders() {
        List<String> transitProviders = ImmutableList.of(BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1);
        String transitProviderName = BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_NAME_16509_1;
        
        MitigationDefinition existingDefinition = validMitigationDefinition();
        constraint(existingDefinition).setTransitProviderIds(transitProviders);
        
        MitigationDefinition newDefinition = validMitigationDefinition();
        constraint(newDefinition).setTransitProviderIds(transitProviders);
        
        String ip = constraint(existingDefinition).getIp();
        try {
            validateConflict(newDefinition, existingDefinition);
            fail("Mitigation should have conflicted");
        } catch (DuplicateDefinitionException400 e) {
            assertThat(e.getMessage(),
                    allOf(containsString(EXISTING_MITIGATION_NAME), containsString(transitProviderName), containsString(ip)));
        }
    }
    
    @Test
    public void overlappingCommunityStrings() {
        MitigationDefinition existingDefinition = validMitigationDefinition();
        constraint(existingDefinition).setTransitProviderIds(
                ImmutableList.of(BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1));
        constraint(existingDefinition).setAdditionalCommunityString(null);
        
        MitigationDefinition newDefinition = validMitigationDefinition();
        constraint(newDefinition).setTransitProviderIds(
                ImmutableList.of(BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_2));
        constraint(newDefinition).setAdditionalCommunityString(null);
        
        String ip = constraint(existingDefinition).getIp();
        try {
            validateConflict(newDefinition, existingDefinition);
            fail("Mitigation should have conflicted");
        } catch (DuplicateDefinitionException400 e) {
            assertThat(e.getMessage(),
                    allOf(containsString(EXISTING_MITIGATION_NAME), containsString("16509:3"), containsString(ip)));
        }
    }
    
    @Test
    public void nonOverlappingTransitProviders() {
        MitigationDefinition existingDefinition = validMitigationDefinition();
        constraint(existingDefinition).setTransitProviderIds(
                ImmutableList.of(BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1));
        constraint(existingDefinition).setAdditionalCommunityString(null);
        
        MitigationDefinition newDefinition = validMitigationDefinition();
        constraint(newDefinition).setTransitProviderIds(
                ImmutableList.of(BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_12345_1));
        constraint(newDefinition).setAdditionalCommunityString(null);
        
        validateConflict(newDefinition, existingDefinition);
    }
    
    @Test
    public void overlappingDevices() {
        MitigationDefinition existingDefinition = validMitigationDefinition();
        constraint(existingDefinition).setTransitProviderIds(
                ImmutableList.of(BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1));
        constraint(existingDefinition).setAdditionalCommunityString(null);
        constraint(existingDefinition).setAdditionalRouters(ImmutableList.of(BlackholeTestUtils.DEVICE_12345));
        
        MitigationDefinition newDefinition = validMitigationDefinition();
        constraint(newDefinition).setTransitProviderIds(
                ImmutableList.of(BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_12345_1));
        constraint(newDefinition).setAdditionalCommunityString(null);
        
        String ip = constraint(existingDefinition).getIp();
        try {
            validateConflict(newDefinition, existingDefinition);
            fail("Mitigation should have conflicted");
        } catch (DuplicateDefinitionException400 e) {
            assertThat(e.getMessage(),
                    allOf(containsString(EXISTING_MITIGATION_NAME), containsString(BlackholeTestUtils.DEVICE_12345), containsString(ip)));
        }
        
        // Test the additional device on the newDefinition as well
        constraint(existingDefinition).setAdditionalRouters(Collections.emptyList());
        constraint(newDefinition).setAdditionalRouters(ImmutableList.of(BlackholeTestUtils.DEVICE_16509));
        
        try {
            validateConflict(newDefinition, existingDefinition);
            fail("Mitigation should have conflicted");
        } catch (DuplicateDefinitionException400 e) {
            assertThat(e.getMessage(),
                    allOf(containsString(EXISTING_MITIGATION_NAME), containsString(BlackholeTestUtils.DEVICE_16509), containsString(ip)));
        }
    }
    
    private static ArborBlackholeConstraint constraint(MitigationModificationRequest request) {
        if (request instanceof CreateMitigationRequest) {
            return constraint(((CreateMitigationRequest) request).getMitigationDefinition());
        } else if (request instanceof EditMitigationRequest) {
            return constraint(((EditMitigationRequest) request).getMitigationDefinition());
        } else {
            throw new RuntimeException("Cannot get constraint for request " + request);
        }
    }
    
    private static ArborBlackholeConstraint constraint(MitigationDefinition definition) {
        return (ArborBlackholeConstraint) definition.getConstraint();
    }

    private void validate(MitigationModificationRequest request) {
        BlackholeArborCustomerValidator validator = new BlackholeArborCustomerValidator(blackholeMitigationHelper);
        validator.validateRequestForTemplate(
            request,
            MitigationTemplate.Blackhole_Mitigation_ArborCustomer,
            mock(TSDMetrics.class));
    }

    private static CreateMitigationRequest validCreateMitigationRequest() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("LKT-TestMitigation");
        request.setServiceName(ServiceName.Blackhole);
        request.setMitigationTemplate(MitigationTemplate.Blackhole_Mitigation_ArborCustomer);

        MitigationActionMetadata actionMetadata = new MitigationActionMetadata();
        actionMetadata.setUser("username");
        actionMetadata.setToolName("unit-tests");
        actionMetadata.setDescription("description");
        request.setMitigationActionMetadata(actionMetadata);

        request.setMitigationDefinition(validMitigationDefinition());
        return request;
    }

    private static MitigationDefinition validMitigationDefinition() {
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setAction(new DropAction());

        ArborBlackholeConstraint constraint = new ArborBlackholeConstraint();
        constraint.setIp("1.2.3.4/32");
        constraint.setEnabled(true);
        constraint.setTransitProviderIds(emptyList());
        constraint.setAdditionalCommunityString("16509:1 16509:10");
        mitigationDefinition.setConstraint(constraint);
        return mitigationDefinition;
    }

    private static EditMitigationRequest validEditMitigationRequest() {
        EditMitigationRequest request = new EditMitigationRequest();
        request.setMitigationName("LKT-TestMitigation");
        request.setServiceName(ServiceName.Blackhole);
        request.setMitigationTemplate(MitigationTemplate.Blackhole_Mitigation_ArborCustomer);
        request.setMitigationVersion(2);

        MitigationActionMetadata actionMetadata = new MitigationActionMetadata();
        actionMetadata.setUser("username");
        actionMetadata.setToolName("unit-tests");
        actionMetadata.setDescription("description");
        request.setMitigationActionMetadata(actionMetadata);

        request.setMitigationDefinition(validMitigationDefinition());
        return request;
    }

    private static DeleteMitigationFromAllLocationsRequest validDeleteMitigationRequest() {
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName("LKT-TestMitigation");
        request.setServiceName(ServiceName.Blackhole);
        request.setMitigationTemplate(MitigationTemplate.Blackhole_Mitigation_ArborCustomer);
        request.setMitigationVersion(2);

        MitigationActionMetadata actionMetadata = new MitigationActionMetadata();
        actionMetadata.setUser("username");
        actionMetadata.setToolName("unit-tests");
        actionMetadata.setDescription("description");
        request.setMitigationActionMetadata(actionMetadata);

        return request;
    }

    private static MitigationModificationRequest notSupportedRequest() {
        return new MitigationModificationRequest() {};
    }

    private <T extends MitigationModificationRequest> String validationMessage(
        T request,
        Consumer<T> setupRequest)
        throws Exception {

        return validationException(request, setupRequest).getMessage();
    }

    private <T extends MitigationModificationRequest> IllegalArgumentException validationException(
        T request,
        Consumer<T> setupRequest)
        throws Exception {

        setupRequest.accept(request);

        IllegalArgumentException actualException = assertThrows(
            IllegalArgumentException.class,
            () -> validate(request));

        return actualException;
    }
}
