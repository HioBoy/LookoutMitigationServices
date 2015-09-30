package com.amazon.lookout.mitigation.service.activity.validator.template;

import com.amazon.lookout.mitigation.service.AlarmCheck;
import com.amazon.lookout.mitigation.service.ArborBlackholeConstraint;
import com.amazon.lookout.mitigation.service.ArborBlackholeSetEnabledStateConstraint;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.DropAction;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.function.Consumer;

import static com.amazon.lookout.test.common.util.AssertUtils.assertThrows;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

@RunWith(JUnitParamsRunner.class)
public class BlackholeArborCustomerValidatorTest {
    @Test
    public void validRequest() throws Exception {
        validate(validCreateMitigationRequest());
        validate(validEditMitigationRequest());
        validate(anyDeleteMitigationRequest());
    }

    @Test
    public void validEnabledStateRequest() {
        validate(validSetEnabledStateRequest());
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
        "Cannot contain both space and \"quote",
        "跑",
        "#"
    })
    public void mitigationNameIsValid(String invalidMitigationName) throws Exception {
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
    public void ipV6IsNotSupported(String invalidIp) throws Exception {
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
    public void specialIpsAreNotSupported() throws Exception {
        assertThat(
            validationMessage(
                validCreateMitigationRequest(),
                request -> constraint(request).setIp("::1/32")),
            containsString("::1"));
        assertThat(
            validationMessage(
                validEditMitigationRequest(),
                request -> constraint(request).setIp("::1/32")),
            containsString("::1"));
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

    private static ArborBlackholeConstraint constraint(MitigationModificationRequest request) {
        if (request instanceof CreateMitigationRequest) {
            return (ArborBlackholeConstraint)
                ((CreateMitigationRequest) request).getMitigationDefinition().getConstraint();
        } else if (request instanceof EditMitigationRequest) {
            return (ArborBlackholeConstraint)
                ((EditMitigationRequest) request).getMitigationDefinition().getConstraint();
        } else {
            throw new RuntimeException("Cannot get constraint for request " + request);
        }
    }

    private static void validate(MitigationModificationRequest request) {
        BlackholeArborCustomerValidator validator = new BlackholeArborCustomerValidator();
        validator.validateRequestForTemplate(
            request,
            MitigationTemplate.Blackhole_Mitigation_ArborCustomer);
    }

    private static CreateMitigationRequest validCreateMitigationRequest() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("TestMitigation");
        request.setServiceName(ServiceName.Blackhole);
        request.setMitigationTemplate(MitigationTemplate.Blackhole_Mitigation_ArborCustomer);

        MitigationActionMetadata actionMetadata = new MitigationActionMetadata();
        actionMetadata.setUser("username");
        actionMetadata.setToolName("unit-tests");
        actionMetadata.setDescription("description");
        request.setMitigationActionMetadata(actionMetadata);

        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setAction(new DropAction());

        ArborBlackholeConstraint constraint = new ArborBlackholeConstraint();
        constraint.setIp("1.2.3.4/32");
        constraint.setEnabled(true);
        constraint.setTransitProviderIds(emptyList());
        mitigationDefinition.setConstraint(constraint);

        request.setMitigationDefinition(mitigationDefinition);
        return request;
    }

    private static EditMitigationRequest validEditMitigationRequest() {
        EditMitigationRequest request = new EditMitigationRequest();
        request.setMitigationName("TestMitigation");
        request.setServiceName(ServiceName.Blackhole);
        request.setMitigationTemplate(MitigationTemplate.Blackhole_Mitigation_ArborCustomer);
        request.setMitigationVersion(2);

        MitigationActionMetadata actionMetadata = new MitigationActionMetadata();
        actionMetadata.setUser("username");
        actionMetadata.setToolName("unit-tests");
        actionMetadata.setDescription("description");
        request.setMitigationActionMetadata(actionMetadata);

        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setAction(new DropAction());

        ArborBlackholeConstraint constraint = new ArborBlackholeConstraint();
        constraint.setIp("1.2.3.4/32");
        constraint.setEnabled(true);
        constraint.setTransitProviderIds(emptyList());
        mitigationDefinition.setConstraint(constraint);

        request.setMitigationDefinition(mitigationDefinition);
        return request;
    }

    private static EditMitigationRequest validSetEnabledStateRequest() {
        EditMitigationRequest request = new EditMitigationRequest();
        request.setMitigationName("TestMitigation");
        request.setServiceName(ServiceName.Blackhole);
        request.setMitigationTemplate(MitigationTemplate.Blackhole_Mitigation_ArborCustomer);
        request.setMitigationVersion(2);

        MitigationActionMetadata actionMetadata = new MitigationActionMetadata();
        actionMetadata.setUser("username");
        actionMetadata.setToolName("unit-tests");
        actionMetadata.setDescription("description");
        request.setMitigationActionMetadata(actionMetadata);

        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setAction(new DropAction());

        ArborBlackholeSetEnabledStateConstraint constraint = new ArborBlackholeSetEnabledStateConstraint();
        constraint.setEnabled(true);
        mitigationDefinition.setConstraint(constraint);

        request.setMitigationDefinition(mitigationDefinition);
        return request;
    }

    private static DeleteMitigationFromAllLocationsRequest anyDeleteMitigationRequest() {
        return new DeleteMitigationFromAllLocationsRequest();
    }

    private static MitigationModificationRequest notSupportedRequest() {
        return new MitigationModificationRequest() {};
    }

    private static <T extends MitigationModificationRequest> String validationMessage(
        T request,
        Consumer<T> setupRequest)
        throws Exception {

        return validationException(request, setupRequest).getMessage();
    }

    private static <T extends MitigationModificationRequest> IllegalArgumentException validationException(
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
