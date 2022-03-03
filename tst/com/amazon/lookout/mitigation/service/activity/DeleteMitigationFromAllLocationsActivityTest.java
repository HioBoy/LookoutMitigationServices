package com.amazon.lookout.mitigation.service.activity;

import static com.amazon.lookout.test.common.util.AssertUtils.assertThrows;
import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.model.RequestType;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.services.s3.AmazonS3;

import com.amazon.lookout.mitigation.datastore.model.CurrentRequest;
import com.amazon.lookout.mitigation.RequestCreator;

public class DeleteMitigationFromAllLocationsActivityTest {

    private static final int MITIGATION_VERSION = 1;

    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
    }

    @Test
    public void enactDeleteNotSupportedForIPTablesEdgeMitigation() throws Exception {
        DeleteMitigationFromAllLocationsActivity activity = createActivityWithValidators();
        DeleteMitigationFromAllLocationsRequest request = sampleDeleteIPTablesMitigationRequest();

        BadRequest400 actualError = assertThrows(BadRequest400.class, () -> activity.enact(request));

        assertThat(actualError.getMessage(), containsString("not supported"));
        assertThat(actualError.getMessage(), containsString(MitigationTemplate.IPTables_Mitigation_EdgeCustomer));
    }

    private DeleteMitigationFromAllLocationsActivity createActivityWithValidators() {
        return new DeleteMitigationFromAllLocationsActivity(
            new RequestValidator("/random/path/location/json"),
            new TemplateBasedRequestValidator(),
            mock(RequestCreator.class));
    }

    private DeleteMitigationFromAllLocationsRequest sampleDeleteIPTablesMitigationRequest() {
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName("IPTablesMitigationName");
        request.setMitigationTemplate(MitigationTemplate.IPTables_Mitigation_EdgeCustomer);
        request.setMitigationVersion(MITIGATION_VERSION);
        request.setDeviceName(DeviceName.BLACKWATCH_BORDER.name());

        MitigationActionMetadata actionMetadata = new MitigationActionMetadata();
        actionMetadata.setUser("username");
        actionMetadata.setToolName("unit-tests");
        actionMetadata.setDescription("description");
        request.setMitigationActionMetadata(actionMetadata);

        return request;
    }
}
