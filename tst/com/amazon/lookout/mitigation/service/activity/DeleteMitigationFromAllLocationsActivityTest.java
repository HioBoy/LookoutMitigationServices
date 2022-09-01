package com.amazon.lookout.mitigation.service.activity;

import static com.amazon.lookout.test.common.util.AssertUtils.assertThrows;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import com.amazon.blackwatch.bwircellconfig.model.BwirCellConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.test.common.util.TestUtils;

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
            new RequestValidator("/random/path/location/json",
                    mock(BwirCellConfig.class)),
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
