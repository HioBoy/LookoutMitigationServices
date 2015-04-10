package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageHandler;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

public class DDBBasedEditRequestStorageHandler extends DDBBasedRequestStorageHandler implements RequestStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedEditRequestStorageHandler.class);

    private final TemplateBasedRequestValidator templateBasedRequestValidator;

    public DDBBasedEditRequestStorageHandler(@Nonnull AmazonDynamoDBClient dynamoDBClient, @Nonnull String domain, @Nonnull TemplateBasedRequestValidator templateBasedRequestValidator) {
        super(dynamoDBClient, domain);

        Validate.notNull(templateBasedRequestValidator);
        this.templateBasedRequestValidator = templateBasedRequestValidator;
    }

    @Override
    public long storeRequestForWorkflow(MitigationModificationRequest mitigationRequest,
            Set<String> locations, TSDMetrics metrics) {
        return 0;
    }
}
