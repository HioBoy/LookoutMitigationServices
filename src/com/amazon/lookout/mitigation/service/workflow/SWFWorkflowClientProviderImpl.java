package com.amazon.lookout.mitigation.service.workflow;

import java.beans.ConstructorProperties;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.workflow.LookoutMitigationWorkflowClientExternalFactory;
import com.amazon.lookout.workflow.LookoutMitigationWorkflowClientExternalFactoryImpl;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;

/**
 * Helper to provide a workflow client based on the template being used.
 *
 */
@ThreadSafe
public class SWFWorkflowClientProviderImpl implements SWFWorkflowClientProvider {
    private static final Log LOG = LogFactory.getLog(SWFWorkflowClientProvider.class);
    
    // As of now (2014-04-17) we only have a single workflow, keeping a cached copy of that client factory here.
    private final LookoutMitigationWorkflowClientExternalFactory workflowFactory;
    
    @ConstructorProperties({"swfClient", "domain"})
    public SWFWorkflowClientProviderImpl(@Nonnull AmazonSimpleWorkflowClient simpleWorkflowClient, @Nonnull String domain) {
        Validate.notNull(simpleWorkflowClient);
        Validate.notEmpty(domain);
        
        this.workflowFactory = new LookoutMitigationWorkflowClientExternalFactoryImpl(simpleWorkflowClient, domain);
    }

    /**
     * Get a workflow client based on the template passed as input.
     * @param template Template name corresponding to which we require a workflow client.
     * @param deviceName Device where the workflow is to be run.
     * @param workflowId WorkflowId for the workflow to be run.
     * @param metrics TSDMetrics to record the time it takes for the factory to hand us a workflow client (after checking for duplicate workflowId).
     * @return WorkflowClientExternal which represents the workflow client to be used for this template and device.
     */
    @Override
    public WorkflowClientExternal getWorkflowClient(@Nonnull String template, @Nonnull String deviceName, long workflowId, @Nonnull TSDMetrics metrics) {
        Validate.notEmpty(template);
        Validate.notEmpty(deviceName);
        Validate.notNull(metrics);
        
        TSDMetrics subMetrics = metrics.newSubMetrics("SWFWorkflowClientProviderImpl.getWorkflowClient");
        try {
            String swfWorkflowId = deviceName + "_" + workflowId;
            LOG.debug("Requested WorkflowFactory for template: " + template + ", device: " + deviceName + ", workflowId: " + workflowId + 
                      " Returning workflow client with SWF workflowId set to: " + swfWorkflowId);
            
            return workflowFactory.getClient(swfWorkflowId);
        } finally {
            subMetrics.end();
        }
    }
}
