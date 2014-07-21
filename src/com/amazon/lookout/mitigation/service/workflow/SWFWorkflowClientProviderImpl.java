package com.amazon.lookout.mitigation.service.workflow;

import java.beans.ConstructorProperties;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.lookout.workflow.LookoutMitigationWorkflowClientExternalFactory;
import com.amazon.lookout.workflow.LookoutMitigationWorkflowClientExternalFactoryImpl;
import com.amazon.lookout.workflow.LookoutReaperWorkflowClientExternalFactory;
import com.amazon.lookout.workflow.LookoutReaperWorkflowClientExternalFactoryImpl;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;

/**
 * Helper to provide a workflow client based on the template being used.
 *
 */
@ThreadSafe
public class SWFWorkflowClientProviderImpl implements SWFWorkflowClientProvider {
    private static final Log LOG = LogFactory.getLog(SWFWorkflowClientProvider.class);
    
    // Keep a cached copy of the mitigation modification workflow client factory here.
    private final LookoutMitigationWorkflowClientExternalFactory mitigationModificationWorkflowFactory;
    
    // Keep a cached copy of the reaper workflow client factory here.
    private final LookoutReaperWorkflowClientExternalFactory reaperWorkflowFactory;
    
    @ConstructorProperties({"swfClient", "swfDomain"})
    public SWFWorkflowClientProviderImpl(@Nonnull AmazonSimpleWorkflowClient simpleWorkflowClient, @Nonnull String swfDomain) {
        Validate.notNull(simpleWorkflowClient);
        Validate.notEmpty(swfDomain);
        
        this.mitigationModificationWorkflowFactory = new LookoutMitigationWorkflowClientExternalFactoryImpl(simpleWorkflowClient, swfDomain);
        this.reaperWorkflowFactory = new LookoutReaperWorkflowClientExternalFactoryImpl(simpleWorkflowClient, swfDomain);
    }

    /**
     * Get a workflow client based on the template passed as input.
     * @param template Template name corresponding to which we require a workflow client.
     * @param deviceName Device where the workflow is to be run.
     * @param workflowId WorkflowId for the workflow to be run.
     * @return WorkflowClientExternal which represents the workflow client to be used for this template and device.
     */
    @Override
    public WorkflowClientExternal getMitigationModificationWorkflowClient(@Nonnull String template, @Nonnull String deviceName, long workflowId) {
        Validate.notEmpty(template);
        Validate.notEmpty(deviceName);
        Validate.isTrue(workflowId > 0);
        
        String swfWorkflowId = deviceName + "_" + workflowId;
        LOG.debug("Requested WorkflowFactory for template: " + template + ", device: " + deviceName + ", workflowId: " + workflowId + 
                  " Returning workflow client with SWF workflowId set to: " + swfWorkflowId);
        return mitigationModificationWorkflowFactory.getClient(swfWorkflowId);
    }
    
    /**
     * Get a reaper workflow client for the swfWorkflowId passed as input.
     * @param swfWorkflowId WorkflowId for the reaper workflow to be run.
     * @return WorkflowClientExternal which represents the reaper workflow client to be used.
     */
    @Override
    public WorkflowClientExternal getReaperWorkflowClient(@Nonnull String swfWorkflowId) {
        Validate.notEmpty(swfWorkflowId);
        
        LOG.debug("Requested ReaperWorkflowFactory for workflowId: " + swfWorkflowId);
        return reaperWorkflowFactory.getClient(swfWorkflowId);
    }
}
