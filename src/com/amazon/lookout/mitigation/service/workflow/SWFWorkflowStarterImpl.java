package com.amazon.lookout.mitigation.service.workflow;

import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.workflow.helper.TemplateBasedLocationsHelperManager;
import com.amazon.lookout.workflow.LookoutMitigationWorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.flow.StartWorkflowOptions;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.model.WorkflowType;

/**
 * Helper for starting new workflows.
 * It uses the SWFWorkflowClientProvider to get an appropriate workflow client corresponding to the template being used for the client request
 * and then uses the client to start the workflow steps.
 */
public class SWFWorkflowStarterImpl implements SWFWorkflowStarter {
    private static final Log LOG = LogFactory.getLog(SWFWorkflowStarterImpl.class);
    
    private static final String WORKFLOW_ID_METRIC_PROPERTY_KEY = "WorkflowId";
    private static final String WORKFLOW_SWF_RUN_ID_METRIC_PROPERTY_KEY = "WorkflowRunId";
    private static final String WORKFLOW_TYPE_NAME_METRIC_PROPERTY_KEY = "WorkflowTypeName";
    private static final String WORKFLOW_TYPE_VERSION_METRIC_PROPERTY_KEY = "WorkflowTypeVersion";
    
    // Default number of seconds we expect the workflow to complete in. Currently set to 10 minutes to allow enough time for workflow steps to complete.
    // This value is deliberately set to higher than what our usual goal for finishing a workflow - to allow workflows to complete even if they're taking a bit longer.
    private static final long DEFAULT_WORKFLOW_COMPLETION_TIMEOUT_SECONDS = 600;
    
    // Default timeout for deciders to finish a single decision task. Deciders are meant to be quick and anything taking above 60s would indicate a problem with our logic.
    private static final long DEFAULT_WORKFLOW_DECISION_TASK_TIMEOUT_SECONDS = 60;
    
    private final SWFWorkflowClientProvider workflowClientProvider;
    private final TemplateBasedLocationsHelperManager templateBasedLocationsHelper;
    
    public SWFWorkflowStarterImpl(@Nonnull SWFWorkflowClientProvider workflowClientProvider, @Nonnull TemplateBasedLocationsHelperManager templateBasedLocationsHelper) {
        Validate.notNull(workflowClientProvider);
        this.workflowClientProvider = workflowClientProvider;
        
        Validate.notNull(templateBasedLocationsHelper);
        this.templateBasedLocationsHelper = templateBasedLocationsHelper;
    }

    /**
     * Start the workflow for the request passed as input.
     * @param workflowId WorkflowId to use for the new workflow to be run.
     * @param request MitigationModificationRequest request passed by the client.
     * @param mitigationVersion Version to use for this mitigation.
     * @param deviceName device on which the workflow steps are to be run.
     * @param metrics TSDMetrics instance to log the time required to start the workflow, including SWF's check to check for workflowId's uniqueness.
     */
    @Override
    public void startWorkflow(long workflowId, @Nonnull MitigationModificationRequest request, int mitigationVersion, @Nonnull String deviceName, @Nonnull TSDMetrics metrics) {
        Validate.isTrue(workflowId > 0);
        Validate.notNull(request);
        Validate.isTrue(mitigationVersion > 0);
        Validate.notEmpty(deviceName);
        Validate.notNull(metrics);
        
        TSDMetrics subMetrics = metrics.newSubMetrics("SWFWorkflowStarterImpl.startWorkflow");
        try {
            String mitigationTemplate = request.getMitigationTemplate();
            
            // Get workflow client for this template + device.
            LookoutMitigationWorkflowClientExternal workflowClient = 
                    (LookoutMitigationWorkflowClientExternal) workflowClientProvider.getWorkflowClient(mitigationTemplate, deviceName, workflowId, subMetrics);
            
            // Add workflow properties to request metrics.
            WorkflowExecution workflowExecution = workflowClient.getWorkflowExecution();
            String swfWorkflowId = workflowExecution.getWorkflowId();
            String swfRunId = workflowExecution.getRunId();
            subMetrics.addProperty(WORKFLOW_ID_METRIC_PROPERTY_KEY, swfWorkflowId);
            subMetrics.addProperty(WORKFLOW_SWF_RUN_ID_METRIC_PROPERTY_KEY, swfRunId);
            
            WorkflowType workflowType = workflowClient.getWorkflowType();
            String workflowTypeName = workflowType.getName();
            String workflowTypeVersion = workflowType.getVersion();
            subMetrics.addProperty(WORKFLOW_TYPE_NAME_METRIC_PROPERTY_KEY, workflowTypeName);
            subMetrics.addProperty(WORKFLOW_TYPE_VERSION_METRIC_PROPERTY_KEY, workflowTypeVersion);
            
            // Get the default configurations for the workflow.
            StartWorkflowOptions workflowOptions = getDefaultStartWorkflowOptions();
            
            // Get locations where we need to run this workflow. In most cases it is provided by the client, but for some templates
            // we might have locations based on the templateName, hence checking with the templateBasedLocationsHelper and also passing it the original request to have the entire context.
            Set<String> locationsToDeploy = templateBasedLocationsHelper.getLocationsForDeployment(request);
            
            // Start running the workflow.
            workflowClient.startMitigationWorkflow(workflowId, locationsToDeploy, request, mitigationVersion, deviceName, workflowOptions);
            
            LOG.debug("Started workflow for workflowId: " + workflowId + " in SWF, with SWFWorkflowId: " + swfWorkflowId + " SWFRunId: " + swfRunId + 
                      " WorkflowType: " + workflowTypeName + " WorkflowTypeVersion: " + workflowTypeVersion + " for request: " + ReflectionToStringBuilder.toString(request) + 
                      " with mitigationVersion: " + mitigationVersion);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Helper to get default configurations to use for this workflow.
     * @return StartWorkflowOptions which represents basic timeout configurations that we want to set for the new workflow.
     */
    private StartWorkflowOptions getDefaultStartWorkflowOptions() {
        StartWorkflowOptions startWorkflowOptions = new StartWorkflowOptions();
        startWorkflowOptions.setExecutionStartToCloseTimeoutSeconds(DEFAULT_WORKFLOW_COMPLETION_TIMEOUT_SECONDS);
        startWorkflowOptions.setTaskStartToCloseTimeoutSeconds(DEFAULT_WORKFLOW_DECISION_TASK_TIMEOUT_SECONDS);
        return startWorkflowOptions;
    }
}
