package com.amazon.lookout.mitigation.service.activity;

import javax.measure.unit.Unit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.annotation.ThreadSafe;

import amazon.odin.awsauth.OdinAWSCredentialsProvider;

import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.service.Activity;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.commons.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.workflows.GreeterWorkflowClientExternal;
import com.amazon.lookout.mitigation.workflows.GreeterWorkflowClientExternalFactory;
import com.amazon.lookout.mitigation.workflows.GreeterWorkflowClientExternalFactoryImpl;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient;

@ThreadSafe
@Service("LookoutMitigationService")
public class CreateDummySimpleWorkFlowActivity extends Activity {

    private static final Log LOG = LogFactory.getLog(CreateDummySimpleWorkFlowActivity.class);

    @Operation("CreateDummySimpleWorkFlow")
    @Documentation("CreateDummySimpleWorkFlow")
    public void enact() {

        Metrics tsdMetrics = getMetrics();
        // This counter is used to calculate the service workload for this operation.
        tsdMetrics.addCount(LookoutMitigationServiceConstants.REQUEST_ENACTED, 1, Unit.ONE);
        
        try {
           
            ClientConfiguration config = new ClientConfiguration().withSocketTimeout(70*1000);
            AWSCredentialsProvider credentialsProvider = new OdinAWSCredentialsProvider("com.amazon.lookout.swf");
            AmazonSimpleWorkflow service = new AmazonSimpleWorkflowClient(credentialsProvider.getCredentials(), config);
            service.setEndpoint("https://swf.us-east-1.amazonaws.com");

            String domain = "Lookout-Test-Domain";

            GreeterWorkflowClientExternalFactory factory = new GreeterWorkflowClientExternalFactoryImpl(service, domain);
            GreeterWorkflowClientExternal greeter = factory.getClient("swf-test");
            LOG.info("Hello, Simple Work Flow");
            greeter.greet();

            tsdMetrics.addCount(LookoutMitigationServiceConstants.INTERNAL_FAILURE, 0, Unit.ONE);  
        } catch (Exception internalError) {                   
            tsdMetrics.addCount(LookoutMitigationServiceConstants.INTERNAL_FAILURE, 1, Unit.ONE);
            throw new InternalServerError500(internalError.getMessage(), internalError);
        }      
    }
}
