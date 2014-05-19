package com.amazon.lookout.mitigation.service.activity;

import javax.measure.unit.Unit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.annotation.ThreadSafe;

import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.service.Activity;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;

@ThreadSafe
@Service("LookoutMitigationService")
public class CreateDummySimpleWorkFlowActivity extends Activity {

    private static final Log LOG = LogFactory.getLog(CreateDummySimpleWorkFlowActivity.class);

    @Operation("CreateDummySimpleWorkFlow")
    @Documentation("CreateDummySimpleWorkFlow")
    public void enact() {

        Metrics tsdMetrics = getMetrics();
        // This counter is used to calculate the service workload for this operation.
        tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, 1, Unit.ONE);
        
        try {
           
            /*ClientConfiguration config = new ClientConfiguration().withSocketTimeout(70*1000);
            AWSCredentialsProvider credentialsProvider = new OdinAWSCredentialsProvider("com.amazon.lookout.swf");
            AmazonSimpleWorkflow service = new AmazonSimpleWorkflowClient(credentialsProvider.getCredentials(), config);
            service.setEndpoint("https://swf.us-east-1.amazonaws.com");

            String domain = "Lookout-Test-Domain";

            GreeterWorkflowClientExternalFactory factory = new GreeterWorkflowClientExternalFactoryImpl(service, domain);
            GreeterWorkflowClientExternal greeter = factory.getClient("swf-test");
            LOG.info("Hello, Simple Work Flow");
            greeter.greet();

            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, 0, Unit.ONE);  */
        } catch (Exception internalError) {                   
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, 1, Unit.ONE);
            throw new InternalServerError500(internalError.getMessage(), internalError);
        }      
    }
}
