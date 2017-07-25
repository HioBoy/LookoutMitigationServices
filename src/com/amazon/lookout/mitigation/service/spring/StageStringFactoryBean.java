package com.amazon.lookout.mitigation.service.spring;
       
import org.springframework.beans.factory.FactoryBean;

/**
 * Bean factory to generate standardized stage names.
 */
public class StageStringFactoryBean implements FactoryBean<String> {
    /** Apollo stage name. */
    private String stageName;

    @Override
    public String getObject() throws Exception {
        return Stage.getStage(stageName).getName();
    }
 
    @Override
    public Class<String> getObjectType() {
        return String.class;
    }
 
    @Override
    public boolean isSingleton() {
        return true;
    }
 
    /**
     * Sets stage name.
     * @param stageName to determine the Stage of
     */
    public void setStageName(final String stageName) {
        this.stageName = stageName;
    }
}
