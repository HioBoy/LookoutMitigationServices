package com.amazon.lookout.mitigation.service.spring;
 
/**
 * Enum representing apollo environment stages.
 *
 * The enum has then is able to return the standardized name of the
 * stage it represents. Which is simply the stage name lowercased.
 */
public enum Stage {
    /**
     * Beta stage.
     */
    BETA("beta"),
    /**
     * Gamma stage.
     */
    GAMMA("gamma"),
    /**
     * Prod stage.
     */
    PROD("prod");
 
    /**
     * The standarized name of the stage.
     */
    private final String name;
 
    /**
     * Constructor.
     * @param name the standardized stage name.
     */
    Stage(final String name) {
        this.name = name;
    }
 
    /**
     * Gets the standardized stage name.
     * @return the standard stage name.
     */
    public String getName() {
        return name;
    }
 
    /**
     * Given a stage name convert it to a Stage enum.
     * @param stageName the stage name.
     * @return Stage enum representing stageName.
     */
    public static Stage getStage(final String stageName) {
        for (Stage stage : Stage.values()) {
            if (stage.getName().equalsIgnoreCase(stageName)) {
                return stage;
            }
        }
 
        throw new IllegalArgumentException(String.format("%s cannot be converted to enum Stage", stageName));
    }

}

