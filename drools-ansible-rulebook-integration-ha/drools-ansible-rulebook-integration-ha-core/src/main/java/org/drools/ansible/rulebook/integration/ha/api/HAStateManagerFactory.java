package org.drools.ansible.rulebook.integration.ha.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating HAStateManager instances based on configuration
 */
public class HAStateManagerFactory {

    private static final Logger logger = LoggerFactory.getLogger(HAStateManagerFactory.class);

    private HAStateManagerFactory() {
    }

    /**
     * Create an HAStateManager instance based on the given database type.
     *
     * @param dbType the database type: "postgres" or "h2" (default is "postgres" if null/empty)
     * @return HAStateManager instance
     */
    public static HAStateManager create(String dbType) {
        if (dbType == null || dbType.isEmpty()) {
            dbType = "postgres";
        }

        try {
            String className;
            String managerName;

            if ("postgres".equalsIgnoreCase(dbType) || "postgresql".equalsIgnoreCase(dbType)) {
                className = "org.drools.ansible.rulebook.integration.ha.postgres.PostgreSQLStateManager";
                managerName = "PostgreSQLStateManager";
            } else if ("h2".equalsIgnoreCase(dbType)) {
                className = "org.drools.ansible.rulebook.integration.ha.h2.H2StateManager";
                managerName = "H2StateManager";
            } else {
                throw new IllegalArgumentException("Unknown db_type: '" + dbType + "'. Supported values are 'postgres' and 'h2'.");
            }

            Class<?> managerClass = Class.forName(className);
            HAStateManager manager = (HAStateManager) managerClass.getDeclaredConstructor().newInstance();
            logger.info("Created {} instance (db_type={})", managerName, dbType);
            return manager;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to find HAStateManager implementation for type '" + dbType + "': " + e.getMessage() +
                ". Make sure the appropriate module (ha-h2 or ha-postgres) is on the classpath.", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create HAStateManager for type '" + dbType + "': " + e.getMessage(), e);
        }
    }
}
