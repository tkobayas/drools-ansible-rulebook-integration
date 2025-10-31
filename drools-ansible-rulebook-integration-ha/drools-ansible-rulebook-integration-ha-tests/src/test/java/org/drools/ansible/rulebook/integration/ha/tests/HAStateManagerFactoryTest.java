package org.drools.ansible.rulebook.integration.ha.tests;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test HAStateManagerFactory creates correct implementation based on system property
 */
class HAStateManagerFactoryTest {

    @Test
    void testFactoryCreatesH2ByDefault() {
        // Clear any existing system property
        System.clearProperty("ha.db.type");

        HAStateManager manager = HAStateManagerFactory.create();

        assertThat(manager).isNotNull();
        assertThat(manager.getClass().getName()).endsWith("H2StateManager");
    }

    @Test
    void testFactoryCreatesH2WhenExplicitlySet() {
        System.setProperty("ha.db.type", "h2");

        try {
            HAStateManager manager = HAStateManagerFactory.create();

            assertThat(manager).isNotNull();
            assertThat(manager.getClass().getName()).endsWith("H2StateManager");
        } finally {
            System.clearProperty("ha.db.type");
        }
    }

    @Test
    void testFactoryCreatesPostgreSQLWhenSet() {
        System.setProperty("ha.db.type", "postgres");

        try {
            HAStateManager manager = HAStateManagerFactory.create();

            assertThat(manager).isNotNull();
            assertThat(manager.getClass().getName()).endsWith("PostgreSQLStateManager");
        } finally {
            System.clearProperty("ha.db.type");
        }
    }

    @Test
    void testFactoryCreatesPostgreSQLWithPostgresqlValue() {
        System.setProperty("ha.db.type", "postgresql");

        try {
            HAStateManager manager = HAStateManagerFactory.create();

            assertThat(manager).isNotNull();
            assertThat(manager.getClass().getName()).endsWith("PostgreSQLStateManager");
        } finally {
            System.clearProperty("ha.db.type");
        }
    }

    @Test
    void testFactoryIsCaseInsensitive() {
        System.setProperty("ha.db.type", "POSTGRES");

        try {
            HAStateManager manager = HAStateManagerFactory.create();

            assertThat(manager).isNotNull();
            assertThat(manager.getClass().getName()).endsWith("PostgreSQLStateManager");
        } finally {
            System.clearProperty("ha.db.type");
        }
    }
}
