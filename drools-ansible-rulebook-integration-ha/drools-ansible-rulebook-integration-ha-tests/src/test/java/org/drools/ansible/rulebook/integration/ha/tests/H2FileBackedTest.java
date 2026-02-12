package org.drools.ansible.rulebook.integration.ha.tests;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;

/**
 * Tests file-backed H2 database for HA using DROOLS_HA_H2_FILE env var.
 *
 * This test is run in a separate surefire execution with env vars:
 *   DROOLS_HA_DB_TYPE=h2
 *   DROOLS_HA_H2_FILE=${project.build.directory}/h2-file-test/eda_ha
 *
 * The test does NOT pass db_url in config — it relies purely on the env var.
 */
class H2FileBackedTest {

    private static final String HA_UUID = "h2-file-test";

    private static final String RULE_SET = """
            {
                "name": "Multi Condition Ruleset",
                "rules": [
                    {"Rule": {
                        "name": "temperature_alert",
                        "condition": {
                            "AllCondition": [
                                {
                                    "EqualsExpression": {
                                        "lhs": {"Event": "i"},
                                        "rhs": {"Integer": 1}
                                    }
                                },
                                {
                                    "EqualsExpression": {
                                        "lhs": {"Event": "j"},
                                        "rhs": {"Integer": 2}
                                    }
                                }
                            ]
                        },
                        "action": {
                            "run_playbook": [{"name": "alert.yml"}]
                        }
                    }}
                ]
            }
            """;

    // Config without db_url — H2StateManager should pick up DROOLS_HA_H2_FILE env var
    private static final String CONFIG_JSON = """
            {"write_after": 1}
            """;

    private Path h2FilePath;

    @BeforeEach
    void setUp() {
        String h2FileEnv = System.getenv("DROOLS_HA_H2_FILE");
        assertThat(h2FileEnv)
                .as("DROOLS_HA_H2_FILE env var must be set (configured in pom.xml surefire execution)")
                .isNotNull()
                .isNotEmpty();
        h2FilePath = Path.of(h2FileEnv + ".mv.db");

        // Ensure ha.db.type is set (env var DROOLS_HA_DB_TYPE should have set this via AbstractHATestBase static block,
        // but set it explicitly in case this test runs standalone)
        System.setProperty("ha.db.type", "h2");
    }

    @AfterEach
    void tearDown() throws Exception {
        // Clean up the H2 file
        Files.deleteIfExists(h2FilePath);
        Path traceFile = h2FilePath.resolveSibling(h2FilePath.getFileName().toString().replace(".mv.db", ".trace.db"));
        Files.deleteIfExists(traceFile);
    }

    @Test
    void testSessionRecoveryWithPartialMatch() {
        // === Node 1: process first event, creating a partial match ===
        AstRulesEngine engine1 = new AstRulesEngine();
        HAIntegrationTestBase.AsyncConsumer consumer1 = null;
        long sessionId1;

        try {
            engine1.initializeHA(HA_UUID, "worker-1", null, CONFIG_JSON);
            sessionId1 = engine1.createRuleset(RULE_SET, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

            consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
            consumer1.startConsuming(engine1.port());

            engine1.enableLeader();

            // Send first event (partial match: i=1, still need j=2)
            String result1 = engine1.assertEvent(sessionId1, createEvent("{\"i\":1}"));
            assertThat(readValueAsListOfMapOfStringAndObject(result1)).isEmpty();

            engine1.advanceTime(sessionId1, 5, "SECONDS");

            // Verify H2 file was created on disk via env var
            assertThat(h2FilePath)
                    .as("H2 database file should be created from DROOLS_HA_H2_FILE env var")
                    .exists();
            assertThat(Files.size(h2FilePath)).isGreaterThan(0);

            // Simulate crash
            engine1.disableLeader();
            engine1.dispose(sessionId1);
            engine1.close();
            engine1 = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (consumer1 != null) {
                consumer1.stop();
            }
            if (engine1 != null) {
                engine1.close();
            }
        }

        // Verify file survives after engine shutdown
        assertThat(h2FilePath).exists();

        // === Node 2: recover from file and complete the match ===
        AstRulesEngine engine2 = new AstRulesEngine();
        HAIntegrationTestBase.AsyncConsumer consumer2 = null;
        long sessionId2;

        try {
            engine2.initializeHA(HA_UUID, "worker-2", null, CONFIG_JSON);
            sessionId2 = engine2.createRuleset(RULE_SET, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

            consumer2 = new HAIntegrationTestBase.AsyncConsumer("consumer2");
            consumer2.startConsuming(engine2.port());

            // Node 2 becomes leader and recovers session from H2 file
            engine2.enableLeader();

            // Send second event that completes the match (j=2)
            String result2 = engine2.assertEvent(sessionId2, createEvent("{\"j\":2}"));

            // Should have a match: i=1 recovered from file + j=2 from this event
            List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result2);
            assertThat(matches).hasSize(1);

            Map<String, Object> match = matches.get(0);
            assertThat(match).containsEntry("name", "temperature_alert")
                    .containsKey("matching_uuid");

            engine2.disableLeader();
            engine2.dispose(sessionId2);
            engine2.close();
            engine2 = null;
        } finally {
            if (consumer2 != null) {
                consumer2.stop();
            }
            if (engine2 != null) {
                engine2.close();
            }
        }
    }
}
