package org.drools.ansible.rulebook.integration.api;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.api.domain.Rule;
import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.domain.conditions.SimpleCondition;
import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.drools.ansible.rulebook.integration.api.ObjectMapperFactory.createMapper;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleLogicalOperatorsTest {

    private static final String JSON1 =
            """
            {
               "rules":[
                  {"Rule": {
                     "name":"R1",
                     "condition":"sensu.data.i == 1"
                  }},
                  {"Rule": {
                     "name":"R2",
                     "condition":{
                        "all":[
                           "sensu.data.i == 3",
                           "j == 2"
                        ]
                     }
                  }},
                  {"Rule": {
                     "name":"R3",
                     "condition":{
                        "any":[
                           {
                              "all":[
                                 "sensu.data.i == 3",
                                 "j == 2"
                              ]
                           },
                           {
                              "all":[
                                 "sensu.data.i == 4",
                                 "j == 3"
                              ]
                           }
                        ]
                     }
                  }}
               ]
            }
            """;


    @Test
    void testWriteJson() throws JsonProcessingException {
        Rule rule = new Rule();
        SimpleCondition c1 = new SimpleCondition();
        c1.setAll(Arrays.asList(new SimpleCondition("sensu.data.i == 3"), new SimpleCondition("j == 2")));
        SimpleCondition c2 = new SimpleCondition();
        c2.setAll(Arrays.asList(new SimpleCondition("sensu.data.i == 4"), new SimpleCondition("j == 3")));
        SimpleCondition c3 = new SimpleCondition();
        c3.setAny(Arrays.asList(c1, c2));
        rule.setCondition(c3);

        ObjectMapper mapper = createMapper(new JsonFactory());
        String json = mapper.writerFor(Rule.class).writeValueAsString(rule);
        System.out.println(json);
    }

    @Test
    void testReadJson() throws JsonProcessingException {
        System.out.println(JSON1);
        ObjectMapper mapper = createMapper(new JsonFactory());
        RulesSet rulesSet = mapper.readValue(JSON1, RulesSet.class);
        System.out.println(rulesSet);
        String json = mapper.writerFor(RulesSet.class).writeValueAsString(rulesSet);
        System.out.println(json);
    }

    @Test
    void testProcessRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"facts\": [ { \"sensu\": { \"data\": { \"i\":3 } } }, { \"j\":3 } ] }" ).join();
        assertEquals( 0, matchedRules.size() );

        matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":4 } } }" ).join();
        assertEquals( 1, matchedRules.size() );

        RuleMatch ruleMatch = RuleMatch.from( matchedRules.get(0) );
        assertEquals( "R3", ruleMatch.getRuleName() );
        assertEquals( 3, ((Map) ruleMatch.getFact("m_3")).get("j") );

        assertEquals( 4, ((Map) ((Map) ((Map) ruleMatch.getFact("m_2")).get("sensu")).get("data")).get("i") );

        rulesExecutor.dispose();
    }
}
