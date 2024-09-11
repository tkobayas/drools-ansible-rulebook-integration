/*
 * Copyright 2024 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ConditionPreprocessUtil {

    private ConditionPreprocessUtil() {
        // utility class
    }

    public static void convertExistsFieldExpressions(MapCondition condition) {
        Map.Entry entry = condition.getMap().entrySet().iterator().next();
        String expressionName = (String) entry.getKey();
        if (expressionName.equals("AllCondition")) {
            List<Map> innerMaps = (List<Map>) entry.getValue();

            if (innerMaps.size() >= 2 && innerMaps.stream().allMatch(map -> map.keySet().stream().anyMatch(key -> key.equals("IsDefinedExpression") || key.equals("IsNotDefinedExpression")))) {
                Map andExpression = createAndExpression(innerMaps.get(0), innerMaps.get(1));

                for (int i = 2; i < innerMaps.size(); i++) {
                    Map outerAndExpression = createAndExpression(andExpression, innerMaps.get(i));
                    andExpression = outerAndExpression;
                }
                condition.setMap(andExpression);
            }
        }
    }

    private static Map createAndExpression(Map lhs, Map rhs) {
        Map andExpressionValue = new LinkedHashMap();
        andExpressionValue.put("lhs", lhs);
        andExpressionValue.put("rhs", rhs);
        Map andExpression = new LinkedHashMap();
        andExpression.put("AndExpression", andExpressionValue);
        return andExpression;
    }
}
