/*
 *  Copyright (c) Telicent Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.jena.kafka.utils;

import org.apache.jena.shared.JenaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class TestEnvVariables {

    private static final String TEST_ENV_VAR = "TEST_ENV_VAR";

    @AfterEach
    public void tearDown() {
        System.clearProperty(TEST_ENV_VAR);
    }

    private void setEnv(String value) {
        System.setProperty(TEST_ENV_VAR, value);
    }

    @Test
    public void testPlainValue() {
        // given, when, then
        String result = EnvVariables.checkForEnvironmentVariableValue("testConfig", "plainValue");
        assertEquals("plainValue", result);
    }

    @Test
    public void testNullValue() {
        // given, when, then
        String result = EnvVariables.checkForEnvironmentVariableValue("testConfig", null);
        assertNull(result);
    }

    @Test
    public void testEnvValue() {
        // given
        String envVarValue = "envValue";
        setEnv(envVarValue);
        // when
        String result = EnvVariables.checkForEnvironmentVariableValue("testConfig", "env:" + TEST_ENV_VAR);
        // then
        assertEquals(envVarValue, result);
    }

    @Test
    public void testEnvValueWithBraces() {
        // given
        String envVarValue = "envValue";
        setEnv(envVarValue);
        // when
        String result = EnvVariables.checkForEnvironmentVariableValue("testConfig", "env:{" + TEST_ENV_VAR + "}");
        // then
        assertEquals(envVarValue, result);
    }

    @Test
    public void testEnvValueWithDefault() {
        // given
        String envVarName = "TEST_ENV_VAR_DEFAULT";
        String defaultValue = "defaultValue";
        // when
        String result = EnvVariables.checkForEnvironmentVariableValue("testConfig", "env:{" + envVarName + ":" + defaultValue + "}");
        // then
        assertEquals(defaultValue, result);
    }

    @Test
    public void testEnvValueWithDefaultAndEnvSet() {
        // given
        String envVarName = "TEST_ENV_VAR";
        String envVarValue = "envValue";
        String defaultValue = "defaultValue";
        setEnv(envVarValue);
        // when
        String result = EnvVariables.checkForEnvironmentVariableValue("testConfig", "env:{" + envVarName + ":" + defaultValue + "}");
        // then
        assertEquals(envVarValue, result);
        System.clearProperty(envVarName);
    }

    @Test
    public void testEnvValueNotSet() {
        String envVarName = "MISSING_TEST_ENV_VAR";
        assertThrows(JenaException.class, () -> {
            EnvVariables.checkForEnvironmentVariableValue("testConfig", "env:{" + envVarName + "}");
        });
    }

    @Test
    public void testInvalidEnvExpression() {
        assertThrows(JenaException.class, () -> {
            EnvVariables.checkForEnvironmentVariableValue("testConfig", "env:{INVALID_EXPRESSION");
        });
    }
}
