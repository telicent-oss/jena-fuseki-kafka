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

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.shared.JenaException;

import java.util.Optional;

/**
 * Utility class for extracting configuration from environment variables or System Properties.
 * Expected string formats: value, env:value, env:{value}, env:{value:default}
 */
public class EnvVariables {

    /**
     * Marking private to avoid accidental usage.
     */
    private EnvVariables(){}

    private static final String ENV_PREFIX = "env:";

    /**
     * Checks for environment variables (or System properties) embedded in given value string.
     * @param configName the name of the config being set
     * @param embeddedString the string with potential env variable expression embedded in it
     * @return value, or if provided the relevant environment variable value (or system property)
     */
    public static String checkForEnvironmentVariableValue(String configName, String embeddedString) {
        if (embeddedString == null) {
            return null;
        }
        if (!embeddedString.startsWith(ENV_PREFIX)) {
            return embeddedString;
        }
        String envVarExpression = embeddedString.substring(ENV_PREFIX.length());
        return resolveEnvVarExpression(configName, envVarExpression);
    }

    /**
     * Resolves the environment variable expression.
     * @param configName the name of the config being set
     * @param envVarExpression the expression to resolve
     * @return the resolved value
     */
    private static String resolveEnvVarExpression(String configName, String envVarExpression) {
        if (envVarExpression.startsWith("{") && envVarExpression.endsWith("}")) {
            String innerExpression = envVarExpression.substring(1, envVarExpression.length() - 1);
            int colonIndex = innerExpression.indexOf(':');
            if (colonIndex != -1) {
                String envVarName = innerExpression.substring(0, colonIndex);
                String defaultValue = innerExpression.substring(colonIndex + 1);
                return getEnvVarOrDefault(envVarName, defaultValue);
            } else {
                return getEnvVarOrThrow(innerExpression, configName, envVarExpression);
            }
        } else {
            return getEnvVarOrThrow(envVarExpression, configName, envVarExpression);
        }
    }

    /**
     * Gets the value of an environment variable or throws an exception if not set.
     * @param envVarName the name of the environment variable
     * @param configName the name of the config being set
     * @return the value of the environment variable
     */
    private static String getEnvVarOrThrow(String envVarName, String configName, String envVarExpression) {
        return Optional.ofNullable(lookupEnvironmentVariable(envVarName))
                       .orElseThrow(() -> new JenaException(
                               String.format("Environment variable %s not set for %s", envVarName, configName)
                       ));
    }

    /**
     * Gets the value of an environment variable or returns a default value if not set.
     * @param envVarName the name of the environment variable
     * @param defaultValue the default value to return if the environment variable is not set
     * @return the value of the environment variable or the default value
     */
    private static String getEnvVarOrDefault(String envVarName, String defaultValue) {
        return Optional.ofNullable(lookupEnvironmentVariable(envVarName)).orElse(defaultValue);
    }

    /**
     * Looks up the value of an environment variable.
     * @param name the name of the environment variable
     * @return the value of the environment variable or null if not set
     */
    private static String lookupEnvironmentVariable(String name) {
        return Lib.getenv(name);
    }
}
