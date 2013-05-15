/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 * Copyright 2013 Altamira Corporation.
 */


package com.nearinfinity.honeycomb.config;

import com.google.common.io.InputSupplier;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.InputStream;

import static java.lang.String.format;

public class ConfigurationParserTest {
    private static final String ROOT_XML_DOC = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
    private static final String SCHEMA = ROOT_XML_DOC +
            "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">" +
            "<xs:element name=\"foo\" type=\"xs:string\" />" +
            "</xs:schema>";

    private static final String ADAPTER = "adapter1";
    private static final String OPTION1_NAME = "option1";
    private static final String OPTION2_NAME = "option2";
    private static final String OPTION1_VALUE = "value1";
    private static final String OPTION2_VALUE = "value2";

    private static final String CONFIG = "<options><adapters>" +
            format("<adapter name=\"%s\">", ADAPTER) +
            "<configuration>" +
            format("<%s>%s</%s>", OPTION1_NAME, OPTION1_VALUE, OPTION1_NAME) +
            format("<%s>%s</%s>", OPTION2_NAME, OPTION2_VALUE, OPTION2_NAME) +
            "</configuration></adapter></adapters></options>";

    @Mock
    private InputSupplier<InputStream> configSupplier;
    private InputSupplier<InputStream> schemaSupplier;

    @Before
    public void setupTestCase() {
        MockitoAnnotations.initMocks(this);
    }

    @Test(expected = NullPointerException.class)
    public void testParseNullConfigPath() {
        ConfigurationParser.parseConfiguration(null, "/foo");
    }

    @Test(expected = NullPointerException.class)
    public void testParseNullSchemaPath() {
        ConfigurationParser.parseConfiguration("/foo", null);
    }
}