/*
 * Copyright [2018] [Jun Ohtani]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.elasticsearch.plugin.ingest.translate;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class TranslateProcessorTests extends ESTestCase {

    public void testRequireSettings() throws Exception {
        TranslateProcessor.Factory factory = new TranslateProcessor.Factory();
        String processorTag = randomAlphaOfLength(10);
        Map<String, Object> config = new HashMap<>();

        // field parameter
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, config));
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));

        // target_field parameter
        config.put("field", "source_field");
        e = expectThrows(ElasticsearchParseException.class,
                () -> factory.create(null, processorTag, config));
        assertThat(e.getMessage(), equalTo("[target_field] required property is missing"));

        // dictionary parameter
        config.put("field", "source_field");
        config.put("target_field", "target_field");
        e = expectThrows(ElasticsearchParseException.class,
                () -> factory.create(null, processorTag, config));
        assertThat(e.getMessage(), equalTo("[dictionary] required property is missing"));

        // non-support object in dictionary
        config.put("field", "source_field");
        config.put("target_field", "target_field");
        config.put("dictionary", "string");
        e = expectThrows(ElasticsearchParseException.class,
                () -> factory.create(null, processorTag, config));
        assertThat(e.getMessage(), equalTo("[dictionary] property isn't a map, but of type [java.lang.String]"));

        // empty dictionary
        config.put("field", "source_field");
        config.put("target_field", "target_field");
        config.put("dictionary", new HashMap<String, String>());
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> factory.create(null, processorTag, config));
        assertThat(iae.getMessage(), equalTo("\"dictionary\" is empty"));
    }

    public void testMinimalSuccess() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "10");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        Map<String, String> dictionary = new HashMap<String, String>();
        dictionary.put("10", "success");
        dictionary.put("20", "fail");

        TranslateProcessor processor = new TranslateProcessor(
                randomAlphaOfLength(10), "source_field", "target_field",
                false, "", dictionary);

        Map<String, Object> data = processor.execute(ingestDocument).getSourceAndMetadata();

        assertThat(data, hasKey("target_field"));
        assertThat(data.get("target_field"), is("success"));
    }

    public void testIgnoreMissingFalse() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("field", "10");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        Map<String, String> dictionary = new HashMap<String, String>();
        dictionary.put("10", "success");
        dictionary.put("20", "fail");

        TranslateProcessor processor = new TranslateProcessor(
                randomAlphaOfLength(10), "source_field", "target_field",
                false, "", dictionary);

        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> processor.execute(ingestDocument).getSourceAndMetadata());
        assertThat(iae.getMessage(), equalTo("field [source_field] not present as part of path [source_field]"));
    }

    public void testIgnoreMissingTrue() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("field", "10");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        Map<String, String> dictionary = new HashMap<String, String>();
        dictionary.put("10", "success");
        dictionary.put("20", "fail");

        TranslateProcessor processor = new TranslateProcessor(
                randomAlphaOfLength(10), "source_field", "target_field",
                true, "", dictionary);

        Map<String, Object> data = processor.execute(ingestDocument).getSourceAndMetadata();

        assertThat(data.get("target_field"), nullValue());
    }

    // TODO unchecked during load dictionary from config
    public void testSetNonStringInDictionaryValue() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "10");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        Map<String, Integer> dictionary = new HashMap<String, Integer>();
        dictionary.put("10", 100);
        dictionary.put("20", 200);

        TranslateProcessor.Factory factory = new TranslateProcessor.Factory();
        String processorTag = randomAlphaOfLength(10);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("target_field", "target_field");
        config.put("dictionary", dictionary);
        TranslateProcessor processor = factory.create(null, processorTag, config);

        ClassCastException cce = expectThrows(ClassCastException.class,
                () -> processor.execute(ingestDocument).getSourceAndMetadata());
        assertThat(cce.getMessage(), startsWith("class java.lang.Integer cannot be cast to class java.lang.String"));
    }

    public void testNoDefault() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "30");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        Map<String, String> dictionary = new HashMap<String, String>();
        dictionary.put("10", "success");
        dictionary.put("20", "fail");

        TranslateProcessor processor = new TranslateProcessor(
                randomAlphaOfLength(10), "source_field", "target_field",
                false, null, dictionary);

        Map<String, Object> data = processor.execute(ingestDocument).getSourceAndMetadata();

        assertThat(data, hasKey("target_field"));
        assertThat(data.get("target_field"), nullValue());
    }

    public void testWithDefault() throws Exception {
        Map<String, Object> document = new HashMap<>();
        document.put("source_field", "30");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        Map<String, String> dictionary = new HashMap<String, String>();
        dictionary.put("10", "success");
        dictionary.put("20", "fail");

        TranslateProcessor processor = new TranslateProcessor(
                randomAlphaOfLength(10), "source_field", "target_field",
                false, "default value", dictionary);

        Map<String, Object> data = processor.execute(ingestDocument).getSourceAndMetadata();

        assertThat(data, hasKey("target_field"));
        assertThat(data.get("target_field"), is("default value"));
    }

    public void testArrayField() throws Exception {
        Map<String, Object> document = new HashMap<>();
        List<String> values = new ArrayList<String>();
        values.add("10");
        values.add("20");
        values.add("10");
        document.put("source_field", values);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        Map<String, String> dictionary = new HashMap<String, String>();
        dictionary.put("10", "success");
        dictionary.put("20", "fail");

        TranslateProcessor processor = new TranslateProcessor(
                randomAlphaOfLength(10), "source_field", "target_field",
                false, "default value", dictionary);

        Map<String, Object> data = processor.execute(ingestDocument).getSourceAndMetadata();

        assertThat(data, hasKey("target_field"));
        List<String> expectedValues = new ArrayList<String>();
        expectedValues.add("success");
        expectedValues.add("fail");
        expectedValues.add("success");
        assertThat(data.get("target_field"), is(expectedValues));
    }

    public void testDuplicateKeyInDictionary() throws Exception {
        // This should be in RestIT

    }

}

