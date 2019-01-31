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

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalStringProperty;

public class TranslateProcessor extends AbstractProcessor {

    public static final String TYPE = "translate";

    private final String field;
    private final String targetField;
    private final Map<String, String> dictionary;
    private final boolean ignoreMissing;
    private final String defaultValue;

    public TranslateProcessor(String tag, String field, String targetField, boolean ignoreMissing, String defaultValue,
                              Map<String, String> dictionary) throws IOException {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.dictionary = dictionary;
        this.ignoreMissing = ignoreMissing;
        this.defaultValue = defaultValue;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {

        Object oldValue = ingestDocument.getFieldValue(field, Object.class, ignoreMissing);
        Object newValue;

        if (oldValue == null && ignoreMissing) {
            return ingestDocument;
        } else if (oldValue == null) {
            throw new IllegalArgumentException("Field [" + field + "] is null, cannot be translated");
        }

        if (oldValue instanceof List) {
            List<?> list = (List<?>)oldValue;
            List<String> newList = new ArrayList<>(list.size());
            for (Object item : list) {
                newList.add(translate(item));
            }
            newValue = newList;
        } else {
            newValue = translate(oldValue);
        }
        ingestDocument.setFieldValue(targetField, newValue);
        return ingestDocument;
    }


    // TODO: support only String values in target
    // TODO: should we support `itarate_on`?
    private String translate(Object key) throws IllegalArgumentException {
        String value;
        if (key instanceof String) {
            value = this.dictionary.getOrDefault(key, this.defaultValue);
        } else {
            //FIXME how to support not String values in source field
            // option 1. use convert before this processor -> if non-string -> ERROR. Impl is same Option 3
            // option 2. set TYPE if user know the key datatype
            // option 3. not support non-string values in source field -> ERROR
            //String keyStr = "";
            //value = this.dictionary.getOrDefault(key, this.defaultValue);
            throw new IllegalArgumentException("Field [" + field + "] has non-string values. " +
                    "Translate Processor only support string value currently");
        }
        return value;
    }

    @Override
    public String getType() {
        return TYPE;
    }


    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
    }

    Map<String, String> getDictionary() {
        return dictionary;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    String getDefaultValue() {
        return defaultValue;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public TranslateProcessor create(Map<String, Processor.Factory> factories, String tag, Map<String, Object> config) 
            throws Exception {
            String field = readStringProperty(TYPE, tag, config, "field");
            String targetField = readStringProperty(TYPE, tag, config, "target_field");
            boolean ignoreMissing = readBooleanProperty(TYPE, tag, config, "ignore_missing", true);
            String defaultValue = readOptionalStringProperty(TYPE, tag, config, "default");
            // TODO: how to handle duplicate keys? write log and use later key
            // TODO: support dictionary file
            // support only String values in dictionary -> if a user want to cast other type, they can use `convert` processor
            // TODO: List<String> v.s. Map<String, String>
            // FIXME :test it works
            Map<String, String> dictionary = ConfigurationUtils.readMap(TYPE, tag, config, "dictionary");
            if (dictionary.isEmpty()) {
                throw new IllegalArgumentException("\"dictionary\" is empty");
            }

            return new TranslateProcessor(tag, field, targetField, ignoreMissing, defaultValue ,dictionary);
        }
    }
}
