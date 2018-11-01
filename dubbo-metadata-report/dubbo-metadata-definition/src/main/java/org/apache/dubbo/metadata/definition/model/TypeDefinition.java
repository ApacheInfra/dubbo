/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.metadata.definition.model;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 2015/1/27.
 */
public class TypeDefinition {

    private String id;
    private String type;
    @SerializedName("items")
    private List<TypeDefinition> items;
    @SerializedName("enum")
    private List<String> enums;
    private String $ref;
    private Map<String, TypeDefinition> properties;

    public TypeDefinition(String type) {
        this.type = type;
    }

    public String get$ref() {
        return $ref;
    }

    public List<String> getEnums() {
        if (enums == null) {
            enums = new ArrayList<String>();
        }
        return enums;
    }

    public String getId() {
        return id;
    }

    public List<TypeDefinition> getItems() {
        if (items == null) {
            items = new ArrayList<TypeDefinition>();
        }
        return items;
    }

    public Map<String, TypeDefinition> getProperties() {
        if (properties == null) {
            properties = new HashMap<String, TypeDefinition>();
        }
        return properties;
    }

    public String getType() {
        return type;
    }

    public void set$ref(String $ref) {
        this.$ref = $ref;
    }

    public void setEnums(List<String> enums) {
        this.enums = enums;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setItems(List<TypeDefinition> items) {
        this.items = items;
    }

    public void setProperties(Map<String, TypeDefinition> properties) {
        this.properties = properties;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "TypeDefinition [id=" + id + ", type=" + type + ", properties=" + properties + ", $ref=" + $ref + "]";
    }

}
