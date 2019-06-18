package com.kineticdata.bridgehub.adapter.amazonecs;

import com.kineticdata.bridgehub.adapter.QualificationParser;

public class AmazonEcsQualificationParser extends QualificationParser {
    public String encodeParameter(String name, String value) {
        return value;
    }
}
