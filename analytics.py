#!/usr/bin/env python
from __future__ import absolute_import
import sys
import json
from r5d4.mapping_functions import MEASURING_FUNCTIONS_MAP,\
    DIMENSION_PARSERS_MAP, CONDITION_KEYS

TOP_KEYS = ["name", "description", "query_dimensions", "slice_dimensions",
            "data_db", "measures", "mapping"]


class Analytics:
    def __init__(self, analytics_definition):
        """
        Constructor for Analytics class.
        Deserializes analytics from a json format definition
        Throws ValueError if the JSON is not parseable
        Throws AssertError if the Analytics definition is invalid
        """
        self.definition = json.loads(analytics_definition)
        self.validate()

    def json_serialize(self, fp=None, indent=2):
        """
        Serialize Analytics definition and return json
        """
        if fp is None:
            return json.dumps(self.definition, indent=indent, sort_keys=True)
        else:
            return json.dump(self.definition,
                             fp,
                             indent=indent,
                             sort_keys=True)

    def validate(self):
        """
        Validates Analytics definition and raises AssertError if invalid.
        """

        # Checking for top-level keys
        assert "name" in self.definition, \
            "Definition doesn't have 'name'"
        assert ':' not in self.definition['name'], \
            "Analytics name cannot contain ':'"
        assert "measures" in self.definition, \
            "Definition doesn't contain 'measures' array"
        assert "query_dimensions" in self.definition, \
            "Definition doesn't contain 'query_dimensions' array"
        assert "slice_dimensions" in self.definition, \
            "Definition doesn't contain 'slice_dimensions' array"
        assert "mapping" in self.definition, \
            "Definition doesn't contain 'mapping' dictionary"
        for top_key in self.definition.keys():
            assert top_key in TOP_KEYS, \
                "Definition has unexpected key '%(top_key)s'" % locals()

        mapping = self.definition["mapping"]
        mapped_measures = set()
        mapped_dimensions = set()

        # Checking if atleast one measure is present
        assert len(self.definition["measures"]) > 0, \
            "Definition should contain atleast one measure"

        for measure in self.definition["measures"]:

            # Checking if measure has mapping
            assert measure in mapping, \
                "Measure '%s' doesn't have a mapping" % measure
            mapped_measures.add(measure)
            # Checking if resource is present
            assert "resource" in mapping[measure], \
                "Measure '%s' is missing 'resource'" % measure

            # Checking type of measure
            assert "type" in mapping[measure], \
                "Measure '%s' is missing 'type'" % measure
            assert mapping[measure]["type"] in MEASURING_FUNCTIONS_MAP, \
                "Measure '%s' type '%s' is not a valid measure type" % (
                    measure,
                    mapping[measure]["type"])

            if mapping[measure]["type"] == "score":
                assert "field" in mapping[measure], \
                    "Measure '%s' has type 'score' but missing 'field'" % \
                    measure
            if mapping[measure]["type"] == "unique":
                assert "field" in mapping[measure], \
                    "Measure '%s' has type 'unique' but missing 'field'" % \
                    measure
            if "conditions" in mapping[measure]:
                for condition in mapping[measure]["conditions"]:
                    assert "field" in condition, \
                        "Conditional measure '%s' missing 'field' in one of " \
                        "the conditions" % measure
                    filter_count = 0
                    for condition_key in CONDITION_KEYS:
                        if condition_key in condition:
                            filter_count += 1
                    assert filter_count > 0, \
                        "Conditional measure '%s' field '%s' has no " \
                        "conditions" % (measure, condition["field"])
                    assert filter_count <= 1, \
                        "Conditional measure '%s' field '%s' has " \
                        "> 1 conditions" % (measure, condition["field"])

        for dimension in self.definition["query_dimensions"] \
                + self.definition["slice_dimensions"]:
            # Checking if all dimensions are mapped
            assert dimension in mapping, \
                "Dimension '%s' doesn't have a mapping" % dimension
            mapped_dimensions.add(dimension)

            # Checking type of dimension
            assert "type" in mapping[dimension], \
                "Dimension '%s' is missing 'type'" % dimension
            assert mapping[dimension]["type"] in DIMENSION_PARSERS_MAP, \
                "Dimension '%s' type '%s' is not valid dimension type" % (
                    dimension,
                    mapping[dimension]["type"])

            # Checking if field is present
            assert "field" in mapping[dimension], \
                "Dimension '%s' is missing 'field'" % dimension

        unmapped = set(mapping.keys()) - (mapped_measures | mapped_dimensions)
        assert unmapped == set(), \
            "Unmapped keys in mapping: [%s]" % ",".join(unmapped)

    def set_data_db(self, data_db):
        self.definition["data_db"] = data_db

    def __getitem__(self, name):
        if name in self.definition:
            return self.definition[name]
        return None

    def __getattr__(self, name):
        return self.definition[name]

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        for filepath in sys.argv[1:]:
            with open(filepath, 'r') as f:
                try:
                    a = Analytics(f.read())
                except AssertionError, e:
                    sys.stderr.write("%s: %s\n" % (filepath, e))
                except ValueError, e:
                    sys.stderr.write("%s: %s\n" % (filepath, e))
