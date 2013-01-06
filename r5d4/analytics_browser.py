from __future__ import absolute_import
from flask import abort
from werkzeug.exceptions import ServiceUnavailable
from r5d4.analytics import Analytics
from r5d4.flask_redis import get_conf_db, get_data_db
from r5d4.mapping_functions import DIMENSION_EXPANSION_MAP
from r5d4.utility import construct_key


def combinatorial_keys(rem_range):
    """
    >>> list(combinatorial_keys([("d1", [1,2]), ("d2", [3,4])])) == \
      [('d1', 1, 'd2', 3), ('d1', 1, 'd2', 4), ('d1', 2, 'd2', 3), \
       ('d1', 2, 'd2', 4)]
    True
    """
    if not rem_range:
        yield ()
        return
    dimension, dim_range = rem_range[0]
    for dim_value in dim_range:
        for rest_key in combinatorial_keys(rem_range[1:]):
            yield (dimension, dim_value) + rest_key
    return


def browse_analytics(a_name, slice_args):
    conf_db = get_conf_db()
    if not conf_db.sismember("Analytics:Active", a_name):
        abort(404)
    analytics_definition = conf_db.get("Analytics:ByName:%s" % a_name)
    if analytics_definition is None:
        abort(404)
    try:
        analytics = Analytics(analytics_definition)
    except (ValueError, AssertionError) as e:
        raise ServiceUnavailable(e.args)
    data_db = get_data_db(analytics["data_db"])

    mapping = analytics["mapping"]
    measures = analytics["measures"]
    query_dimensions = set(analytics["query_dimensions"])
    slice_dimensions = set(analytics["slice_dimensions"])

    d_range = []
    for d in slice_dimensions:
        expand = DIMENSION_EXPANSION_MAP[mapping[d]["type"]]
        try:
            value_set = expand(slice_args[d])
            d_range.append((d, value_set))
        except ValueError as e:
            abort(400, e.args)
        except KeyError as e:
            abort(400, ("Missing slice parameter", str(e.args[0])))

    d_range_dict = dict(d_range)

    def get_range(dimensions):
        d_range = map(lambda d: (d, sorted(list(d_range_dict[d]))),
                      sorted(list(dimensions)))
        return d_range

    qnos_dimensions = query_dimensions - slice_dimensions
    snoq_dimensions = slice_dimensions - query_dimensions

    s_range = get_range(slice_dimensions)
    snoq_range = get_range(snoq_dimensions)

    for qnos in qnos_dimensions:
        d_range_dict[qnos] = set()
        for s_key in combinatorial_keys(s_range):
            refcount_key_str = construct_key('RefCount', s_key, qnos)
            d_range_dict[qnos] |= set(data_db.hkeys(refcount_key_str))

    q_range = get_range(query_dimensions)
    output = []
    for q_key in combinatorial_keys(q_range):
        row = {}
        key_is_set = False
        key = None
        for q in q_key:  # q_key=(Date,20110808,Practice,1)
            if not key_is_set:
                key = q
                key_is_set = True
            else:
                row[key] = q
                key_is_set = False

        for measure in measures:
            if mapping[measure]["type"][-5:] == "float":
                is_float = True
            else:
                is_float = False
            row[measure] = 0
            snoq_keys = list(combinatorial_keys(snoq_range))
            if len(snoq_keys) < 2:
                if len(snoq_keys) == 1:
                    snoq_key = snoq_keys[0]
                else:
                    snoq_key = None
                val_key = construct_key(measure, q_key, snoq_key)
                if mapping[measure]["type"] == "unique":
                    val = data_db.scard(val_key)
                else:
                    val = data_db.get(val_key)
                if val:
                    if is_float:
                        row[measure] = float(val)
                    else:
                        row[measure] = int(val)
            else:
                for snoq_key in snoq_keys:
                    val_key = construct_key(measure, q_key, snoq_key)
                    if mapping[measure]["type"] == "unique":
                        abort(400, (
                            "Measure type 'unique' cannot be aggregated"))
                    else:
                        val = data_db.get(val_key)
                    if val:
                        if is_float:
                            row[measure] += float(val)
                        else:
                            row[measure] += int(val)
        output.append(row)
    output_response = {
        "status": "OK",
        "data": output
    }
    return output_response
