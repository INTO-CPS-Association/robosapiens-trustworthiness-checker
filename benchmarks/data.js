window.BENCHMARK_DATA = {
  "lastUpdate": 1784675864952,
  "repoUrl": "https://github.com/INTO-CPS-Association/robosapiens-trustworthiness-checker",
  "entries": {
    "RoboSAPIENS Criterion benchmarks": [
      {
        "commit": {
          "author": {
            "email": "tom.tdw@gmail.com",
            "name": "Thomas Wright",
            "username": "twright"
          },
          "committer": {
            "email": "tom.tdw@gmail.com",
            "name": "Thomas Wright",
            "username": "twright"
          },
          "distinct": true,
          "id": "7f57bf150324966cc49a89ba2afa7125a9bd8b2b",
          "message": "Fix handling of dataflow dependency cycles",
          "timestamp": "2026-07-21T23:42:49+02:00",
          "tree_id": "2ed5020ab474068e58a95fbc0e0d41623796f368",
          "url": "https://github.com/INTO-CPS-Association/robosapiens-trustworthiness-checker/commit/7f57bf150324966cc49a89ba2afa7125a9bd8b2b"
        },
        "date": 1784675864547,
        "tool": "cargo",
        "benches": [
          {
            "name": "compilation_phases/lalr_parse/32",
            "value": 612925,
            "range": "± 1871",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/strict_type_check/32",
            "value": 25039,
            "range": "± 1024",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/untyped_dependency_graph/32",
            "value": 5086,
            "range": "± 44",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/typed_dependency_graph/32",
            "value": 5110,
            "range": "± 42",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/dataflow_compile_untyped/32",
            "value": 44635,
            "range": "± 2723",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/dataflow_compile_typed/32",
            "value": 44130,
            "range": "± 2567",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/parse_typecheck_compile_typed/32",
            "value": 705130,
            "range": "± 4368",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/lalr_parse/256",
            "value": 1574354,
            "range": "± 10544",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/strict_type_check/256",
            "value": 211060,
            "range": "± 7764",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/untyped_dependency_graph/256",
            "value": 49708,
            "range": "± 381",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/typed_dependency_graph/256",
            "value": 49664,
            "range": "± 512",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/dataflow_compile_untyped/256",
            "value": 628273,
            "range": "± 20121",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/dataflow_compile_typed/256",
            "value": 614261,
            "range": "± 12360",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/parse_typecheck_compile_typed/256",
            "value": 2492761,
            "range": "± 12741",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/lalr_parse/1024",
            "value": 10595285,
            "range": "± 178842",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/strict_type_check/1024",
            "value": 894819,
            "range": "± 68438",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/untyped_dependency_graph/1024",
            "value": 211678,
            "range": "± 2219",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/typed_dependency_graph/1024",
            "value": 209988,
            "range": "± 1993",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/dataflow_compile_untyped/1024",
            "value": 5786323,
            "range": "± 65034",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/dataflow_compile_typed/1024",
            "value": 5789921,
            "range": "± 69558",
            "unit": "ns/iter"
          },
          {
            "name": "compilation_phases/parse_typecheck_compile_typed/1024",
            "value": 17366421,
            "range": "± 94767",
            "unit": "ns/iter"
          },
          {
            "name": "lexical_binding_typecheck/8",
            "value": 2294,
            "range": "± 32",
            "unit": "ns/iter"
          },
          {
            "name": "lexical_binding_typecheck/64",
            "value": 16358,
            "range": "± 53",
            "unit": "ns/iter"
          },
          {
            "name": "lexical_binding_typecheck/256",
            "value": 92363,
            "range": "± 353",
            "unit": "ns/iter"
          },
          {
            "name": "indexed_arena_production_comparison/parse_production_arena/127",
            "value": 559593,
            "range": "± 2403",
            "unit": "ns/iter"
          },
          {
            "name": "indexed_arena_production_comparison/typecheck_production_arena/127",
            "value": 9363,
            "range": "± 377",
            "unit": "ns/iter"
          },
          {
            "name": "indexed_arena_production_comparison/compile_production_arena/127",
            "value": 9292,
            "range": "± 251",
            "unit": "ns/iter"
          },
          {
            "name": "indexed_arena_production_comparison/evaluate_production_arena_plan/127",
            "value": 2595,
            "range": "± 38",
            "unit": "ns/iter"
          },
          {
            "name": "indexed_arena_production_comparison/parse_production_arena/1023",
            "value": 948869,
            "range": "± 2941",
            "unit": "ns/iter"
          },
          {
            "name": "indexed_arena_production_comparison/typecheck_production_arena/1023",
            "value": 70509,
            "range": "± 4116",
            "unit": "ns/iter"
          },
          {
            "name": "indexed_arena_production_comparison/compile_production_arena/1023",
            "value": 55985,
            "range": "± 879",
            "unit": "ns/iter"
          },
          {
            "name": "indexed_arena_production_comparison/evaluate_production_arena_plan/1023",
            "value": 20880,
            "range": "± 170",
            "unit": "ns/iter"
          },
          {
            "name": "indexed_arena_production_comparison/parse_production_arena/8191",
            "value": 4031818,
            "range": "± 46837",
            "unit": "ns/iter"
          },
          {
            "name": "indexed_arena_production_comparison/typecheck_production_arena/8191",
            "value": 580817,
            "range": "± 68958",
            "unit": "ns/iter"
          },
          {
            "name": "indexed_arena_production_comparison/compile_production_arena/8191",
            "value": 448544,
            "range": "± 7763",
            "unit": "ns/iter"
          },
          {
            "name": "indexed_arena_production_comparison/evaluate_production_arena_plan/8191",
            "value": 176470,
            "range": "± 883",
            "unit": "ns/iter"
          },
          {
            "name": "specification_import/32_roots/5",
            "value": 98354,
            "range": "± 1676",
            "unit": "ns/iter"
          },
          {
            "name": "specification_import/256_roots/5",
            "value": 804824,
            "range": "± 11189",
            "unit": "ns/iter"
          },
          {
            "name": "specification_import/32_roots/9",
            "value": 1514141,
            "range": "± 24966",
            "unit": "ns/iter"
          },
          {
            "name": "ast_traversal/postorder/1023",
            "value": 1844,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "ast_traversal/fold_node_count/1023",
            "value": 10396,
            "range": "± 324",
            "unit": "ns/iter"
          },
          {
            "name": "ast_traversal/variable_references/1023",
            "value": 11385,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "ast_traversal/free_variables/1023",
            "value": 17016,
            "range": "± 52",
            "unit": "ns/iter"
          },
          {
            "name": "ast_traversal/postorder/8191",
            "value": 14459,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "ast_traversal/fold_node_count/8191",
            "value": 89684,
            "range": "± 2383",
            "unit": "ns/iter"
          },
          {
            "name": "ast_traversal/variable_references/8191",
            "value": 93381,
            "range": "± 179",
            "unit": "ns/iter"
          },
          {
            "name": "ast_traversal/free_variables/8191",
            "value": 142827,
            "range": "± 513",
            "unit": "ns/iter"
          },
          {
            "name": "ast_traversal/postorder/65535",
            "value": 115294,
            "range": "± 137",
            "unit": "ns/iter"
          },
          {
            "name": "ast_traversal/fold_node_count/65535",
            "value": 725004,
            "range": "± 10315",
            "unit": "ns/iter"
          },
          {
            "name": "ast_traversal/variable_references/65535",
            "value": 750181,
            "range": "± 495",
            "unit": "ns/iter"
          },
          {
            "name": "ast_traversal/free_variables/65535",
            "value": 1481640,
            "range": "± 25168",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_recursive/semisync_untyped_runtime/1000",
            "value": 3682071,
            "range": "± 20018",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_recursive/dataflow_untyped_runtime/1000",
            "value": 890249,
            "range": "± 2593",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_recursive/semisync_typed_runtime/1000",
            "value": 2725035,
            "range": "± 65323",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_recursive/dataflow_typed_runtime/1000",
            "value": 904864,
            "range": "± 4738",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_recursive/semisync_untyped_runtime/10000",
            "value": 37139065,
            "range": "± 411125",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_recursive/dataflow_untyped_runtime/10000",
            "value": 8738580,
            "range": "± 20047",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_recursive/semisync_typed_runtime/10000",
            "value": 27519704,
            "range": "± 128719",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_recursive/dataflow_typed_runtime/10000",
            "value": 8797928,
            "range": "± 18381",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_recursive/semisync_untyped_runtime/50000",
            "value": 191673441,
            "range": "± 849557",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_recursive/dataflow_untyped_runtime/50000",
            "value": 43945220,
            "range": "± 412467",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_recursive/semisync_typed_runtime/50000",
            "value": 142162498,
            "range": "± 1641179",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_recursive/dataflow_typed_runtime/50000",
            "value": 44019658,
            "range": "± 245555",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_arithmetic/semisync_untyped_runtime/1000",
            "value": 2667961,
            "range": "± 5056",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_arithmetic/dataflow_untyped_runtime/1000",
            "value": 620018,
            "range": "± 1609",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_arithmetic/semisync_typed_runtime/1000",
            "value": 2251432,
            "range": "± 3644",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_arithmetic/dataflow_typed_runtime/1000",
            "value": 620173,
            "range": "± 1452",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_arithmetic/semisync_untyped_runtime/10000",
            "value": 26660833,
            "range": "± 107881",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_arithmetic/dataflow_untyped_runtime/10000",
            "value": 6334242,
            "range": "± 26387",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_arithmetic/semisync_typed_runtime/10000",
            "value": 22727299,
            "range": "± 102436",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_arithmetic/dataflow_typed_runtime/10000",
            "value": 6200849,
            "range": "± 7828",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_arithmetic/semisync_untyped_runtime/50000",
            "value": 137456023,
            "range": "± 466027",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_arithmetic/dataflow_untyped_runtime/50000",
            "value": 31205991,
            "range": "± 473944",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_arithmetic/semisync_typed_runtime/50000",
            "value": 116037529,
            "range": "± 505043",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_arithmetic/dataflow_typed_runtime/50000",
            "value": 31481729,
            "range": "± 383625",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_stream_dependency/semisync_untyped_runtime/1000",
            "value": 3750110,
            "range": "± 17499",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_stream_dependency/dataflow_untyped_runtime/1000",
            "value": 726923,
            "range": "± 1737",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_stream_dependency/semisync_typed_runtime/1000",
            "value": 3057285,
            "range": "± 10555",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_stream_dependency/dataflow_typed_runtime/1000",
            "value": 725258,
            "range": "± 5363",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_stream_dependency/semisync_untyped_runtime/10000",
            "value": 37864860,
            "range": "± 351659",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_stream_dependency/dataflow_untyped_runtime/10000",
            "value": 7272016,
            "range": "± 15934",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_stream_dependency/semisync_typed_runtime/10000",
            "value": 30400101,
            "range": "± 115856",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_stream_dependency/dataflow_typed_runtime/10000",
            "value": 7336871,
            "range": "± 25386",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_stream_dependency/semisync_untyped_runtime/50000",
            "value": 194065695,
            "range": "± 1078393",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_stream_dependency/dataflow_untyped_runtime/50000",
            "value": 37155169,
            "range": "± 7972245",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_stream_dependency/semisync_typed_runtime/50000",
            "value": 159694331,
            "range": "± 7149154",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_stream_dependency/dataflow_typed_runtime/50000",
            "value": 37418503,
            "range": "± 414498",
            "unit": "ns/iter"
          },
          {
            "name": "function_call_binding/untyped/8",
            "value": 124,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "function_call_binding/checked/8",
            "value": 123,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "function_call_binding/untyped/64",
            "value": 122,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "function_call_binding/checked/64",
            "value": 123,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "function_call_binding/untyped/512",
            "value": 123,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "function_call_binding/checked/512",
            "value": 122,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_arithmetic_monitor/100",
            "value": 24100,
            "range": "± 31",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_function_monitor/100",
            "value": 516215,
            "range": "± 2309",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_arithmetic_monitor/100",
            "value": 24191,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_if_arithmetic_monitor/100",
            "value": 34209,
            "range": "± 108",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_if_arithmetic_monitor/100",
            "value": 34359,
            "range": "± 221",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_function_monitor/100",
            "value": 452602,
            "range": "± 3118",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_direct_if_function_monitor/100",
            "value": 39574,
            "range": "± 233",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_direct_if_function_monitor/100",
            "value": 39810,
            "range": "± 52",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_recursive_if_function_monitor/100",
            "value": 327465,
            "range": "± 563",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_recursive_if_function_monitor/100",
            "value": 323888,
            "range": "± 1166",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_arithmetic_monitor/1000",
            "value": 219216,
            "range": "± 281",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_function_monitor/1000",
            "value": 5047964,
            "range": "± 8916",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_arithmetic_monitor/1000",
            "value": 217543,
            "range": "± 324",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_if_arithmetic_monitor/1000",
            "value": 318134,
            "range": "± 710",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_if_arithmetic_monitor/1000",
            "value": 316743,
            "range": "± 1570",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_function_monitor/1000",
            "value": 4467809,
            "range": "± 53259",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_direct_if_function_monitor/1000",
            "value": 351028,
            "range": "± 2604",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_direct_if_function_monitor/1000",
            "value": 351095,
            "range": "± 741",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_recursive_if_function_monitor/1000",
            "value": 3273632,
            "range": "± 9851",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_recursive_if_function_monitor/1000",
            "value": 3227001,
            "range": "± 10309",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_arithmetic_monitor/5000",
            "value": 1086108,
            "range": "± 1493",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_function_monitor/5000",
            "value": 25178025,
            "range": "± 103608",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_arithmetic_monitor/5000",
            "value": 1079338,
            "range": "± 10693",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_if_arithmetic_monitor/5000",
            "value": 1568156,
            "range": "± 959",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_if_arithmetic_monitor/5000",
            "value": 1552715,
            "range": "± 4894",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_function_monitor/5000",
            "value": 22352264,
            "range": "± 256205",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_direct_if_function_monitor/5000",
            "value": 1726582,
            "range": "± 10678",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_direct_if_function_monitor/5000",
            "value": 1739963,
            "range": "± 4367",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_untyped_recursive_if_function_monitor/5000",
            "value": 16259344,
            "range": "± 66598",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_semantics_function_evaluation/dataflow_typed_recursive_if_function_monitor/5000",
            "value": 16087424,
            "range": "± 52627",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_0/1",
            "value": 16608,
            "range": "± 1939",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_0/1",
            "value": 16511,
            "range": "± 1063",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_1/1",
            "value": 21500,
            "range": "± 4523",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_1/1",
            "value": 16415,
            "range": "± 364",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_5/1",
            "value": 16583,
            "range": "± 1087",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_5/1",
            "value": 16571,
            "range": "± 1171",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_10/1",
            "value": 18121,
            "range": "± 3923",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_10/1",
            "value": 17216,
            "range": "± 3229",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_20/1",
            "value": 16496,
            "range": "± 2381",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_20/1",
            "value": 17349,
            "range": "± 2108",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_50/1",
            "value": 27579,
            "range": "± 2971",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_50/1",
            "value": 16480,
            "range": "± 2896",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_100/1",
            "value": 621999,
            "range": "± 5260",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_100/1",
            "value": 612372,
            "range": "± 927",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_0/100",
            "value": 715030,
            "range": "± 38023",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_0/100",
            "value": 718510,
            "range": "± 10671",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_1/100",
            "value": 1348836,
            "range": "± 95682",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_1/100",
            "value": 1335736,
            "range": "± 53794",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_5/100",
            "value": 3982978,
            "range": "± 192457",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_5/100",
            "value": 3989590,
            "range": "± 173759",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_10/100",
            "value": 6744293,
            "range": "± 175022",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_10/100",
            "value": 6952200,
            "range": "± 173700",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_20/100",
            "value": 13465044,
            "range": "± 297736",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_20/100",
            "value": 12807109,
            "range": "± 277410",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_50/100",
            "value": 31480278,
            "range": "± 540973",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_50/100",
            "value": 31925475,
            "range": "± 434997",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_on_percent_100/100",
            "value": 61109325,
            "range": "± 878707",
            "unit": "ns/iter"
          },
          {
            "name": "simple_and/reconf_ct_off_percent_100/100",
            "value": 61220329,
            "range": "± 874689",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_0/1",
            "value": 16127,
            "range": "± 1345",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_0/1",
            "value": 16320,
            "range": "± 1548",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_1/1",
            "value": 16393,
            "range": "± 1280",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_1/1",
            "value": 16267,
            "range": "± 3267",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_5/1",
            "value": 25126,
            "range": "± 5985",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_5/1",
            "value": 16398,
            "range": "± 621",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_10/1",
            "value": 24428,
            "range": "± 4730",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_10/1",
            "value": 17523,
            "range": "± 4577",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_20/1",
            "value": 18349,
            "range": "± 2422",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_20/1",
            "value": 16263,
            "range": "± 1512",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_50/1",
            "value": 16185,
            "range": "± 3757",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_50/1",
            "value": 16167,
            "range": "± 2307",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_100/1",
            "value": 644910,
            "range": "± 9874",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_100/1",
            "value": 638161,
            "range": "± 19190",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_0/100",
            "value": 688184,
            "range": "± 50849",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_0/100",
            "value": 689305,
            "range": "± 38344",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_1/100",
            "value": 1323447,
            "range": "± 49875",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_1/100",
            "value": 1316375,
            "range": "± 88564",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_5/100",
            "value": 3858635,
            "range": "± 95671",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_5/100",
            "value": 3788590,
            "range": "± 34047",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_10/100",
            "value": 7075293,
            "range": "± 217840",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_10/100",
            "value": 7036872,
            "range": "± 204672",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_20/100",
            "value": 13314221,
            "range": "± 159921",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_20/100",
            "value": 12944227,
            "range": "± 121609",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_50/100",
            "value": 33240595,
            "range": "± 649569",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_50/100",
            "value": 32113936,
            "range": "± 648843",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_on_percent_100/100",
            "value": 63585041,
            "range": "± 955762",
            "unit": "ns/iter"
          },
          {
            "name": "rec_moving_average/reconf_ct_off_percent_100/100",
            "value": 63518701,
            "range": "± 707262",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_0/1",
            "value": 13857,
            "range": "± 1112",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_0/1",
            "value": 13706,
            "range": "± 1501",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_1/1",
            "value": 16155,
            "range": "± 3624",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_1/1",
            "value": 13828,
            "range": "± 1540",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_5/1",
            "value": 13584,
            "range": "± 2946",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_5/1",
            "value": 13500,
            "range": "± 1551",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_10/1",
            "value": 13472,
            "range": "± 2132",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_10/1",
            "value": 14221,
            "range": "± 2041",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_20/1",
            "value": 13821,
            "range": "± 709",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_20/1",
            "value": 15796,
            "range": "± 2917",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_50/1",
            "value": 13524,
            "range": "± 1643",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_50/1",
            "value": 13545,
            "range": "± 289",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_100/1",
            "value": 604816,
            "range": "± 8451",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_100/1",
            "value": 615280,
            "range": "± 8186",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_0/100",
            "value": 530532,
            "range": "± 16870",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_0/100",
            "value": 531642,
            "range": "± 9443",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_1/100",
            "value": 1167468,
            "range": "± 65133",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_1/100",
            "value": 1142268,
            "range": "± 18246",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_5/100",
            "value": 3623192,
            "range": "± 135593",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_5/100",
            "value": 3560163,
            "range": "± 151095",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_10/100",
            "value": 6499771,
            "range": "± 146237",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_10/100",
            "value": 6461659,
            "range": "± 158061",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_20/100",
            "value": 12583178,
            "range": "± 228938",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_20/100",
            "value": 12141874,
            "range": "± 35241",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_50/100",
            "value": 30014556,
            "range": "± 266466",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_50/100",
            "value": 31132746,
            "range": "± 396131",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_on_percent_100/100",
            "value": 59869727,
            "range": "± 837139",
            "unit": "ns/iter"
          },
          {
            "name": "sindex1/reconf_ct_off_percent_100/100",
            "value": 58534367,
            "range": "± 1026544",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_0/1",
            "value": 13453,
            "range": "± 1378",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_0/1",
            "value": 13636,
            "range": "± 2341",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_1/1",
            "value": 14217,
            "range": "± 1423",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_1/1",
            "value": 13668,
            "range": "± 63",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_5/1",
            "value": 13377,
            "range": "± 479",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_5/1",
            "value": 13813,
            "range": "± 2874",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_10/1",
            "value": 13364,
            "range": "± 1884",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_10/1",
            "value": 13461,
            "range": "± 602",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_20/1",
            "value": 13624,
            "range": "± 500",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_20/1",
            "value": 13439,
            "range": "± 102",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_50/1",
            "value": 13429,
            "range": "± 1648",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_50/1",
            "value": 13517,
            "range": "± 1877",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_100/1",
            "value": 605192,
            "range": "± 1417",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_100/1",
            "value": 621186,
            "range": "± 9207",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_0/100",
            "value": 532513,
            "range": "± 42934",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_0/100",
            "value": 532961,
            "range": "± 10278",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_1/100",
            "value": 1239869,
            "range": "± 98103",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_1/100",
            "value": 1131805,
            "range": "± 3663",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_5/100",
            "value": 3582354,
            "range": "± 129177",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_5/100",
            "value": 3496620,
            "range": "± 96854",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_10/100",
            "value": 6675332,
            "range": "± 130435",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_10/100",
            "value": 6410474,
            "range": "± 229357",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_20/100",
            "value": 12811265,
            "range": "± 310363",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_20/100",
            "value": 12224231,
            "range": "± 287827",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_50/100",
            "value": 31181684,
            "range": "± 665700",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_50/100",
            "value": 29499248,
            "range": "± 430170",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_on_percent_100/100",
            "value": 60695550,
            "range": "± 922362",
            "unit": "ns/iter"
          },
          {
            "name": "sindex10/reconf_ct_off_percent_100/100",
            "value": 60816429,
            "range": "± 745833",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_0/1",
            "value": 13993,
            "range": "± 1729",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_0/1",
            "value": 13413,
            "range": "± 127",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_1/1",
            "value": 13535,
            "range": "± 1840",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_1/1",
            "value": 13539,
            "range": "± 420",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_5/1",
            "value": 13668,
            "range": "± 1247",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_5/1",
            "value": 13494,
            "range": "± 2409",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_10/1",
            "value": 13512,
            "range": "± 1588",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_10/1",
            "value": 13557,
            "range": "± 120",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_20/1",
            "value": 13479,
            "range": "± 220",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_20/1",
            "value": 13474,
            "range": "± 1230",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_50/1",
            "value": 13591,
            "range": "± 642",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_50/1",
            "value": 13516,
            "range": "± 151",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_100/1",
            "value": 609802,
            "range": "± 11238",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_100/1",
            "value": 626173,
            "range": "± 7469",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_0/100",
            "value": 519938,
            "range": "± 24661",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_0/100",
            "value": 518457,
            "range": "± 16942",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_1/100",
            "value": 1262016,
            "range": "± 26905",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_1/100",
            "value": 1142409,
            "range": "± 9974",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_5/100",
            "value": 3869563,
            "range": "± 41723",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_5/100",
            "value": 3506283,
            "range": "± 15735",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_10/100",
            "value": 7256829,
            "range": "± 224920",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_10/100",
            "value": 6849137,
            "range": "± 225276",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_20/100",
            "value": 13537766,
            "range": "± 48173",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_20/100",
            "value": 12225861,
            "range": "± 166233",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_50/100",
            "value": 34170635,
            "range": "± 635778",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_50/100",
            "value": 31172763,
            "range": "± 595540",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_on_percent_100/100",
            "value": 65352377,
            "range": "± 1130176",
            "unit": "ns/iter"
          },
          {
            "name": "sindex100/reconf_ct_off_percent_100/100",
            "value": 58740127,
            "range": "± 13670697",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_0/1",
            "value": 32978,
            "range": "± 2489",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_0/1",
            "value": 32192,
            "range": "± 2919",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_1/1",
            "value": 33961,
            "range": "± 7557",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_1/1",
            "value": 32201,
            "range": "± 2149",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_5/1",
            "value": 32113,
            "range": "± 7859",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_5/1",
            "value": 39332,
            "range": "± 6752",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_10/1",
            "value": 33140,
            "range": "± 4861",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_10/1",
            "value": 33550,
            "range": "± 5231",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_20/1",
            "value": 31881,
            "range": "± 2574",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_20/1",
            "value": 37885,
            "range": "± 5167",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_50/1",
            "value": 31837,
            "range": "± 6218",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_50/1",
            "value": 31912,
            "range": "± 4726",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_100/1",
            "value": 755535,
            "range": "± 3731",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_100/1",
            "value": 752584,
            "range": "± 4436",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_0/100",
            "value": 1344753,
            "range": "± 243536",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_0/100",
            "value": 1305067,
            "range": "± 35065",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_1/100",
            "value": 2088511,
            "range": "± 119380",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_1/100",
            "value": 2095140,
            "range": "± 245773",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_5/100",
            "value": 5098535,
            "range": "± 190314",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_5/100",
            "value": 4985957,
            "range": "± 371877",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_10/100",
            "value": 9017042,
            "range": "± 367032",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_10/100",
            "value": 8674514,
            "range": "± 436635",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_20/100",
            "value": 16123217,
            "range": "± 59819",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_20/100",
            "value": 15844465,
            "range": "± 206748",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_50/100",
            "value": 39208363,
            "range": "± 884956",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_50/100",
            "value": 38349475,
            "range": "± 801345",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_on_percent_100/100",
            "value": 75202102,
            "range": "± 683247",
            "unit": "ns/iter"
          },
          {
            "name": "maple_index/reconf_ct_off_percent_100/100",
            "value": 74680421,
            "range": "± 1395119",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_async/1",
            "value": 523531,
            "range": "± 4216",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_async/1",
            "value": 521357,
            "range": "± 966",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_dataflow/1",
            "value": 501221,
            "range": "± 1804",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_semisync/1",
            "value": 503058,
            "range": "± 772",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_dataflow/1",
            "value": 501443,
            "range": "± 1527",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_semisync/1",
            "value": 527432,
            "range": "± 1559",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_async/10",
            "value": 564389,
            "range": "± 826",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_async/10",
            "value": 563585,
            "range": "± 1571",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_dataflow/10",
            "value": 518593,
            "range": "± 1347",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_semisync/10",
            "value": 526271,
            "range": "± 1255",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_dataflow/10",
            "value": 518321,
            "range": "± 1774",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_semisync/10",
            "value": 550604,
            "range": "± 1939",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_async/100",
            "value": 974165,
            "range": "± 2191",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_async/100",
            "value": 977206,
            "range": "± 2245",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_dataflow/100",
            "value": 673591,
            "range": "± 716",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_semisync/100",
            "value": 738890,
            "range": "± 2515",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_dataflow/100",
            "value": 672704,
            "range": "± 792",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_semisync/100",
            "value": 766465,
            "range": "± 1893",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_async/500",
            "value": 2767742,
            "range": "± 4240",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_async/500",
            "value": 2763695,
            "range": "± 4481",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_dataflow/500",
            "value": 1379200,
            "range": "± 4431",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_semisync/500",
            "value": 1700579,
            "range": "± 5662",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_dataflow/500",
            "value": 1384928,
            "range": "± 2520",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_semisync/500",
            "value": 1741560,
            "range": "± 5706",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_async/1000",
            "value": 5033757,
            "range": "± 144953",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_async/1000",
            "value": 5032974,
            "range": "± 11598",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_dataflow/1000",
            "value": 2283696,
            "range": "± 2767",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_semisync/1000",
            "value": 2886794,
            "range": "± 10421",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_dataflow/1000",
            "value": 2261258,
            "range": "± 6985",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_semisync/1000",
            "value": 2952241,
            "range": "± 6630",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_async/2000",
            "value": 9365007,
            "range": "± 19631",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_async/2000",
            "value": 9390914,
            "range": "± 16950",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_dataflow/2000",
            "value": 3962244,
            "range": "± 12256",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_semisync/2000",
            "value": 5222306,
            "range": "± 18076",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_dataflow/2000",
            "value": 3921210,
            "range": "± 11017",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_semisync/2000",
            "value": 5262861,
            "range": "± 8782",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_async/5000",
            "value": 22463911,
            "range": "± 50175",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_async/5000",
            "value": 22456978,
            "range": "± 33173",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_dataflow/5000",
            "value": 8960547,
            "range": "± 12544",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_semisync/5000",
            "value": 12184301,
            "range": "± 55625",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_dataflow/5000",
            "value": 8906792,
            "range": "± 16773",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_semisync/5000",
            "value": 12363522,
            "range": "± 11803",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_async/10000",
            "value": 44012560,
            "range": "± 137889",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_async/10000",
            "value": 44078291,
            "range": "± 115566",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_dataflow/10000",
            "value": 17445651,
            "range": "± 34232",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_semisync/10000",
            "value": 23649898,
            "range": "± 109121",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_dataflow/10000",
            "value": 17205458,
            "range": "± 22321",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_semisync/10000",
            "value": 23762177,
            "range": "± 43281",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_async/25000",
            "value": 109128539,
            "range": "± 453213",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_async/25000",
            "value": 109960802,
            "range": "± 446973",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_dataflow/25000",
            "value": 42762306,
            "range": "± 72968",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dynamic_untyped_semisync/25000",
            "value": 57859713,
            "range": "± 221863",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_dataflow/25000",
            "value": 42199887,
            "range": "± 79371",
            "unit": "ns/iter"
          },
          {
            "name": "dup_defer/dup_defer_untyped_semisync/25000",
            "value": 58808400,
            "range": "± 86353",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_dynamic_phases/dynamic_first_compile",
            "value": 492020,
            "range": "± 8663",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_dynamic_phases/dynamic_cached_tick",
            "value": 1633,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_dynamic_phases/defer_first_compile",
            "value": 490528,
            "range": "± 9604",
            "unit": "ns/iter"
          },
          {
            "name": "dataflow_dynamic_phases/defer_cached_tick",
            "value": 1620,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_direct/1",
            "value": 8396,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_direct_dataflow/1",
            "value": 5341,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_direct/25000",
            "value": 24456226,
            "range": "± 161061",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_direct_dataflow/25000",
            "value": 12993627,
            "range": "± 53114",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_direct/50000",
            "value": 48744590,
            "range": "± 343932",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_direct_dataflow/50000",
            "value": 26902988,
            "range": "± 138314",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_direct/75000",
            "value": 73548780,
            "range": "± 596042",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_direct_dataflow/75000",
            "value": 38733212,
            "range": "± 553475",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_direct/100000",
            "value": 97303271,
            "range": "± 791357",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_direct_dataflow/100000",
            "value": 51792231,
            "range": "± 289275",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_0/1",
            "value": 539364,
            "range": "± 1802",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_0_dataflow/1",
            "value": 515514,
            "range": "± 3712",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_25/1",
            "value": 538262,
            "range": "± 2123",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_25_dataflow/1",
            "value": 516971,
            "range": "± 1510",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_50/1",
            "value": 537475,
            "range": "± 1680",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_50_dataflow/1",
            "value": 514605,
            "range": "± 3109",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_75/1",
            "value": 538162,
            "range": "± 840",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_75_dataflow/1",
            "value": 515240,
            "range": "± 922",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_100/1",
            "value": 16161,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_100_dataflow/1",
            "value": 6918,
            "range": "± 52",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_0/25000",
            "value": 118969808,
            "range": "± 324185",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_0_dataflow/25000",
            "value": 59407250,
            "range": "± 157131",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_25/25000",
            "value": 115083268,
            "range": "± 260014",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_25_dataflow/25000",
            "value": 68684381,
            "range": "± 87591",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_50/25000",
            "value": 111255066,
            "range": "± 314422",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_50_dataflow/25000",
            "value": 77564827,
            "range": "± 322608",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_75/25000",
            "value": 107317140,
            "range": "± 254852",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_75_dataflow/25000",
            "value": 86715652,
            "range": "± 355284",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_100/25000",
            "value": 102493749,
            "range": "± 235291",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_100_dataflow/25000",
            "value": 45427560,
            "range": "± 281177",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_0/50000",
            "value": 239088184,
            "range": "± 805072",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_0_dataflow/50000",
            "value": 118949256,
            "range": "± 638952",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_25/50000",
            "value": 231027838,
            "range": "± 668383",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_25_dataflow/50000",
            "value": 137086543,
            "range": "± 453021",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_50/50000",
            "value": 223004788,
            "range": "± 700145",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_50_dataflow/50000",
            "value": 155770275,
            "range": "± 223822",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_75/50000",
            "value": 216550827,
            "range": "± 592329",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_75_dataflow/50000",
            "value": 176143735,
            "range": "± 270617",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_100/50000",
            "value": 203339033,
            "range": "± 363796",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_100_dataflow/50000",
            "value": 91325266,
            "range": "± 314446",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_0/75000",
            "value": 358110088,
            "range": "± 1339398",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_0_dataflow/75000",
            "value": 177712307,
            "range": "± 614087",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_25/75000",
            "value": 341305466,
            "range": "± 930918",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_25_dataflow/75000",
            "value": 205235919,
            "range": "± 644947",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_50/75000",
            "value": 329281037,
            "range": "± 1477223",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_50_dataflow/75000",
            "value": 231096774,
            "range": "± 601213",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_75/75000",
            "value": 317954281,
            "range": "± 789333",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_75_dataflow/75000",
            "value": 258103335,
            "range": "± 607411",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_100/75000",
            "value": 303165649,
            "range": "± 995097",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_100_dataflow/75000",
            "value": 137929066,
            "range": "± 449311",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_0/100000",
            "value": 476583804,
            "range": "± 1661395",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_0_dataflow/100000",
            "value": 236045220,
            "range": "± 467945",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_25/100000",
            "value": 456827122,
            "range": "± 832756",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_25_dataflow/100000",
            "value": 272569902,
            "range": "± 1041889",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_50/100000",
            "value": 440956174,
            "range": "± 1203433",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_50_dataflow/100000",
            "value": 306966659,
            "range": "± 2308920",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_75/100000",
            "value": 423762987,
            "range": "± 1700420",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_75_dataflow/100000",
            "value": 343022515,
            "range": "± 3043493",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_100/100000",
            "value": 404476399,
            "range": "± 1898730",
            "unit": "ns/iter"
          },
          {
            "name": "dyn_paper/dyn_paper_100_dataflow/100000",
            "value": 181587214,
            "range": "± 699349",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_async/1",
            "value": 32901,
            "range": "± 81",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_dataflow/1",
            "value": 13307,
            "range": "± 38",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_async/1",
            "value": 30988,
            "range": "± 49",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_dataflow/1",
            "value": 12670,
            "range": "± 45",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_async/10",
            "value": 127111,
            "range": "± 160",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_dataflow/10",
            "value": 27234,
            "range": "± 60",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_async/10",
            "value": 116438,
            "range": "± 381",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_dataflow/10",
            "value": 26696,
            "range": "± 272",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_async/100",
            "value": 1070295,
            "range": "± 2497",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_dataflow/100",
            "value": 159236,
            "range": "± 200",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_async/100",
            "value": 976348,
            "range": "± 6495",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_dataflow/100",
            "value": 159571,
            "range": "± 2818",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_async/500",
            "value": 5278386,
            "range": "± 8412",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_dataflow/500",
            "value": 741335,
            "range": "± 2165",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_async/500",
            "value": 4763006,
            "range": "± 33461",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_dataflow/500",
            "value": 741382,
            "range": "± 12535",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_async/1000",
            "value": 10525716,
            "range": "± 13122",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_dataflow/1000",
            "value": 1480784,
            "range": "± 2250",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_async/1000",
            "value": 9549833,
            "range": "± 66601",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_dataflow/1000",
            "value": 1482806,
            "range": "± 26635",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_async/2000",
            "value": 20985471,
            "range": "± 58186",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_dataflow/2000",
            "value": 2944088,
            "range": "± 6658",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_async/2000",
            "value": 19032577,
            "range": "± 101537",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_dataflow/2000",
            "value": 2948432,
            "range": "± 54474",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_async/5000",
            "value": 52584215,
            "range": "± 105596",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_dataflow/5000",
            "value": 7370581,
            "range": "± 19946",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_async/5000",
            "value": 47653581,
            "range": "± 297553",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_dataflow/5000",
            "value": 7382234,
            "range": "± 141136",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_async/10000",
            "value": 104648419,
            "range": "± 180662",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_dataflow/10000",
            "value": 14708651,
            "range": "± 59807",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_async/10000",
            "value": 95502188,
            "range": "± 524818",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_dataflow/10000",
            "value": 14721985,
            "range": "± 272489",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_async/25000",
            "value": 261315551,
            "range": "± 1010917",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_untyped_dataflow/25000",
            "value": 36917117,
            "range": "± 194051",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_async/25000",
            "value": 239496778,
            "range": "± 1872822",
            "unit": "ns/iter"
          },
          {
            "name": "maple_sequence/maple_sequence_typed_dataflow/25000",
            "value": 36639213,
            "range": "± 658302",
            "unit": "ns/iter"
          },
          {
            "name": "parse_small_varied_inputs/parsing_lalrpop/10",
            "value": 539411,
            "range": "± 2096",
            "unit": "ns/iter"
          },
          {
            "name": "parse_small_varied_inputs/parsing_lalrpop/100",
            "value": 736924,
            "range": "± 4173",
            "unit": "ns/iter"
          },
          {
            "name": "parse_small_varied_inputs/parsing_lalrpop/500",
            "value": 1403783,
            "range": "± 5273",
            "unit": "ns/iter"
          },
          {
            "name": "parse_small_varied_inputs/parsing_lalrpop/1000",
            "value": 2199876,
            "range": "± 5656",
            "unit": "ns/iter"
          },
          {
            "name": "parse_small_varied_inputs/parsing_lalrpop/2000",
            "value": 3842441,
            "range": "± 41239",
            "unit": "ns/iter"
          },
          {
            "name": "parse_small_varied_inputs/parsing_lalrpop/5000",
            "value": 8768915,
            "range": "± 40764",
            "unit": "ns/iter"
          },
          {
            "name": "parse_small_varied_inputs/parsing_lalrpop/10000",
            "value": 16652978,
            "range": "± 38689",
            "unit": "ns/iter"
          },
          {
            "name": "DRAFT_DO_NOT_PUBLISH_parse_growing_complexity_input/parsing_lalrpop/1",
            "value": 480738,
            "range": "± 501",
            "unit": "ns/iter"
          },
          {
            "name": "DRAFT_DO_NOT_PUBLISH_parse_growing_complexity_input/parsing_lalrpop/2",
            "value": 484626,
            "range": "± 1446",
            "unit": "ns/iter"
          },
          {
            "name": "DRAFT_DO_NOT_PUBLISH_parse_growing_complexity_input/parsing_lalrpop/4",
            "value": 497140,
            "range": "± 1046",
            "unit": "ns/iter"
          },
          {
            "name": "DRAFT_DO_NOT_PUBLISH_parse_growing_complexity_input/parsing_lalrpop/6",
            "value": 518960,
            "range": "± 1830",
            "unit": "ns/iter"
          },
          {
            "name": "DRAFT_DO_NOT_PUBLISH_parse_growing_complexity_input/parsing_lalrpop/8",
            "value": 520918,
            "range": "± 2527",
            "unit": "ns/iter"
          },
          {
            "name": "DRAFT_DO_NOT_PUBLISH_parse_growing_complexity_input/parsing_lalrpop/10",
            "value": 526596,
            "range": "± 1664",
            "unit": "ns/iter"
          },
          {
            "name": "DRAFT_DO_NOT_PUBLISH_parse_growing_complexity_input/parsing_lalrpop/12",
            "value": 534434,
            "range": "± 2451",
            "unit": "ns/iter"
          },
          {
            "name": "DRAFT_DO_NOT_PUBLISH_parse_growing_complexity_input/parsing_lalrpop/14",
            "value": 532040,
            "range": "± 2057",
            "unit": "ns/iter"
          },
          {
            "name": "DRAFT_DO_NOT_PUBLISH_parse_growing_complexity_input/parsing_lalrpop/16",
            "value": 536216,
            "range": "± 1813",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_async/1",
            "value": 7505,
            "range": "± 99",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_async/1",
            "value": 6622,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_dataflow/1",
            "value": 4157,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_dataflow/1",
            "value": 3676,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_little/1",
            "value": 6188,
            "range": "± 35",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_async/10",
            "value": 12933,
            "range": "± 23",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_async/10",
            "value": 11573,
            "range": "± 49",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_dataflow/10",
            "value": 6783,
            "range": "± 92",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_dataflow/10",
            "value": 6017,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_little/10",
            "value": 19366,
            "range": "± 89",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_async/100",
            "value": 70355,
            "range": "± 158",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_async/100",
            "value": 63284,
            "range": "± 265",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_dataflow/100",
            "value": 30869,
            "range": "± 1137",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_dataflow/100",
            "value": 26882,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_little/100",
            "value": 150875,
            "range": "± 823",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_async/500",
            "value": 318628,
            "range": "± 349",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_async/500",
            "value": 284952,
            "range": "± 1193",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_dataflow/500",
            "value": 137925,
            "range": "± 6005",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_dataflow/500",
            "value": 121750,
            "range": "± 189",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_little/500",
            "value": 721700,
            "range": "± 5496",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_async/1000",
            "value": 632060,
            "range": "± 802",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_async/1000",
            "value": 569546,
            "range": "± 1931",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_dataflow/1000",
            "value": 272128,
            "range": "± 11046",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_dataflow/1000",
            "value": 238354,
            "range": "± 1327",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_little/1000",
            "value": 1447260,
            "range": "± 8924",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_async/2000",
            "value": 1254280,
            "range": "± 1415",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_async/2000",
            "value": 1128193,
            "range": "± 5604",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_dataflow/2000",
            "value": 537571,
            "range": "± 22872",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_dataflow/2000",
            "value": 466492,
            "range": "± 6168",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_little/2000",
            "value": 2891799,
            "range": "± 17929",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_async/5000",
            "value": 3125804,
            "range": "± 7201",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_async/5000",
            "value": 2815033,
            "range": "± 13568",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_dataflow/5000",
            "value": 1341572,
            "range": "± 57189",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_dataflow/5000",
            "value": 1162703,
            "range": "± 1853",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_little/5000",
            "value": 7213422,
            "range": "± 52659",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_async/10000",
            "value": 6224406,
            "range": "± 15098",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_async/10000",
            "value": 5632313,
            "range": "± 38783",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_dataflow/10000",
            "value": 2679905,
            "range": "± 115023",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_dataflow/10000",
            "value": 2333111,
            "range": "± 2019",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_little/10000",
            "value": 14507515,
            "range": "± 85804",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_async/25000",
            "value": 15563644,
            "range": "± 30213",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_async/25000",
            "value": 13874583,
            "range": "± 54363",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_dataflow/25000",
            "value": 6673798,
            "range": "± 286055",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_typed_dataflow/25000",
            "value": 5851276,
            "range": "± 12057",
            "unit": "ns/iter"
          },
          {
            "name": "simple_add/simple_add_untyped_little/25000",
            "value": 35882358,
            "range": "± 148917",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/dsrv_semisync/100",
            "value": 124568,
            "range": "± 219",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/dsrv_dataflow/100",
            "value": 23028,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_runtime_qual/100",
            "value": 50454,
            "range": "± 440",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_runtime_quant/100",
            "value": 50923,
            "range": "± 569",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_direct_qual/100",
            "value": 22164,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_direct_quant/100",
            "value": 22391,
            "range": "± 185",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/dsrv_semisync/1000",
            "value": 1186159,
            "range": "± 2531",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/dsrv_dataflow/1000",
            "value": 184509,
            "range": "± 101",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_runtime_qual/1000",
            "value": 507951,
            "range": "± 4858",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_runtime_quant/1000",
            "value": 507677,
            "range": "± 5131",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_direct_qual/1000",
            "value": 220907,
            "range": "± 150",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_direct_quant/1000",
            "value": 221377,
            "range": "± 2066",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/dsrv_semisync/5000",
            "value": 5882171,
            "range": "± 558472",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/dsrv_dataflow/5000",
            "value": 893404,
            "range": "± 45794",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_runtime_qual/5000",
            "value": 2739758,
            "range": "± 14245",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_runtime_quant/5000",
            "value": 2751188,
            "range": "± 21036",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_direct_qual/5000",
            "value": 1104961,
            "range": "± 4072",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_direct_quant/5000",
            "value": 1102164,
            "range": "± 10545",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/dsrv_semisync/10000",
            "value": 11667790,
            "range": "± 35904",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/dsrv_dataflow/10000",
            "value": 1781796,
            "range": "± 5904",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_runtime_qual/10000",
            "value": 5840638,
            "range": "± 141140",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_runtime_quant/10000",
            "value": 5619579,
            "range": "± 33484",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_direct_qual/10000",
            "value": 2189489,
            "range": "± 4138",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property/mstlo_direct_quant/10000",
            "value": 2213157,
            "range": "± 20544",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/dsrv_semisync_untyped/1000",
            "value": 1232425,
            "range": "± 2168",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/dsrv_dataflow_untyped/1000",
            "value": 181871,
            "range": "± 149",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/dsrv_semisync_gradual_typed/1000",
            "value": 1163368,
            "range": "± 3708",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/dsrv_dataflow_gradual_typed/1000",
            "value": 182739,
            "range": "± 275",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/mstlo_runtime_qual_list_input/1000",
            "value": 509084,
            "range": "± 4162",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/mstlo_runtime_qual_map_input/1000",
            "value": 684162,
            "range": "± 5043",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/mstlo_direct_value_adapter_qual/1000",
            "value": 418594,
            "range": "± 2013",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/mstlo_runtime_output_streams/1",
            "value": 509524,
            "range": "± 1039",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/mstlo_direct_output_streams/1",
            "value": 222162,
            "range": "± 218",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/mstlo_runtime_output_streams/2",
            "value": 941019,
            "range": "± 2499",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/mstlo_direct_output_streams/2",
            "value": 438140,
            "range": "± 602",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/mstlo_runtime_output_streams/4",
            "value": 1803189,
            "range": "± 5379",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/mstlo_direct_output_streams/4",
            "value": 873855,
            "range": "± 947",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/mstlo_runtime_output_streams/8",
            "value": 3571655,
            "range": "± 14423",
            "unit": "ns/iter"
          },
          {
            "name": "threshold_property_diagnostics/mstlo_direct_output_streams/8",
            "value": 1731382,
            "range": "± 1218",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/dsrv_default_window_semisync/100",
            "value": 200356,
            "range": "± 884",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/dsrv_default_window_dataflow/100",
            "value": 48715,
            "range": "± 49",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_globally_window_qual/100",
            "value": 66235,
            "range": "± 168",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_globally_window_quant/100",
            "value": 66197,
            "range": "± 109",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_direct_globally_window_qual/100",
            "value": 36851,
            "range": "± 133",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_direct_globally_window_quant/100",
            "value": 37119,
            "range": "± 1489",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/dsrv_default_window_semisync/1000",
            "value": 1911625,
            "range": "± 6871",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/dsrv_default_window_dataflow/1000",
            "value": 400433,
            "range": "± 238",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_globally_window_qual/1000",
            "value": 672620,
            "range": "± 862",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_globally_window_quant/1000",
            "value": 662043,
            "range": "± 2111",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_direct_globally_window_qual/1000",
            "value": 366597,
            "range": "± 1245",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_direct_globally_window_quant/1000",
            "value": 369324,
            "range": "± 671",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/dsrv_default_window_semisync/5000",
            "value": 9642223,
            "range": "± 41187",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/dsrv_default_window_dataflow/5000",
            "value": 1966060,
            "range": "± 2370",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_globally_window_qual/5000",
            "value": 3525415,
            "range": "± 7872",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_globally_window_quant/5000",
            "value": 3499639,
            "range": "± 12146",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_direct_globally_window_qual/5000",
            "value": 1829290,
            "range": "± 6218",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_direct_globally_window_quant/5000",
            "value": 1844537,
            "range": "± 1886",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/dsrv_default_window_semisync/10000",
            "value": 19156161,
            "range": "± 103370",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/dsrv_default_window_dataflow/10000",
            "value": 3928505,
            "range": "± 458422",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_globally_window_qual/10000",
            "value": 7274472,
            "range": "± 407121",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_globally_window_quant/10000",
            "value": 7123256,
            "range": "± 23978",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_direct_globally_window_qual/10000",
            "value": 3658372,
            "range": "± 12958",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property/mstlo_direct_globally_window_quant/10000",
            "value": 3682960,
            "range": "± 5533",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property_diagnostics/dsrv_counted_window_semisync/1000",
            "value": 2576548,
            "range": "± 4494",
            "unit": "ns/iter"
          },
          {
            "name": "time_dependent_property_diagnostics/dsrv_counted_window_dataflow/1000",
            "value": 673244,
            "range": "± 669",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}