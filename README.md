## RUNNING TESTS

https://www.elastic.co/blog/getting-started-with-the-elastic-stack-and-docker-compose

```
curl -X PUT http://localhost:9200/_test/v1/_testing_and_processing_mode
```

Some of the tests use shared data state so they must be run single threaded.

```
RUST_BACKTRACE=1 cargo test -- --nocapture --test-threads=1
```

Run flamegraph

```
CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph
CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph --package powdrr-io-engine -- 9200
```


Run the benchmark

```
cargo run --package powdrr-io-engine --release 9200
cargo run --package powdrr-io-engine --release 9201
cargo run --package benchmark
```

Run test script

Monolith
```
python test_client.py monolith <num_batches> <num_processes>
python test_client.py monolith 1000 10
```

ES
```
python test_client.py es <num_batches> <num_processes>
python test_client.py es 1000 10
```


SAMPLE CREATE INDEX ERROR

{"error":{"root_cause":[{"type":"resource_already_exists_exception","reason":"index [test_index1/RL5Y0azVRI-RR8NE03FEfA] already exists","index_uuid":"RL5Y0azVRI-RR8NE03FEfA","index":"test_index1"}],"type":"resource_already_exists_exception","reason":"index [test_index1/RL5Y0azVRI-RR8NE03FEfA] already exists","index_uuid":"RL5Y0azVRI-RR8NE03FEfA","index":"test_index1"},"status":400}


SAMPLE RESULTS

{"took":49,"timed_out":false,"_shards":{"total":2,"successful":2,"skipped":0,"failed":0},"hits":{"total":{"value":5,"relation":"eq"},"max_score":0.18232156, hits":[{"_index":"test_index1","_id":"EkDfopYB6m2ajDpZ7pqA","_score":0.18232156,"_source":{ "@timestamp": "2025-05-05T16:55:06.727733", "index_col": 17464893063809093, "user": { "id": "vlb44hny" }, "message": "Login attempt failed 17464893063809093" }},{"_index":"test_index1","_id":"E0DfopYB6m2ajDpZ7pqA","_score":0.18232156,"_source":{ "@timestamp": "2025-05-05T16:55:06.727736", "index_col": 17464893063809094, "user": { "id": "vlb44hny" }, "message": "Login attempt failed 17464893063809094" }},{"_index":"test_index1","_id":"D0DfopYB6m2ajDpZ7pp_","_score":0.13353139,"_source":{ "@timestamp": "2025-05-05T16:55:06.727671", "index_col": 17464893063809090, "user": { "id": "vlb44hny" }, "message": "Login attempt failed 17464893063809090" }},{"_index":"test_index1","_id":"EEDfopYB6m2ajDpZ7pqA","_score":0.13353139,"_source":{ "@timestamp": "2025-05-05T16:55:06.727727", "index_col": 17464893063809091, "user": { "id": "vlb44hny" }, "message": "Login attempt failed 17464893063809091" }},{"_index":"test_index1","_id":"EUDfopYB6m2ajDpZ7pqA","_score":0.13353139,"_source":{ "@timestamp": "2025-05-05T16:55:06.727731", "index_col": 17464893063809092, "user": { "id": "vlb44hny" }, "message": "Login attempt failed 17464893063809092" }}]}}

{"took":10,"timed_out":false,"_shards":{"total":2,"successful":2,"skipped":0,"failed":0},"hits":{"total":{"value":"35","relation":"eq"},
{"took":49,"timed_out":false,"_shards":{"total":2,"successful":2,"skipped":0,"failed":0},"hits":{"total":{"value":5,   "relation":"eq"},"max_score":0.

"max_score":-0.938372585100368,

"hits":[{"_index":"test_index1","_id":"EkDfopYB6m2ajDpZ7pqA  ","_score":0.18232156,        "_source":{ "@timestamp": "2025-05-05T16:55:06.727733", "index_col": 17464893063809093, "user": { "id": "vlb44hny" }, "message": "Login attempt failed 17464893063809093" }},
"hits":[{"_index":"test_index1","_id":"n8qkA0QZTSS_rjkLN1Dwfw","_score":-0.938372585100368,"_source":{"@timestamp":"2025-05-07T20:59:15.211041","doc_id":17466767549809935,"index_col":17466767549809935,"message":"Login attempt failed 17466767549809935","user":{"id":"vlb44hny"}}},{"_index":"test_index1","_id":"9bPbh7vCSoC6A_UDRZbWag","_score":-0.938372585100368,"_source":{"@timestamp":"2025-05-07T20:59:15.418177","doc_id":17466767549809940,"index_col":17466767549809940,"message":"Login attempt failed 17466767549809940","user":{"id":"vlb44hny"}}},{"_index":"test_index1","_id":"FMxlJg6PRkW5vj5HP6MgEA","_score":-0.938372585100368,"_source":{"@timestamp":"2025-05-07T20:59:16.310873","doc_id":17466767549809961,"index_col":17466767549809961,"message":"Login attempt failed 17466767549809961","user":{"id":"vlb44hny"}}},{"_index":"test_index1","_id":"6WH8b8UXTNeposZKvrRZaA","_score":-0.938372585100368,"_source":{"@timestamp":"2025-05-07T20:59:15.001888","doc_id":17466767549809933,"index_col":17466767549809933,"message":"Login attempt failed 17466767549809933","user":{"id":"vlb44hny"}}},{"_index":"test_index1","_id":"qvzRNQYyRtGnQTfAPdd8iA","_score":-0.938372585100368,"_source":{"@timestamp":"2025-05-07T20:59:16.310859","doc_id":17466767549809960,"index_col":17466767549809960,"message":"Login attempt failed 17466767549809960","user":{"id":"vlb44hny"}}},{"_index":"test_index1","_id":"95aYRmvaSUq1rvNKkylH1w","_score":-0.938372585100368,"_source":{"@timestamp":"2025-05-07T20:59:15.001891","doc_id":17466767549809934,"index_col":17466767549809934,"message":"Login attempt failed 17466767549809934","user":{"id":"vlb44hny"}}},{"_index":"test_index1","_id":"8H9-o7XaREyOTKIahO61pA","_score":-0.938372585100368,"_source":{"@timestamp":"2025-05-07T20:59:15.211058","doc_id":17466767549809938,"index_col":17466767549809938,"message":"Login attempt failed 17466767549809938","user":{"id":"vlb44hny"}}},{"_index":"test_index1","_id":"Pl-cEItpQt6_GRpCkNw81Q","_score":-0.938372585100368,"_source":{"@timestamp":"2025-05-07T20:59:15.211060","doc_id":17466767549809939,"index_col":17466767549809939,"message":"Login attempt failed 17466767549809939","user":{"id":"vlb44hny"}}},{"_index":"test_index1","_id":"nQd_-iggS4WF8BNVs8g-Ig","_score":-0.938372585100368,"_source":{"@timestamp":"2025-05-07T20:59:15.824846","doc_id":17466767549809951,"index_col":17466767549809951,"message":"Login attempt failed 17466767549809951","user":{"id":"vlb44hny"}}},{"_index":"test_index1","_id":"w6t-7X7cRFK3RuRUIsbbPg","_score":-0.938372585100368,"_source":{"@timestamp":"2025-05-07T20:59:16.310876","doc_id":17466767549809962,"index_col":17466767549809962,"message":"Login attempt failed 17466767549809962","user":{"id":"vlb44hny"}}}]}}
0: Search #9 took 49ms
Sleeping for 5s



0: Search #0 took 59ms

SAMPLE INGEST RESULT

{"errors":false,"took":0,"items":[{"create":{"_index":"test_index1","_id":"_2dOpJYBvyTxCEkndif2","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":4,"_primary_term":1,"status":201}},{"create":{"_index":"test_index1","_id":"AGdOpJYBvyTxCEkndij2","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":6,"_primary_term":1,"status":201}},{"create":{"_index":"test_index1","_id":"AWdOpJYBvyTxCEkndij2","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":7,"_primary_term":1,"status":201}},{"create":{"_index":"test_index1","_id":"AmdOpJYBvyTxCEkndij3","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":8,"_primary_term":1,"status":201}},{"create":{"_index":"test_index1","_id":"A2dOpJYBvyTxCEkndij3","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":5,"_primary_term":1,"status":201}}]}

{"errors":false,"took":0,"items":[{"created":{"_index":"test_index1","_id":"YwlkcdZWSI6KdDhBUcQs0Q","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1,"status":201}},{"created":{"_index":"test_index1","_id":"DvnnUOBqR-6R0z2EsV1cig","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1,"status":201}},{"created":{"_index":"test_index1","_id":"fZLpd5GvRCyzx0BaFkYMvw","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1,"status":201}},{"created":{"_index":"test_index1","_id":"ML9BnxVOTFqQNex2cdzbew","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1,"status":201}},{"created":{"_index":"test_index1","_id":"v_vk3pCjRUSr6koF1fGWUA","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1,"status":201}}]}



TODO

http://es01:9200/.kibana_task_manager/_update_by_query?ignore_unavailable=true&refresh=true
{
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "type": "task"
          }
        },
        {
          "bool": {
            "must": [
              {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "task.enabled": true
                      }
                    }
                  ]
                }
              },
              {
                "bool": {
                  "should": [
                    {
                      "bool": {
                        "must": [
                          {
                            "term": {
                              "task.status": "idle"
                            }
                          },
                          {
                            "range": {
                              "task.runAt": {
                                "lte": "now"
                              }
                            }
                          }
                        ]
                      }
                    },
                    {
                      "bool": {
                        "must": [
                          {
                            "bool": {
                              "should": [
                                {
                                  "term": {
                                    "task.status": "running"
                                  }
                                },
                                {
                                  "term": {
                                    "task.status": "claiming"
                                  }
                                }
                              ]
                            }
                          },
                          {
                            "range": {
                              "task.retryAt": {
                                "lte": "now"
                              }
                            }
                          }
                        ]
                      }
                    }
                  ]
                }
              }
            ],
            "filter": [
              {
                "bool": {
                  "must_not": [
                    {
                      "bool": {
                        "should": [
                          {
                            "term": {
                              "task.status": "running"
                            }
                          },
                          {
                            "term": {
                              "task.status": "claiming"
                            }
                          }
                        ],
                        "must": {
                          "range": {
                            "task.retryAt": {
                              "gt": "now"
                            }
                          }
                        }
                      }
                    }
                  ]
                }
              }
            ]
          }
        }
      ]
    }
  },
  "script": {
    "source": "\n    if (params.claimableTaskTypes.contains(ctx._source.task.taskType)) {\n      if (ctx._source.task.schedule != null || ctx._source.task.attempts < params.taskMaxAttempts[ctx._source.task.taskType]) {\n        if(ctx._source.task.retryAt != null && ZonedDateTime.parse(ctx._source.task.retryAt).toInstant().toEpochMilli() < params.now) {\n    ctx._source.task.scheduledAt=ctx._source.task.retryAt;\n  } else {\n    ctx._source.task.scheduledAt=ctx._source.task.runAt;\n  }\n    ctx._source.task.status = \"claiming\"; ctx._source.task.ownerId=params.fieldUpdates.ownerId; ctx._source.task.retryAt=params.fieldUpdates.retryAt;\n      } else {\n        ctx._source.task.status = \"failed\";\n      }\n    } else if (params.unusedTaskTypes.contains(ctx._source.task.taskType)) {\n      ctx._source.task.status = \"unrecognized\";\n    } else {\n      ctx.op = \"noop\";\n    }",
    "lang": "painless",
    "params": {
      "now": 1747276015002,
      "fieldUpdates": {
        "ownerId": "kibana:38c063b1-087a-441c-8773-4f1c166f9afb",
        "retryAt": "2025-05-15T02:27:24.973Z"
      },
      "claimableTaskTypes": [
        "apm-source-map-migration-task"
      ],
      "skippedTaskTypes": [
        "session_cleanup",
        "actions_telemetry",
        "cleanup_failed_action_executions",
        "alerting_telemetry",
        "alerts_invalidate_api_keys",
        "alerting_health_check",
        "report:execute",
        "reports:monitor",
        "alerting:transform_health",
        "actions:.email",
        "actions:.index",
        "actions:.pagerduty",
        "actions:.swimlane",
        "actions:.server-log",
        "actions:.slack",
        "actions:.webhook",
        "actions:.cases-webhook",
        "actions:.xmatters",
        "actions:.servicenow",
        "actions:.servicenow-sir",
        "actions:.servicenow-itom",
        "actions:.jira",
        "actions:.resilient",
        "actions:.teams",
        "actions:.torq",
        "actions:.opsgenie",
        "actions:.tines",
        "alerting:.index-threshold",
        "alerting:.geo-containment",
        "alerting:.es-query",
        "dashboard_telemetry",
        "cases-telemetry-task",
        "Fleet-Usage-Sender",
        "Fleet-Usage-Logger",
        "fleet:reassign_action:retry",
        "fleet:unenroll_action:retry",
        "fleet:upgrade_action:retry",
        "fleet:update_agent_tags:retry",
        "fleet:request_diagnostics:retry",
        "fleet:check-deleted-files-task",
        "osquery:telemetry-packs",
        "osquery:telemetry-saved-queries",
        "osquery:telemetry-configs",
        "cloud_security_posture-stats_task",
        "ML:saved-objects-sync",
        "alerting:xpack.ml.anomaly_detection_alert",
        "alerting:xpack.ml.anomaly_detection_jobs_health",
        "UPTIME:SyntheticsService:Sync-Saved-Monitor-Objects",
        "alerting:xpack.uptime.alerts.monitorStatus",
        "alerting:xpack.uptime.alerts.tlsCertificate",
        "alerting:xpack.uptime.alerts.durationAnomaly",
        "alerting:xpack.uptime.alerts.tls",
        "alerting:xpack.synthetics.alerts.monitorStatus",
        "alerting:siem.eqlRule",
        "alerting:siem.savedQueryRule",
        "alerting:siem.indicatorRule",
        "alerting:siem.mlRule",
        "alerting:siem.queryRule",
        "alerting:siem.thresholdRule",
        "alerting:siem.newTermsRule",
        "alerting:siem.notifications",
        "endpoint:user-artifact-packager",
        "security:endpoint-diagnostics",
        "security:endpoint-meta-telemetry",
        "security:telemetry-lists",
        "security:telemetry-detection-rules",
        "security:telemetry-prebuilt-rule-alerts",
        "security:telemetry-timelines",
        "security:telemetry-configuration",
        "security:telemetry-filterlist-artifact",
        "endpoint:metadata-check-transforms-task",
        "alerting:metrics.alert.anomaly",
        "alerting:logs.alert.document.count",
        "alerting:metrics.alert.inventory.threshold",
        "alerting:metrics.alert.threshold",
        "alerting:monitoring_alert_cluster_health",
        "alerting:monitoring_alert_license_expiration",
        "alerting:monitoring_alert_cpu_usage",
        "alerting:monitoring_alert_missing_monitoring_data",
        "alerting:monitoring_alert_disk_usage",
        "alerting:monitoring_alert_thread_pool_search_rejections",
        "alerting:monitoring_alert_thread_pool_write_rejections",
        "alerting:monitoring_alert_jvm_memory_usage",
        "alerting:monitoring_alert_nodes_changed",
        "alerting:monitoring_alert_logstash_version_mismatch",
        "alerting:monitoring_alert_kibana_version_mismatch",
        "alerting:monitoring_alert_elasticsearch_version_mismatch",
        "alerting:monitoring_ccr_read_exceptions",
        "alerting:monitoring_shard_size",
        "apm-telemetry-task",
        "alerting:apm.transaction_duration",
        "alerting:apm.anomaly",
        "alerting:apm.error_rate",
        "alerting:apm.transaction_error_rate"
      ],
      "unusedTaskTypes": [
        "sampleTaskRemovedType",
        "alerting:siem.signals",
        "search_sessions_monitor",
        "search_sessions_cleanup",
        "search_sessions_expire"
      ],
      "taskMaxAttempts": {
        "apm-source-map-migration-task": 5
      }
    }
  },
  "sort": [
    {
      "_script": {
        "type": "number",
        "order": "asc",
        "script": {
          "lang": "painless",
          "source": "\nif (doc['task.retryAt'].size()!=0) {\n  return doc['task.retryAt'].value.toInstant().toEpochMilli();\n}\nif (doc['task.runAt'].size()!=0) {\n  return doc['task.runAt'].value.toInstant().toEpochMilli();\n}\n    "
        }
      }
    }
  ],
  "max_docs": 1,
  "conflicts": "proceed"
}


_bulk
{"create":{"_index":"filebeat-8.7.1"}}
{"@timestamp":"2025-05-15T02:26:54.170Z","log":{"offset":56413,"file":{"path":"/var/lib/docker/containers/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49-json.log"}},"agent":{"type":"filebeat","version":"8.7.1","ephemeral_id":"0084ab57-2cfc-481e-a0bc-74617362d0a8","id":"8036ad98-31a3-4224-a180-0c76a3fa4234","name":"4d67677bf91b"},"stream":"stderr","message":"client[4] 172.17.0.1: connected to 172.24.0.4:9200","input":{"type":"container"},"container":{"name":"httptoolkit-docker-tunnel-8000","image":{"name":"ghcr.io/httptoolkit/docker-socks-tunnel:v1.2.0"},"labels":{"tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_licenses":"MIT","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel"},"id":"b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49"},"docker":{"container":{"labels":{"org_opencontainers_image_title":"docker-socks-tunnel","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_licenses":"MIT","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel"}}},"ecs":{"version":"8.0.0"},"host":{"name":"4d67677bf91b"}}
{"create":{"_index":"filebeat-8.7.1"}}
{"@timestamp":"2025-05-15T02:26:54.352Z","message":"client[5] 172.17.0.1: connected to 172.24.0.4:9200","input":{"type":"container"},"ecs":{"version":"8.0.0"},"host":{"name":"4d67677bf91b"},"log":{"offset":56534,"file":{"path":"/var/lib/docker/containers/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49-json.log"}},"stream":"stderr","docker":{"container":{"labels":{"org_opencontainers_image_licenses":"MIT","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0"}}},"container":{"image":{"name":"ghcr.io/httptoolkit/docker-socks-tunnel:v1.2.0"},"id":"b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49","labels":{"org_opencontainers_image_licenses":"MIT","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","tech_httptoolkit_docker_tunnel":"8000"},"name":"httptoolkit-docker-tunnel-8000"},"agent":{"type":"filebeat","version":"8.7.1","ephemeral_id":"0084ab57-2cfc-481e-a0bc-74617362d0a8","id":"8036ad98-31a3-4224-a180-0c76a3fa4234","name":"4d67677bf91b"}}
{"create":{"_index":"filebeat-8.7.1"}}
{"@timestamp":"2025-05-15T02:26:54.360Z","input":{"type":"container"},"docker":{"container":{"labels":{"org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_licenses":"MIT","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel"}}},"host":{"name":"4d67677bf91b"},"agent":{"name":"4d67677bf91b","type":"filebeat","version":"8.7.1","ephemeral_id":"0084ab57-2cfc-481e-a0bc-74617362d0a8","id":"8036ad98-31a3-4224-a180-0c76a3fa4234"},"ecs":{"version":"8.0.0"},"log":{"offset":56655,"file":{"path":"/var/lib/docker/containers/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49-json.log"}},"stream":"stderr","message":"client[4] 172.17.0.1: connected to 172.24.0.4:9200","container":{"name":"httptoolkit-docker-tunnel-8000","image":{"name":"ghcr.io/httptoolkit/docker-socks-tunnel:v1.2.0"},"id":"b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49","labels":{"desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_licenses":"MIT","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy"}}}
{"create":{"_index":"filebeat-8.7.1"}}
{"@timestamp":"2025-05-15T02:26:54.369Z","log":{"offset":56776,"file":{"path":"/var/lib/docker/containers/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49-json.log"}},"message":"client[5] 172.17.0.1: connected to 172.24.0.4:9200","input":{"type":"container"},"docker":{"container":{"labels":{"org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_title":"docker-socks-tunnel","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_version":"v1.2.0","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_licenses":"MIT","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f"}}},"stream":"stderr","container":{"name":"httptoolkit-docker-tunnel-8000","image":{"name":"ghcr.io/httptoolkit/docker-socks-tunnel:v1.2.0"},"id":"b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49","labels":{"tech_httptoolkit_docker_tunnel":"8000","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_licenses":"MIT","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z"}},"ecs":{"version":"8.0.0"},"host":{"name":"4d67677bf91b"},"agent":{"ephemeral_id":"0084ab57-2cfc-481e-a0bc-74617362d0a8","id":"8036ad98-31a3-4224-a180-0c76a3fa4234","name":"4d67677bf91b","type":"filebeat","version":"8.7.1"}}
{"create":{"_index":"filebeat-8.7.1"}}
{"@timestamp":"2025-05-15T02:26:54.377Z","host":{"name":"4d67677bf91b"},"stream":"stderr","message":"client[4] 172.17.0.1: connected to 172.24.0.4:9200","input":{"type":"container"},"agent":{"ephemeral_id":"0084ab57-2cfc-481e-a0bc-74617362d0a8","id":"8036ad98-31a3-4224-a180-0c76a3fa4234","name":"4d67677bf91b","type":"filebeat","version":"8.7.1"},"ecs":{"version":"8.0.0"},"log":{"offset":56897,"file":{"path":"/var/lib/docker/containers/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49-json.log"}},"container":{"labels":{"org_opencontainers_image_version":"v1.2.0","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_title":"docker-socks-tunnel","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_licenses":"MIT","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel"},"name":"httptoolkit-docker-tunnel-8000","image":{"name":"ghcr.io/httptoolkit/docker-socks-tunnel:v1.2.0"},"id":"b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49"},"docker":{"container":{"labels":{"org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_version":"v1.2.0","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_licenses":"MIT"}}}}
{"create":{"_index":"filebeat-8.7.1"}}
{"@timestamp":"2025-05-15T02:26:54.491Z","log":{"offset":57018,"file":{"path":"/var/lib/docker/containers/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49-json.log"}},"message":"client[5] 172.17.0.1: connected to 172.24.0.4:9200","input":{"type":"container"},"host":{"name":"4d67677bf91b"},"ecs":{"version":"8.0.0"},"stream":"stderr","container":{"name":"httptoolkit-docker-tunnel-8000","image":{"name":"ghcr.io/httptoolkit/docker-socks-tunnel:v1.2.0"},"id":"b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49","labels":{"desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_licenses":"MIT","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_version":"v1.2.0"}},"docker":{"container":{"labels":{"desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_version":"v1.2.0","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_licenses":"MIT","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f"}}},"agent":{"name":"4d67677bf91b","type":"filebeat","version":"8.7.1","ephemeral_id":"0084ab57-2cfc-481e-a0bc-74617362d0a8","id":"8036ad98-31a3-4224-a180-0c76a3fa4234"}}
{"create":{"_index":"filebeat-8.7.1"}}
{"@timestamp":"2025-05-15T02:26:54.983Z","container":{"id":"b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49","labels":{"org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_licenses":"MIT","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_version":"v1.2.0","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy"},"name":"httptoolkit-docker-tunnel-8000","image":{"name":"ghcr.io/httptoolkit/docker-socks-tunnel:v1.2.0"}},"docker":{"container":{"labels":{"org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_licenses":"MIT","org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z"}}},"agent":{"type":"filebeat","version":"8.7.1","ephemeral_id":"0084ab57-2cfc-481e-a0bc-74617362d0a8","id":"8036ad98-31a3-4224-a180-0c76a3fa4234","name":"4d67677bf91b"},"log":{"offset":57139,"file":{"path":"/var/lib/docker/containers/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49-json.log"}},"message":"client[4] 172.17.0.1: connected to 172.24.0.4:9200","input":{"type":"container"},"host":{"name":"4d67677bf91b"},"ecs":{"version":"8.0.0"},"stream":"stderr"}
{"create":{"_index":"filebeat-8.7.1"}}
{"@timestamp":"2025-05-15T02:26:54.983Z","log":{"offset":57260,"file":{"path":"/var/lib/docker/containers/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49-json.log"}},"stream":"stderr","input":{"type":"container"},"docker":{"container":{"labels":{"org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_licenses":"MIT","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f"}}},"message":"client[5] 172.17.0.1: connected to 172.24.0.4:9200","container":{"name":"httptoolkit-docker-tunnel-8000","image":{"name":"ghcr.io/httptoolkit/docker-socks-tunnel:v1.2.0"},"id":"b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49","labels":{"org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_licenses":"MIT","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","tech_httptoolkit_docker_tunnel":"8000"}},"ecs":{"version":"8.0.0"},"host":{"name":"4d67677bf91b"},"agent":{"type":"filebeat","version":"8.7.1","ephemeral_id":"0084ab57-2cfc-481e-a0bc-74617362d0a8","id":"8036ad98-31a3-4224-a180-0c76a3fa4234","name":"4d67677bf91b"}}
{"create":{"_index":"filebeat-8.7.1"}}
{"@timestamp":"2025-05-15T02:26:55.018Z","docker":{"container":{"labels":{"org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_version":"v1.2.0","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_licenses":"MIT","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_title":"docker-socks-tunnel"}}},"host":{"name":"4d67677bf91b"},"log":{"offset":57381,"file":{"path":"/var/lib/docker/containers/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49-json.log"}},"stream":"stderr","input":{"type":"container"},"agent":{"ephemeral_id":"0084ab57-2cfc-481e-a0bc-74617362d0a8","id":"8036ad98-31a3-4224-a180-0c76a3fa4234","name":"4d67677bf91b","type":"filebeat","version":"8.7.1"},"message":"client[6] 172.17.0.1: connected to 172.24.0.4:9200","container":{"name":"httptoolkit-docker-tunnel-8000","image":{"name":"ghcr.io/httptoolkit/docker-socks-tunnel:v1.2.0"},"id":"b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49","labels":{"org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_licenses":"MIT","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_title":"docker-socks-tunnel"}},"ecs":{"version":"8.0.0"}}
{"create":{"_index":"filebeat-8.7.1"}}
{"@timestamp":"2025-05-15T02:26:55.050Z","stream":"stderr","container":{"name":"httptoolkit-docker-tunnel-8000","image":{"name":"ghcr.io/httptoolkit/docker-socks-tunnel:v1.2.0"},"id":"b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49","labels":{"org_opencontainers_image_title":"docker-socks-tunnel","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_licenses":"MIT","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy"}},"docker":{"container":{"labels":{"desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_licenses":"MIT"}}},"host":{"name":"4d67677bf91b"},"agent":{"name":"4d67677bf91b","type":"filebeat","version":"8.7.1","ephemeral_id":"0084ab57-2cfc-481e-a0bc-74617362d0a8","id":"8036ad98-31a3-4224-a180-0c76a3fa4234"},"ecs":{"version":"8.0.0"},"log":{"offset":57502,"file":{"path":"/var/lib/docker/containers/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49-json.log"}},"message":"client[4] 172.17.0.1: connected to 172.24.0.4:9200","input":{"type":"container"}}
{"create":{"_index":"filebeat-8.7.1"}}
{"@timestamp":"2025-05-15T02:26:55.116Z","stream":"stderr","message":"client[6] 172.17.0.1: connected to 172.24.0.4:9200","host":{"name":"4d67677bf91b"},"log":{"offset":57623,"file":{"path":"/var/lib/docker/containers/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49/b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49-json.log"}},"input":{"type":"container"},"container":{"name":"httptoolkit-docker-tunnel-8000","image":{"name":"ghcr.io/httptoolkit/docker-socks-tunnel:v1.2.0"},"id":"b41797069aaa6c76c278faaaebbd888c5c3784a91ba7c5d2f8ec4a382631ff49","labels":{"org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_licenses":"MIT","org_opencontainers_image_version":"v1.2.0","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_title":"docker-socks-tunnel","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z"}},"docker":{"container":{"labels":{"org_opencontainers_image_version":"v1.2.0","org_opencontainers_image_licenses":"MIT","org_opencontainers_image_title":"docker-socks-tunnel","desktop_docker_io/ports/1080/tcp":"127.0.0.1:0","tech_httptoolkit_docker_tunnel":"8000","org_opencontainers_image_description":"A tiny Dockerized SOCKS5 proxy","org_opencontainers_image_source":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_revision":"bb898bf3e33731038516546e02c422e4bda1218f","org_opencontainers_image_url":"https://github.com/httptoolkit/docker-socks-tunnel","org_opencontainers_image_created":"2023-05-22T15:10:45.125Z"}}},"ecs":{"version":"8.0.0"},"agent":{"version":"8.7.1","ephemeral_id":"0084ab57-2cfc-481e-a0bc-74617362d0a8","id":"8036ad98-31a3-4224-a180-0c76a3fa4234","name":"4d67677bf91b","type":"filebeat"}}



http://es01:9200/_nodes/_local/stats


http://es01:9200/_bulk?refresh=false&_source_includes=originId&require_alias=true
{"update":{"_id":"task:reports:monitor","_index":".kibana_task_manager_8.7.1","if_seq_no":5411,"if_primary_term":19}}
{"doc":{"task":{"runAt":"2025-05-15T02:26:58.128Z","state":"{}","schedule":{"interval":"3s"},"attempts":0,"status":"idle","startedAt":null,"retryAt":null,"ownerId":null,"params":"{}","taskType":"reports:monitor","traceparent":"00-e7d70636652eeb3c700b1f32893ea4d8-83855c04f24e0e61-00","scheduledAt":"2025-05-15T02:26:52.067Z"},"updated_at":"2025-05-15T02:26:55.271Z"}}
