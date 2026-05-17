use powdrr_lib::local_cli::{
    LocalQueryAnalysisRequest, LocalQueryExecutionPath, LocalQueryLanguage,
    LocalQueryPerformanceClassification, analyze_local_query,
};

#[test]
fn analyze_query_reports_highly_optimized_path() {
    let analysis = analyze_local_query(&LocalQueryAnalysisRequest {
        language: LocalQueryLanguage::ElasticsearchJson,
        body: r#"{
  "query": {
    "match": {
      "message": {
        "query": "failed"
      }
    }
  }
}"#
        .to_string(),
    });

    assert_eq!(
        analysis.classification,
        LocalQueryPerformanceClassification::HighlyOptimized
    );
    assert_eq!(
        analysis.execution_path,
        LocalQueryExecutionPath::TypedNodeMerge
    );
    assert!(analysis.reason.contains("typed node-merge path"));
}

#[test]
fn analyze_query_reports_supported_but_probably_slow_path() {
    let analysis = analyze_local_query(&LocalQueryAnalysisRequest {
        language: LocalQueryLanguage::ElasticsearchJson,
        body: r#"{
  "aggs": {
    "price_ranges": {
      "range": {
        "field": "price",
        "ranges": [
          { "from": "0", "to": "100" }
        ]
      }
    }
  }
}"#
        .to_string(),
    });

    assert_eq!(
        analysis.classification,
        LocalQueryPerformanceClassification::SupportedButProbablySlow
    );
    assert_eq!(
        analysis.execution_path,
        LocalQueryExecutionPath::LegacySqlFanout
    );
    assert!(analysis.reason.contains("Range aggregation `price_ranges`"));
}

#[test]
fn analyze_query_reports_unsupported_path() {
    let analysis = analyze_local_query(&LocalQueryAnalysisRequest {
        language: LocalQueryLanguage::ElasticsearchJson,
        body: r#"{
  "from": 1
}"#
        .to_string(),
    });

    assert_eq!(
        analysis.classification,
        LocalQueryPerformanceClassification::Unsupported
    );
    assert_eq!(
        analysis.execution_path,
        LocalQueryExecutionPath::Unsupported
    );
    assert_eq!(analysis.reason, "from != 0 is not implemented");
}
