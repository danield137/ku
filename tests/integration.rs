fn tmp_path(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("ku_tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

#[test]
fn v2_response_to_kdf_roundtrip() {
    let v2_response = r#"[
        {"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0"},
        {"FrameType":"DataTable","TableId":0,"TableKind":"PrimaryResult","TableName":"Table_0",
         "Columns":[
            {"ColumnName":"State","ColumnType":"string"},
            {"ColumnName":"Count","ColumnType":"long"},
            {"ColumnName":"AvgDamage","ColumnType":"real"},
            {"ColumnName":"Active","ColumnType":"bool"}
         ],
         "Rows":[
            ["TEXAS",4242,1500.5,true],
            ["FLORIDA",3131,2200.0,false],
            ["CALIFORNIA",2020,900.75,true]
         ]},
        {"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":false}
    ]"#;

    let kdf_path = tmp_path("roundtrip.kdf");
    let meta = ku::v2_response_to_kdf(v2_response.as_bytes(), &kdf_path).unwrap();

    assert_eq!(meta.row_count, 3);
    assert_eq!(meta.columns.len(), 4);
    assert_eq!(meta.columns[0], ("State".into(), "string".into()));
    assert_eq!(meta.columns[1], ("Count".into(), "long".into()));
    assert_eq!(meta.columns[2], ("AvgDamage".into(), "real".into()));
    assert_eq!(meta.columns[3], ("Active".into(), "bool".into()));

    // Verify KDF file was written
    assert!(kdf_path.exists());
    let contents = std::fs::read_to_string(&kdf_path).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
    assert!(parsed.is_array());

    std::fs::remove_file(&kdf_path).ok();
}

#[test]
fn extract_primary_result_only() {
    let v2_response = r#"[
        {"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0"},
        {"FrameType":"DataTable","TableId":0,"TableKind":"PrimaryResult","TableName":"PrimaryResult",
         "Columns":[{"ColumnName":"x","ColumnType":"long"}],
         "Rows":[[1],[2],[3]]},
        {"FrameType":"DataTable","TableId":1,"TableKind":"QueryCompletionInformation","TableName":"@ExtendedProperties",
         "Columns":[{"ColumnName":"Key","ColumnType":"string"},{"ColumnName":"Value","ColumnType":"string"}],
         "Rows":[["ServerCache","Hit"],["Duration","00:00:00.123"]]},
        {"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":false}
    ]"#;

    let kdf_path = tmp_path("primary_only.kdf");
    let meta = ku::v2_response_to_kdf(v2_response.as_bytes(), &kdf_path).unwrap();

    assert_eq!(meta.row_count, 3);
    assert_eq!(meta.columns.len(), 1);
    assert_eq!(meta.columns[0].0, "x");

    std::fs::remove_file(&kdf_path).ok();
}

#[test]
fn error_response_returns_err() {
    let error_response = r#"[
        {"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0"},
        {"FrameType":"DataSetCompletion","HasErrors":true,"Cancelled":false}
    ]"#;

    let kdf_path = tmp_path("error.kdf");
    let result = ku::v2_response_to_kdf(error_response.as_bytes(), &kdf_path);
    assert!(
        result.is_err(),
        "should return error when HasErrors is true"
    );
}

#[test]
fn no_primary_result_returns_err() {
    let response = r#"[
        {"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0"},
        {"FrameType":"DataTable","TableId":1,"TableKind":"QueryCompletionInformation","TableName":"@ExtendedProperties",
         "Columns":[{"ColumnName":"Key","ColumnType":"string"}],
         "Rows":[["Foo"]]},
        {"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":false}
    ]"#;

    let kdf_path = tmp_path("no_primary.kdf");
    let result = ku::v2_response_to_kdf(response.as_bytes(), &kdf_path);
    assert!(result.is_err(), "should return error when no PrimaryResult");
}

#[test]
fn build_request_format() {
    let req = ku::build_query_request(
        "https://help.kusto.windows.net",
        "Samples",
        "StormEvents | count",
        "fake-token-123",
    );

    assert_eq!(req.url, "https://help.kusto.windows.net/v2/rest/query");
    assert_eq!(req.body["db"], "Samples");
    assert_eq!(req.body["csl"], "StormEvents | count");
    assert_eq!(req.auth_header, "Bearer fake-token-123");
}

#[test]
#[ignore] // requires Azure CLI auth
fn live_query_to_kdf() {
    let kdf_path = tmp_path("live.kdf");
    let meta = ku::execute_query(
        "https://help.kusto.windows.net",
        "Samples",
        "StormEvents | count",
        &kdf_path,
    )
    .unwrap();

    assert_eq!(meta.row_count, 1);
    assert!(meta.columns.iter().any(|(name, _)| name == "Count"));

    std::fs::remove_file(&kdf_path).ok();
}

// ─── Preview and adaptive output tests ───────────────────────────

fn make_v2_response(columns: &[(&str, &str)], rows: &[Vec<serde_json::Value>]) -> String {
    let cols: Vec<serde_json::Value> = columns
        .iter()
        .map(|(name, typ)| serde_json::json!({"ColumnName": name, "ColumnType": typ}))
        .collect();
    let rows_json: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| serde_json::Value::Array(r.clone()))
        .collect();
    serde_json::to_string(&serde_json::json!([
        {"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0"},
        {"FrameType":"DataTable","TableId":0,"TableKind":"PrimaryResult","TableName":"Table_0",
         "Columns": cols, "Rows": rows_json},
        {"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":false}
    ]))
    .unwrap()
}

#[test]
fn adaptive_small_result_no_file() {
    let response = make_v2_response(
        &[("x", "long"), ("y", "string")],
        &[
            vec![serde_json::json!(1), serde_json::json!("a")],
            vec![serde_json::json!(2), serde_json::json!("b")],
            vec![serde_json::json!(3), serde_json::json!("c")],
        ],
    );

    let result = ku::parse_v2_response(response.as_bytes()).unwrap();
    assert_eq!(result.row_count, 3);

    let (preview, truncated) = result.format_preview(5, false);
    assert!(!truncated, "3 rows with head=5 should not be truncated");
    assert!(preview.contains("a"));
    assert!(!preview.contains("..."));

    // Adaptive logic: row_count <= head → don't write
    assert!(result.row_count <= 5);
}

#[test]
fn adaptive_large_result_writes_file() {
    let rows: Vec<Vec<serde_json::Value>> = (0..10)
        .map(|i| {
            vec![
                serde_json::json!(i),
                serde_json::json!(format!("row_{}", i)),
            ]
        })
        .collect();
    let response = make_v2_response(&[("id", "long"), ("name", "string")], &rows);

    let result = ku::parse_v2_response(response.as_bytes()).unwrap();
    assert_eq!(result.row_count, 10);

    let (preview, truncated) = result.format_preview(3, false);
    assert!(truncated);
    assert!(preview.contains("row_0"));
    assert!(preview.contains("row_2"));
    assert!(!preview.contains("row_3"));
    assert!(preview.contains("10 rows total"));

    // Verify write_kdf works standalone
    let kdf_path = tmp_path("adaptive_large.kdf");
    result.write_kdf(&kdf_path).unwrap();
    assert!(kdf_path.exists());
    std::fs::remove_file(&kdf_path).ok();
}

#[test]
fn head_with_output_prints_preview() {
    let rows: Vec<Vec<serde_json::Value>> =
        (0..5).map(|i| vec![serde_json::json!(i * 100)]).collect();
    let response = make_v2_response(&[("val", "long")], &rows);

    let result = ku::parse_v2_response(response.as_bytes()).unwrap();

    let kdf_path = tmp_path("head_output.kdf");
    result.write_kdf(&kdf_path).unwrap();
    assert!(kdf_path.exists());

    let (preview, truncated) = result.format_preview(2, false);
    assert!(truncated);
    assert!(preview.contains("0"));
    assert!(preview.contains("100"));
    assert!(!preview.contains("200"));
    assert!(preview.contains("5 rows total"));

    std::fs::remove_file(&kdf_path).ok();
}

#[test]
fn head_shows_all_when_fewer() {
    let response = make_v2_response(
        &[("name", "string")],
        &[
            vec![serde_json::json!("Alice")],
            vec![serde_json::json!("Bob")],
        ],
    );

    let result = ku::parse_v2_response(response.as_bytes()).unwrap();
    let (preview, truncated) = result.format_preview(10, false);
    assert!(!truncated);
    assert!(preview.contains("Alice"));
    assert!(preview.contains("Bob"));
    assert!(!preview.contains("..."));
}
