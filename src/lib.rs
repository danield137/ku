//! ku — slim Kusto CLI that streams query results to KDF files.
//!
//! Core library: parse Kusto V2 REST API responses and write KDF files.
//! No Kusto SDK dependency — just HTTP + JSON.

use serde_json::Value;
use std::io::Read;
use std::path::Path;

/// Parsed query result — the PrimaryResult frame with metadata.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub row_count: usize,
    pub columns: Vec<(String, String)>, // (name, kusto_type)
    pub rows: Vec<Vec<Value>>,          // raw row data
    frame: Value,                        // the full PrimaryResult DataTable frame
}

impl QueryResult {
    /// Write the result to a KDF file.
    pub fn write_kdf(&self, output: &Path) -> Result<(), String> {
        let kdf_content = Value::Array(vec![self.frame.clone()]);
        let file = std::fs::File::create(output)
            .map_err(|e| format!("Failed to create '{}': {}", output.display(), e))?;
        let writer = std::io::BufWriter::new(file);
        serde_json::to_writer(writer, &kdf_content)
            .map_err(|e| format!("Failed to write KDF: {}", e))?;
        Ok(())
    }

    /// Format a preview of the first `head` rows.
    /// `pretty` = true for padded table, false for CSV with typed header.
    /// Returns (preview_text, is_truncated).
    pub fn format_preview(&self, head: usize, pretty: bool) -> (String, bool) {
        let show_rows = head.min(self.row_count);
        let truncated = self.row_count > head;

        if self.columns.is_empty() || show_rows == 0 {
            return ("(empty result)".into(), false);
        }

        if pretty {
            self.format_pretty(show_rows, truncated)
        } else {
            self.format_csv(show_rows, truncated)
        }
    }

    fn format_csv(&self, show_rows: usize, truncated: bool) -> (String, bool) {
        let mut out = String::new();

        // Typed header: Name:type,Name:type,...
        for (i, (name, typ)) in self.columns.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(name);
            out.push(':');
            out.push_str(typ);
        }
        out.push('\n');

        // Data rows
        for row in self.rows.iter().take(show_rows) {
            for (i, val) in row.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let s = format_cell(val);
                // Quote if contains comma or newline
                if s.contains(',') || s.contains('\n') {
                    out.push('"');
                    out.push_str(&s.replace('"', "\"\""));
                    out.push('"');
                } else {
                    out.push_str(&s);
                }
            }
            out.push('\n');
        }

        if truncated {
            out.push_str(&format!("... ({} rows total)\n", self.row_count));
        }

        (out, truncated)
    }

    fn format_pretty(&self, show_rows: usize, truncated: bool) -> (String, bool) {
        let col_names: Vec<&str> = self.columns.iter().map(|(n, _)| n.as_str()).collect();
        let mut widths: Vec<usize> = col_names.iter().map(|n| n.len()).collect();
        for row in self.rows.iter().take(show_rows) {
            for (i, val) in row.iter().enumerate() {
                if i < widths.len() {
                    let s = format_cell(val);
                    widths[i] = widths[i].max(s.len());
                }
            }
        }

        let mut out = String::new();

        for (i, name) in col_names.iter().enumerate() {
            if i > 0 {
                out.push_str(" | ");
            }
            out.push_str(&format!("{:<w$}", name, w = widths[i]));
        }
        out.push('\n');

        for (i, w) in widths.iter().enumerate() {
            if i > 0 {
                out.push_str("-+-");
            }
            out.push_str(&"-".repeat(*w));
        }
        out.push('\n');

        for row in self.rows.iter().take(show_rows) {
            for (i, val) in row.iter().enumerate() {
                if i > 0 {
                    out.push_str(" | ");
                }
                let s = format_cell(val);
                if i < widths.len() {
                    out.push_str(&format!("{:<w$}", s, w = widths[i]));
                } else {
                    out.push_str(&s);
                }
            }
            out.push('\n');
        }

        if truncated {
            out.push_str(&format!("... ({} rows total)\n", self.row_count));
        }

        (out, truncated)
    }

    /// Return metadata summary (for backward compat).
    pub fn meta(&self) -> QueryMeta {
        QueryMeta {
            row_count: self.row_count,
            columns: self.columns.clone(),
        }
    }
}

/// Metadata-only result (backward compat).
#[derive(Debug, Clone)]
pub struct QueryMeta {
    pub row_count: usize,
    pub columns: Vec<(String, String)>,
}

fn format_cell(val: &Value) -> String {
    match val {
        Value::Null => "".into(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => val.to_string(),
    }
}

/// HTTP request description (for testability — no actual HTTP here).
#[derive(Debug)]
pub struct QueryRequest {
    pub url: String,
    pub body: serde_json::Map<String, Value>,
    pub auth_header: String,
}

/// Build the HTTP request for a Kusto V2 query.
pub fn build_query_request(
    cluster: &str,
    database: &str,
    query: &str,
    bearer_token: &str,
) -> QueryRequest {
    let url = format!("{}/v2/rest/query", cluster.trim_end_matches('/'));

    let mut body = serde_json::Map::new();
    body.insert("db".into(), Value::String(database.into()));
    body.insert("csl".into(), Value::String(query.into()));

    QueryRequest {
        url,
        body,
        auth_header: format!("Bearer {}", bearer_token),
    }
}

/// Parse a Kusto V2 JSON response and extract the PrimaryResult.
pub fn parse_v2_response(reader: impl Read) -> Result<QueryResult, String> {
    let frames: Vec<Value> =
        serde_json::from_reader(reader).map_err(|e| format!("Failed to parse V2 response: {}", e))?;

    // Check for errors in DataSetCompletion
    for frame in &frames {
        if frame.get("FrameType").and_then(|f| f.as_str()) == Some("DataSetCompletion") {
            if frame.get("HasErrors").and_then(|h| h.as_bool()) == Some(true) {
                return Err("Query returned errors (HasErrors: true)".into());
            }
        }
    }

    // Find the PrimaryResult DataTable
    let primary = frames
        .iter()
        .find(|f| {
            f.get("FrameType").and_then(|ft| ft.as_str()) == Some("DataTable")
                && f.get("TableKind").and_then(|tk| tk.as_str()) == Some("PrimaryResult")
        })
        .ok_or("No PrimaryResult DataTable in V2 response")?;

    // Extract columns
    let columns_arr = primary
        .get("Columns")
        .and_then(|c| c.as_array())
        .ok_or("PrimaryResult missing Columns")?;

    let columns: Vec<(String, String)> = columns_arr
        .iter()
        .map(|c| {
            let name = c.get("ColumnName").and_then(|n| n.as_str()).unwrap_or("?").to_string();
            let col_type = c.get("ColumnType").or_else(|| c.get("DataType"))
                .and_then(|t| t.as_str()).unwrap_or("string").to_string();
            (name, col_type)
        })
        .collect();

    // Extract rows
    let rows: Vec<Vec<Value>> = primary
        .get("Rows")
        .and_then(|r| r.as_array())
        .map(|arr| arr.iter().filter_map(|r| r.as_array().cloned()).collect())
        .unwrap_or_default();

    let row_count = rows.len();

    Ok(QueryResult {
        row_count,
        columns,
        rows,
        frame: primary.clone(),
    })
}

/// Parse a Kusto V2 JSON response and write the PrimaryResult to a KDF file.
/// (Backward-compatible wrapper.)
pub fn v2_response_to_kdf(reader: impl Read, output: &Path) -> Result<QueryMeta, String> {
    let result = parse_v2_response(reader)?;
    result.write_kdf(output)?;
    Ok(result.meta())
}

/// Get an Azure CLI access token for a Kusto cluster.
pub fn get_az_cli_token(cluster: &str) -> Result<String, String> {
    let output = if cfg!(windows) {
        std::process::Command::new("cmd")
            .args(["/c", "az", "account", "get-access-token", "--resource", cluster, "--query", "accessToken", "-o", "tsv"])
            .output()
    } else {
        std::process::Command::new("az")
            .args(["account", "get-access-token", "--resource", cluster, "--query", "accessToken", "-o", "tsv"])
            .output()
    }
    .map_err(|e| format!("Failed to run 'az': {} (is Azure CLI installed?)", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("az account get-access-token failed: {}", stderr.trim()));
    }

    let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if token.is_empty() {
        return Err("az returned empty token".into());
    }
    Ok(token)
}

/// Execute a KQL query against a Kusto cluster. Returns the parsed result.
pub fn run_query(cluster: &str, database: &str, query: &str) -> Result<QueryResult, String> {
    let token = get_az_cli_token(cluster)?;
    let req = build_query_request(cluster, database, query, &token);

    let client = reqwest::blocking::Client::new();
    let response = client
        .post(&req.url)
        .header("Authorization", &req.auth_header)
        .header("Content-Type", "application/json; charset=utf-8")
        .header("Accept", "application/json")
        .json(&req.body)
        .send()
        .map_err(|e| format!("HTTP request failed: {}", e))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().unwrap_or_default();
        return Err(format!("Kusto returned HTTP {}: {}", status, &body[..body.len().min(500)]));
    }

    let bytes = response.bytes().map_err(|e| format!("Failed to read response: {}", e))?;
    parse_v2_response(bytes.as_ref())
}

/// Execute a KQL query and save results as a KDF file.
/// (Backward-compatible wrapper.)
pub fn execute_query(cluster: &str, database: &str, query: &str, output: &Path) -> Result<QueryMeta, String> {
    let result = run_query(cluster, database, query)?;
    result.write_kdf(output)?;
    Ok(result.meta())
}
