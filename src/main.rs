//! ku CLI — query Kusto and save results as KDF files.

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut cluster = String::new();
    let mut database = String::new();
    let mut query = String::new();
    let mut output = String::new();
    let mut adaptive_output = String::new();
    let mut head: Option<usize> = None;
    let mut pretty = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--cluster" => {
                i += 1;
                cluster = args.get(i).cloned().unwrap_or_default();
            }
            "--database" => {
                i += 1;
                database = args.get(i).cloned().unwrap_or_default();
            }
            "--query" => {
                i += 1;
                query = args.get(i).cloned().unwrap_or_default();
            }
            "--output" => {
                i += 1;
                output = args.get(i).cloned().unwrap_or_default();
            }
            "--adaptive-output" => {
                i += 1;
                adaptive_output = args.get(i).cloned().unwrap_or_default();
            }
            "--head" => {
                i += 1;
                head = args.get(i).and_then(|s| s.parse().ok());
            }
            "-pp" | "--pretty-print" => {
                pretty = true;
            }
            "-h" | "--help" => {
                print_help();
                return;
            }
            _ => {
                if query.is_empty() {
                    query = args[i].clone();
                } else {
                    eprintln!("Unknown argument: {}", args[i]);
                    std::process::exit(1);
                }
            }
        }
        i += 1;
    }

    if cluster.is_empty() || database.is_empty() || query.is_empty() {
        eprintln!("Error: --cluster, --database, and --query are required.");
        eprintln!();
        print_help();
        std::process::exit(1);
    }

    if output.is_empty() && adaptive_output.is_empty() {
        eprintln!("Error: --output or --adaptive-output is required.");
        std::process::exit(1);
    }

    let result = match ku::run_query(&cluster, &database, &query) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    if !adaptive_output.is_empty() {
        // Adaptive mode: small results → inline, large results → file
        let head_n = head.unwrap_or(5);
        let (preview, _truncated) = result.format_preview(head_n, pretty);

        if result.row_count <= head_n {
            // Small result — print all inline, no file
            print!("{}", preview);
            eprintln!(
                "({} rows)",
                result.row_count,
            );
        } else {
            // Large result — print preview + save file
            print!("{}", preview);
            match result.write_kdf(std::path::Path::new(&adaptive_output)) {
                Ok(()) => {
                    eprintln!(
                        "-> {} ({} rows)",
                        adaptive_output,
                        result.row_count,
                    );
                }
                Err(e) => {
                    eprintln!("Error writing '{}': {}", adaptive_output, e);
                    std::process::exit(1);
                }
            }
        }
    } else {
        // Standard --output mode: always write file
        match result.write_kdf(std::path::Path::new(&output)) {
            Ok(()) => {
                if let Some(head_n) = head {
                    // --head: print preview + file
                    let (preview, _) = result.format_preview(head_n, pretty);
                    print!("{}", preview);
                }
                eprintln!(
                    "-> {} ({} rows)",
                    output,
                    result.row_count,
                );
            }
            Err(e) => {
                eprintln!("Error writing '{}': {}", output, e);
                std::process::exit(1);
            }
        }
    }
}

fn print_help() {
    eprintln!(
        r#"ku — Query Kusto → KDF file

USAGE:
    ku --cluster <url> --database <db> --query <kql> --output <file.kdf>
    ku --cluster <url> --database <db> --query <kql> --adaptive-output <file.kdf>

OPTIONS:
    --cluster <url>              Kusto cluster URL
    --database <db>              Database name
    --query <kql>                KQL query
    --output <file>              Always write KDF file
    --adaptive-output <file>     Write KDF only if result is large; print small results inline
    --head <N>                   Print first N rows (default: 5 with --adaptive-output)
    -pp, --pretty-print          Use padded table format instead of CSV
    -h, --help                   Print this help

EXAMPLES:
    ku --cluster https://help.kusto.windows.net --database Samples \
       --query "StormEvents | count" --adaptive-output result.kdf

    ku --cluster https://help.kusto.windows.net --database Samples \
       --query "StormEvents | summarize count() by State" --output states.kdf --head 3
"#
    );
}



