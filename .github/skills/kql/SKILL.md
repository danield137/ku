---
name: kql
description: "KQL language expertise for writing correct, efficient Kusto queries using the `ku` CLI. Covers syntax gotchas, join patterns, dynamic types, datetime pitfalls, regex patterns, serialization, memory management, result-size discipline, and advanced functions (geo, vector, graph). USE THIS SKILL whenever writing, debugging, or reviewing KQL queries — even simple ones — because the gotchas section prevents the most common errors that waste tool calls and cause expensive retry cascades. Trigger on: KQL, Kusto, ADX, Azure Data Explorer, ku CLI, log analysis, data exploration, time series, anomaly detection, summarize, where clause, join, extend, project, let statement, parse operator, extract function, any mention of pipe-forward query syntax."
---

# KQL Mastery

Everything in this skill is derived from analysis of **1,205 real KQL queries** across 50 agent sessions. The 119 errors (9.9%) fell into clear, recurring categories. Each section below maps directly to an observed failure pattern — this isn't theoretical advice, it's battle-tested.

## 1. Running KQL with `ku`

`ku` is the CLI for querying Azure Data Explorer clusters. Authentication uses Azure CLI (`az login`).

### Basic usage
```bash
# Query and save results to KDF file
ku --cluster https://help.kusto.windows.net --database Samples \
   --query "StormEvents | count" --output result.kdf

# Adaptive output — small results print inline, large ones save to file
ku --cluster https://help.kusto.windows.net --database Samples \
   --query "StormEvents | summarize count() by State | top 5 by count_ desc" \
   --adaptive-output states.kdf

# Preview first 3 rows with pretty-print
ku --cluster https://help.kusto.windows.net --database Samples \
   --query "StormEvents | take 100" --output events.kdf --head 3 -pp
```

### Key flags
| Flag | Description |
|------|-------------|
| `--cluster <url>` | Kusto cluster URL |
| `--database <db>` | Database name |
| `--query <kql>` | KQL query string |
| `--output <file>` | Always write results to KDF file |
| `--adaptive-output <file>` | Write KDF only if result is large; print small results inline |
| `--head <N>` | Print first N rows (default: 5 with `--adaptive-output`) |
| `-pp`, `--pretty-print` | Use padded table format instead of CSV |

### Query vs management commands

KQL has two execution planes. Both use `ku --query`:

| Plane | Starts with | Examples |
|-------|-------------|----------|
| **Query** | Table name, `let`, `print`, `datatable` | `StormEvents \| where State == "TEXAS"` |
| **Management** | `.show`, `.create`, `.set`, `.drop`, `.alter` | `.show tables`, `.show table T schema` |

Management commands work with `ku --query` just like regular queries — `ku` handles routing automatically.

```bash
# Query plane
ku --cluster $CLUSTER --database $DB \
   --query "StormEvents | summarize count() by EventType | top 5 by count_ desc" \
   --adaptive-output events.kdf

# Management plane (same ku command)
ku --cluster $CLUSTER --database $DB \
   --query ".show tables" --adaptive-output tables.kdf
```

### Shell escaping tips

KQL queries contain pipes (`|`), quotes, and special characters. Wrap queries in double quotes and escape inner quotes:

```bash
# Simple — no inner quotes
ku --query "StormEvents | count"

# Inner string literals — use single quotes in KQL
ku --query "StormEvents | where State == 'TEXAS'"

# Complex — use a variable or heredoc for readability
QUERY='StormEvents | where State == "TEXAS" | summarize count() by EventType'
ku --cluster $CLUSTER --database $DB --query "$QUERY" --adaptive-output result.kdf
```

## 2. Dynamic Type Discipline

KQL's `dynamic` type is flexible but strict in certain contexts. The agent hit this **7 times** across 5 cases — always the same pattern: using a dynamic column in `summarize by`, `order by`, or `join on` without casting.

**The rule**: Any time you use a dynamic-typed column in `by`, `on`, or `order by`, wrap it in an explicit cast.

```kql
// ❌ ERROR: "Summarize group key 'Partners' is of a 'dynamic' type"
| summarize count() by Partners

// ✅ FIX
| summarize count() by tostring(Partners)
```

```kql
// ❌ ERROR: "order operator: key can't be of dynamic type"
| order by Area desc

// ✅ FIX
| order by tostring(Area) desc
```

```kql
// ❌ ERROR in join: dynamic join key
| join kind=inner other on $left.Area == $right.Area

// ✅ FIX — cast both sides
| extend Area_str = tostring(Area)
| join kind=inner (other | extend Area_str = tostring(Area)) on Area_str
```

**Self-correction**: When you see "is of a 'dynamic' type" in an error, add `tostring()`, `tolong()`, or `todouble()`.

## 3. Join Patterns & Pitfalls

Joins caused **14 errors** — the second-largest category. KQL joins have constraints that differ from SQL.

### Equality only (5 errors)
KQL join conditions support **only `==`**. No `<`, `>`, `!=`, or function calls in join predicates.

```kql
// ❌ ERROR: "Only equality is allowed in this context"
| join on geo_distance_2points(a.Lat, a.Lon, b.Lat, b.Lon) < 1000

// ✅ WORKAROUND — pre-bucket into spatial cells, then join on cell ID
| extend cell = geo_point_to_s2cell(Lon, Lat, 8)
| join kind=inner (other | extend cell = geo_point_to_s2cell(Lon, Lat, 8)) on cell
```

For range joins, pre-bin values: `| extend bin_val = bin(Value, 100)`, then join on `bin_val`.

### Left/right attribute matching (9 errors)
Both sides of a join `on` clause must reference **column entities only** — not expressions, not aggregates.

```kql
// ❌ ERROR: "for each left attribute, right attribute should be selected"
| join kind=inner other on $left.col1

// ✅ FIX — specify both sides explicitly
| join kind=inner other on $left.col1 == $right.col1
```

### Cardinality check before large joins
**Always** check cardinality before joining tables with >10K rows. A cross-join explosion was the source of the single `E_RUNAWAY_QUERY` error (25K × 195 = potential 4.8M rows).

```kql
// Before joining, check how many rows each side contributes
TableA | summarize dcount(JoinKey)  // → 25,000? Too many for an unconstrained join
TableB | summarize dcount(JoinKey)  // → 195? OK if filtered first
```

## 4. Regex in KQL

Two `extract_all` errors and 13 unnecessary Python fallbacks for regex work that KQL handles natively.

### The `extract_all` gotcha
Unlike Python's `re.findall()`, KQL's `extract_all` **requires capturing groups** in the regex:

```kql
// ❌ ERROR: "extractall(): argument 2 must be a valid regex with [1..16] matching groups"
| extend words = extract_all(@"[a-zA-Z]{3,}", Text)

// ✅ FIX — add parentheses around the pattern
| extend words = extract_all(@"([a-zA-Z]{3,})", Text)
```

### Regex toolkit — don't fall back to Python
| Function | Use case | Example |
|----------|----------|---------|
| `extract(regex, group, source)` | Single match | `extract(@"User '([^']+)'", 1, Msg)` |
| `extract_all(regex, source)` | All matches (needs `()`) | `extract_all(@"(\w+)", Text)` |
| `parse` | Structured extraction | `parse Msg with * "User '" Sender "' sent" *` |
| `matches regex` | Boolean filter | `where Url matches regex @"^https?://"` |
| `replace_regex` | Find and replace | `replace_regex(@"\s+", " ", Text)` |

## 5. Serialization Requirements

Window functions need serialized (ordered) input. The agent hit this **2 times**.

```kql
// ❌ ERROR: "Function 'row_cumsum' cannot be invoked. The row set must be serialized."
| summarize Online = sum(Direction) by bin(Timestamp, 5m)
| extend CumulativeOnline = row_cumsum(Online)

// ✅ FIX — add | serialize (or | order by, which implicitly serializes)
| summarize Online = sum(Direction) by bin(Timestamp, 5m)
| order by Timestamp asc
| extend CumulativeOnline = row_cumsum(Online)
```

Functions requiring serialization: `row_number()`, `row_cumsum()`, `prev()`, `next()`, `row_window_session()`.

## 6. Memory-Safe Query Patterns

**21 `E_LOW_MEMORY` errors** — the largest single category. All caused by scanning too much data without pre-filtering.

### The progression of safety
```
Safest ──────────────────────────────────────────────── Most dangerous
| count    | take 10    | where + summarize    | summarize (no filter)    | full scan
```

### Rules for large tables (>1M rows)

1. **Always start with `| count`** to understand table size
2. **Always `| where` before `| summarize`** — filter time range, partition key, or category first
3. **Never `dcount()` on high-cardinality columns** without pre-filtering
4. **Check join cardinality** before executing (see Section 3)
5. **Use `materialize()`** for subqueries referenced multiple times

```kql
// ❌ OUT OF MEMORY — 24M rows, no filter, dcount on every column
Consumption
| summarize dcount(Consumed), count() by Timestamp, HouseholdId, MeterType
| where dcount_Consumed > 1

// ✅ SAFE — filter first, then aggregate
Consumption
| where Timestamp between (datetime(2023-04-15) .. datetime(2023-04-16))
| summarize dcount(Consumed) by HouseholdId, MeterType
| where dcount_Consumed > 1
```

### When you see `E_LOW_MEMORY_CONDITION`
The query touched too much data. Your options:
- Add `| where` filters (time range, partition key)
- Reduce the number of `by` columns in `summarize`
- Break into smaller time windows and union results
- Use `| sample 10000` for exploratory work instead of full scans

### When you see `E_RUNAWAY_QUERY`
A join or aggregation produced too many output rows. Check join cardinality — one or both sides is too large.

## 7. Result Size Discipline

25 queries returned results so large they fragmented the agent's reasoning (results got written to temp files, requiring extra tool calls to read). Prevention:

| Query type | Safeguard |
|-----------|-----------|
| Exploratory | Always end with `\| take 10` or `\| take 20` |
| Aggregation | Use `\| top 20 by ...` not unbounded `summarize` |
| Wide rows (vectors, JSON) | `\| project` only needed columns |
| `make_list()` / `make_set()` | Avoid on high-cardinality groups (produces huge cells) |
| Unknown size | Run `\| count` first |

**The vector trap**: Tables with embedding columns (1536-dim float arrays) produce ~30KB per row. Even `| take 20` yields 600KB. Always `| project` away vector columns unless you specifically need them.

**With `ku`**: Use `--adaptive-output` to let `ku` decide whether to print inline or save to file. Use `--head N` to limit what's printed to the terminal.

## 8. String Comparison Strictness

4 errors from KQL requiring explicit casts when comparing computed string values — even when both sides are already strings.

```kql
// ❌ ERROR: "Cannot compare values of types string and string. Try adding explicit casts"
| where geo_point_to_s2cell(Lon, Lat, 16) == other_cell

// ✅ FIX — wrap both sides in tostring()
| where tostring(geo_point_to_s2cell(Lon, Lat, 16)) == tostring(other_cell)
```

This is most common with computed values from `geo_point_to_s2cell()`, `hash()`, and `strcat()` comparisons. When in doubt, cast with `tostring()`.

## 9. Advanced Functions — Don't Reinvent the Wheel

The agent fell back to Python **34 times** for operations KQL handles natively. Before writing Python:

### Vector similarity
```kql
// Don't export vectors and compute cosine similarity in Python
let target = toscalar(Vectors | where Word == "test" | project Vec);
Data | extend sim = series_cosine_similarity(parse_json(VecColumn), target)
| top 10 by sim desc
```

### Geo operations
```kql
// Point-in-polygon check
| where geo_point_in_polygon(Longitude, Latitude, dynamic({"type":"Polygon","coordinates":[...]}))

// Distance between two points (meters)
| extend dist = geo_distance_2points(Lon1, Lat1, Lon2, Lat2)

// Spatial bucketing for joins
| extend cell = geo_point_to_s2cell(Lon, Lat, 8)
```

### Graph queries
```kql
// Build and traverse a graph
graph(Nodes, Edges)
| graph-match (src)-[e*1..5]->(dst)
  where src.Name == "start" and dst.IsTarget == true
  project src.Name, dst.Name, path_length = array_length(e)
```

### Time series
```kql
// Create a time series and detect anomalies
| make-series count() default=0 on Timestamp step 1h
| extend anomalies = series_decompose_anomalies(count_)
```

For detailed examples and patterns, consult `references/advanced-patterns.md`.

## 10. Self-Correction Lookup Table

When you encounter an error, look it up here before retrying:

| Error message contains | Likely cause | Fix |
|---|---|---|
| `is of a 'dynamic' type` | Dynamic column in `by`/`on`/`order by` | Wrap in `tostring()`/`tolong()` |
| `Only equality is allowed` | Range predicate in join condition | Pre-bucket with S2/H3 cells or `bin()` |
| `extractall(): matching groups` | Missing `()` in regex | Add `()`: `@"(\w+)"` not `@"\w+"` |
| `row set must be serialized` | Window function on unsorted data | Add `\| serialize` or `\| order by` before it |
| `Cannot compare values of types string and string` | Computed string comparison | Add `tostring()` on both sides |
| `Failed to resolve column named 'X'` | Wrong column name or wrong table | Run `.show table T schema` to check column names |
| `E_LOW_MEMORY_CONDITION` | Query touched too much data | Add `\| where` filters, reduce time range, break into steps |
| `E_RUNAWAY_QUERY` | Join/aggregation produced too many rows | Check cardinality before joining; add pre-filters |
| `for each left attribute, right attribute` | Join `on` clause incomplete | Use explicit form: `on $left.X == $right.Y` |
| `needs to be bracketed` | Reserved word used as identifier | Use `['keyword']` syntax |
| `plugin doesn't exist` | Unavailable plugin on this cluster | Fall back to equivalent function or Python |
| `Expected string literal in datetime()` | Bare integer in datetime literal | Use `datetime(2024-01-01)` not `datetime(2024)` |
| `Unexpected token` after `by` | Complex expression in summarize by-clause | `extend` the expression first, then `summarize by` the column |
| `not recognized` / `unknown operator` | Operator not available on this engine | Check operator support; try equivalent (`order by` = `sort by`) |

## 11. Datetime Pitfalls

Datetime operations caused the highest retry rates in our experiments — agents get the literal syntax wrong, then cascade into completely different approaches instead of fixing the small error.

### Literal format
```kql
// ❌ WRONG — bare year is not a valid datetime
| where StartTime > datetime(2007)

// ✅ RIGHT — always use full date format
| where StartTime > datetime(2007-01-01)
```

### Filtering by year, month, or hour
```kql
// ❌ WRONG — comparing datetime column to integer
| where StartTime == 2007

// ✅ RIGHT — use datetime_part() to extract components
| where datetime_part("year", StartTime) == 2007

// ✅ ALSO RIGHT — use between with datetime range
| where StartTime between (datetime(2007-01-01) .. datetime(2007-12-31))
```

### Time bucketing in summarize
```kql
// ❌ WRONG — complex expression directly in by-clause can fail in some engines
| summarize count() by startofmonth(StartTime)

// ✅ SAFER — extend first, then summarize by the computed column
| extend Month = startofmonth(StartTime)
| summarize count() by Month
| order by Month asc
```

### Useful datetime functions
| Function | Purpose | Example |
|----------|---------|---------|
| `bin(ts, 1h)` | Round to nearest bucket | `bin(Timestamp, 1d)` |
| `startofmonth(ts)` | First day of month | `startofmonth(Timestamp)` |
| `datetime_part("hour", ts)` | Extract component | `datetime_part("year", Timestamp)` |
| `format_datetime(ts, fmt)` | Format as string | `format_datetime(Timestamp, "yyyy-MM")` |
| `ago(1d)` | Relative time | `where Timestamp > ago(7d)` |
| `between(a .. b)` | Range filter | `where Timestamp between (datetime(2024-01-01) .. datetime(2024-01-31))` |
| `todatetime(str)` | Parse string → datetime | `todatetime("2024-01-15T10:30:00Z")` |
| `totimespan(str)` | Parse string → timespan | `totimespan("01:30:00")` |

## 12. Operator Naming & Equality

KQL has subtle differences from SQL syntax that frequently trip up agents.

### Equality operators
```kql
// In where clauses, == is case-sensitive, =~ is case-insensitive
| where State == "TEXAS"      // exact match
| where State =~ "texas"      // case-insensitive
| where State != "TEXAS"      // not equal
| where State !~ "texas"      // case-insensitive not equal

// In joins, use == only
| join kind=inner other on $left.Key == $right.Key
```

### sort vs order
Both `sort by` and `order by` work identically in KQL — they are aliases. Use whichever you prefer, but be consistent.

### contains vs has
```kql
// contains: substring match (slower)
| where Message contains "error"        // finds "MyErrorHandler" too

// has: term/word match (faster, uses index)
| where Message has "error"             // matches word boundaries only

// For exact prefix/suffix
| where Message startswith "Error:"
| where Message endswith ".log"
```

## 13. The Strategy Cascade Trap

Our experiments show that when a first KQL query fails, agents often abandon their entire approach and try something completely different — leading to 3-5x token cost. The correct response is almost always to **fix the specific error**, not change strategy.

### The pattern to avoid
```
Query 1: extract(@"pattern", 1, col)  → Parse error
Query 2: todynamic(col)               → Different error  
Query 3: parse_json(col)              → Another error
Query 4: Python script                → Works but 10x tokens
```

### The correct pattern
```
Query 1: extract(@"pattern", 1, col)  → Parse error (bad escaping)
Query 2: extract(@"pattern", 1, col)  → Fix the specific escaping issue → Success
```

**Rules for error recovery:**
1. Read the error message carefully — it almost always tells you exactly what's wrong
2. Fix the **specific** syntax/escaping issue, don't switch approaches
3. Use the self-correction table (Section 10) to map errors to fixes
4. Only switch approaches after 2 failed fixes of the same query
5. The `parse` operator is often simpler than `extract()` for structured text:

```kql
// Instead of complex regex:
// extract(@"User '([^']+)' sent (\d+) bytes", 1, Message)

// Use parse for structured extraction:
| parse Message with * "User '" Username "' sent " ByteCount " bytes" *
```

## 14. Query Writing Checklist

Before running any KQL query via `ku`, mentally check:

1. **Pre-filtered?** Large tables have a `| where` before any `| summarize`
2. **Result bounded?** Exploratory queries end with `| take N` or `| top N`
3. **Dynamic columns cast?** Any dynamic column in `by`/`on`/`order by` is wrapped
4. **Regex has groups?** `extract_all` patterns have `()` around what you want to capture
5. **Join cardinality safe?** Both sides checked with `dcount()` before joining
6. **Needed columns only?** Wide tables get `| project` to drop unneeded columns
7. **Datetime literals valid?** Using `datetime(2024-01-01)` not `datetime(2024)` or bare integers
8. **Complex by-expressions?** Use `| extend` first, then `| summarize by` the computed column
9. **Error recovery plan?** If a query fails, fix the specific error — don't change strategy
10. **Output mode set?** Use `--adaptive-output` for exploration, `--output` for pipeline steps
