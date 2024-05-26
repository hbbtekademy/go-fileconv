# parquet-converter

Convert CSV and JSON files to Apache Parquet format. Powered by DuckDB!

## Usage
### CLI
```
./pqconv-cli -h
Harness the power of DuckDB to easily convert CSV and JSON files to Apache Parquet format.
https://duckdb.org/

Usage:
  pqconv-cli [command]

Available Commands:
  csv2parquet  convert csv files to apache parquet files (https://duckdb.org/docs/data/csv/overview#parameters)
  json2parquet convert json files to apache parquet files (https://duckdb.org/docs/data/json/overview#parameters)
  help         Help about any command
  completion   Generate the autocompletion script for the specified shell

Flags:
      --pq-compression string        (Optional) The compression type for the output parquet file. (default "snappy")
      --pq-partition-by strings      (Optional) Write to a Hive partitioned data set of Parquet files.
      --pq-overwrite-or-ignore       (Optional) Use this flag to allow overwriting an existing directory.
      --pq-filename-pattern string   (Optional) With this flag a pattern with {i} or {uuid} can be defined to create specific partition filenames. (default "data_{i}.parquet")
      --pq-per-thread-output         (Optional) If the final number of Parquet files is not important, writing one file per thread can significantly improve performance.
      --config-dir string            (Optional) Config Directory for the CLI (default "$HOME/.pqconv-cli")
      --duckdb-config strings        (Optional) List of DuckDB configuration parameters. e.g.
                                     --duckdb-config "SET threads TO 1"
                                     --duckdb-config "SET memory_limit = '10GB'"
                                     Refer https://duckdb.org/docs/configuration/overview.html for list of all the configurations
  -h, --help                         help for pqconv-cli
  -v, --version                      version for pqconv-cli
```
#### json2parquet
```
./pqconv-cli json2parquet -h
convert json files to apache parquet files (https://duckdb.org/docs/data/json/overview#parameters)

Usage:
  pqconv-cli json2parquet [flags]

Flags:
      --source string            full path of json file or regex for multiple json files.
      --dest string              filename of output parquet file or directory in which to write hive partitioned parquet files.
                                 
      --disable-autodetect       (Optional) Disable automatically detecting the names of the keys and data types of the values.
      --compression string       (Optional) The compression type for the file (auto, gzip, zstd). (default "auto")
      --columns strings          (Optional) A list of key names and value types contained within the JSON file. (e.g., "key1:INTEGER,key2:VARCHAR"). If auto detect is enabled these will be inferred.
      --format string            (Optional) Can be one of ('auto', 'unstructured', 'newline_delimited', 'array'). (default "array")
      --dateformat string        (Optional) Specifies the date format to use when parsing dates. https://duckdb.org/docs/sql/functions/dateformat (default "iso")
      --timestampformat string   (Optional) Specifies the date format to use when parsing timestamps. https://duckdb.org/docs/sql/functions/dateformat (default "iso")
      --max-depth int            (Optional) Maximum nesting depth to which the automatic schema detection detects types. (default -1)
      --max-obj-size uint        (Optional) The maximum size of a JSON object (in bytes). (default 16777216)
      --records string           (Optional) Can be one of ('auto', 'true', 'false'). (default "auto")
      --sample-size uint         (Optional) Flag to define number of sample objects for automatic JSON type detection. Set to -1 to scan the entire input file. (default 20480)
      --convert-str-to-int       (Optional) Whether strings representing integer values should be converted to a numerical type.
      --filename                 (Optional) Whether or not an extra filename column should be included in the result.
      --hive-partitioning        (Optional) Whether or not to interpret the path as a Hive partitioned path.
      --ignore-errors            (Optional) Whether to ignore parse errors (only possible when format is 'newline_delimited').
      --union-by-name            (Optional) Whether the schema's of multiple JSON files should be unified.
  -h, --help                     help for json2parquet
```

#### csv2parquet
```
./pqconv-cli csv2parquet -h
convert csv files to apache parquet files (https://duckdb.org/docs/data/csv/overview#parameters)

Usage:
  pqconv-cli csv2parquet [flags]

Flags:
      --source string                  full path of csv file or regex for multiple csv files.
      --dest string                    filename of output parquet file or directory in which to write hive partitioned parquet files.
                                       
      --delim string                   (Optional) Specifies the character that separates columns within each row (line) of the file. (default ",")
      --quote string                   (Optional) Specifies the quoting string to be used when a data value is quoted. (default "\"")
      --new-line string                (Optional) Set the new line character(s) in the file. Options are '\r','\n', or '\r\n'.
      --decimal-sep string             (Optional) The decimal separator of numbers. (default ".")
      --escape string                  (Optional) Specifies the string that should appear before a data character sequence that matches the quote value. (default "\"")
      --dateformat string              (Optional) Specifies the date format to use when parsing dates. https://duckdb.org/docs/sql/functions/dateformat
      --timestampformat string         (Optional) Specifies the date format to use when parsing timestamps. https://duckdb.org/docs/sql/functions/dateformat
      --compression string             (Optional) The compression type for the file (auto, gzip, zstd). (default "auto")
      --max-line-size int              (Optional) The maximum line size in bytes. (default 2097152)
      --sample-size int                (Optional) The number of sample rows for auto detection of parameters. (default 20480)
      --skip int                       (Optional) The number of lines at the top of the file to skip.
      --force-not-null strings         (Optional) Do not match the specified columns’ values against the NULL string. 
                                       In the default case where the NULL string is empty, this means that empty values will be read as zero-length strings rather than NULLs.
      --auto-type-candidates strings   (Optional) This option allows you to specify the types that the sniffer will use when detecting CSV column types. 
                                       The VARCHAR type is always included in the detected types (as a fallback option). 
                                       Valid values (SQLNULL, BOOLEAN, BIGINT, DOUBLE, TIME, DATE, TIMESTAMP, VARCHAR).
      --columns strings                (Optional) A list that specifies the column names and column types contained within the CSV file (e.g., col1:INTEGER,col2:VARCHAR).
                                       The order of the Name:Type definitions should match the order of columns in the CSV file.
                                       Using this option implies that auto detection is not used.
      --names strings                  (Optional) If the file does not contain a header, names will be auto-generated by default. You can provide your own names with the names option.
      --nullstr strings                (Optional) Specifies a list of strings that represent a NULL value.
      --types strings                  (Optional) The types flag can be used to override types of only certain columns by providing a list of name:type mappings (e.g., col1:INTEGER,col2:VARCHAR)
      --disable-autodetect             (Optional) Disable auto detection of CSV parameters.
      --all-varchar                    (Optional) Option to skip type detection for CSV parsing and assume all columns to be of type VARCHAR.
      --disable-quoted-nulls           (Optional) Disable the conversion of quoted values to NULL values.
      --normalize-names                (Optional) Boolean value that specifies whether or not column names should be normalized, removing any non-alphanumeric characters from them.
      --filename                       (Optional) Whether or not an extra filename column should be included in the result.
      --header                         (Optional) Specifies that the file contains a header line with the names of each column in the file.
      --hive-partitioning              (Optional) Whether or not to interpret the path as a Hive partitioned path.
      --ignore-errors                  (Optional) Whether to ignore parse errors (only possible when format is 'newline_delimited').
      --null-padding                   (Optional) If this option is enabled, when a row lacks columns, it will pad the remaining columns on the right with null values.
      --parallel                       (Optional) Whether or not the parallel CSV reader is used.
      --union-by-name                  (Optional) Whether the schema's of multiple CSV files should be unified.
  -h, --help                           help for csv2parquet
```

### Go Module
```
go get github.com/hbbtekademy/parquet-converter
```
`go-duckdb` uses `CGO` to make calls to DuckDB. You must build your binaries with `CGO_ENABLED=1`.

#### Json2Parquet
```go
client, err := pqconv.New(context.Background(), "file.db", "SET threads TO 1", "SET memory_limit = '1GB'")
if err != nil {
  return fmt.Errorf("error: %w. failed getting duckdb client", err)
}

err = client.Json2Parquet(context.Background(), "path/to/source.json", "path/to/dest.parquet",
  pqparam.NewWriteParams(
    pqparam.WithCompression(pqparam.Zstd),
    pqparam.WithPerThreadOutput(false),
    pqparam.WithHivePartitionConfig(
      pqparam.WithFilenamePattern("file_{uuid}"),
      pqparam.WithOverwriteOrIgnore(true),
      pqparam.WithPartitionBy("col1", "col2"),
    ),
  ),
  jsonparam.WithConvStr2Int(false),
  jsonparam.WithFormat(jsonparam.NewlineDelimited),
  jsonparam.WithMaxDepth(jsonFlags.maxDepth),
)
if err != nil {
  return fmt.Errorf("error: %w. failed converting json to parquet", err)
}
```

#### Csv2Parquet
```go
client, err := pqconv.New(context.Background(), "file.db")
if err != nil {
  return fmt.Errorf("error: %w. failed getting duckdb client", err)
}

err = client.Csv2Parquet(context.Background(), "path/to/source.csv", "path/to/dest.parquet",
  pqparam.NewWriteParams(
    pqparam.WithCompression(pqparam.Zstd),
    pqparam.WithPerThreadOutput(false),
    pqparam.WithHivePartitionConfig(
      pqparam.WithFilenamePattern("file_{uuid}"),
      pqparam.WithOverwriteOrIgnore(true),
      pqparam.WithPartitionBy("col1", "col2"),
    ),
  ),
  csvparam.WithAllVarchar(true),
  csvparam.WithAllowQuotedNulls(false),
  csvparam.WithAutoTypeCandidates([]string{"BIGINT", "DOUBLE"}),
  csvparam.WithDelim("|"),
  csvparam.WithHeader(true),
)
if err != nil {
  return fmt.Errorf("error: %w. failed converting csv to parquet", err)
}
```

## This utility depends on the following notable projects

- DuckDB: https://github.com/duckdb/duckdb
- go-duckdb Client: https://github.com/marcboeker/go-duckdb
- Cobra: https://github.com/spf13/cobra
