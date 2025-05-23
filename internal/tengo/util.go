package tengo

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// EscapeIdentifier is for use in safely escaping MySQL identifiers (table
// names, column names, etc). It doubles any backticks already present in the
// input string, and then returns the string wrapped in outer backticks.
func EscapeIdentifier(input string) string {
	return "`" + strings.ReplaceAll(input, "`", "``") + "`"
}

var replacerCreateTableString = strings.NewReplacer(`\`, `\\`, "\000", `\0`, "'", "''", "\n", `\n`, "\r", `\r`)

// EscapeValueForCreateTable returns the supplied value (typically obtained from
// querying an information_schema table) escaped in the same manner as SHOW
// CREATE TABLE would display it. Examples include default values, table
// comments, column comments, index comments. This function does not wrap the
// value in single quotes; the caller should do that as appropriate.
func EscapeValueForCreateTable(input string) string {
	return replacerCreateTableString.Replace(input)
}

// SplitHostOptionalPort takes an address string containing a hostname, ipv4
// addr, or ipv6 addr; *optionally* followed by a colon and port number. It
// splits the hostname portion from the port portion and returns them
// separately. If no port was present, 0 will be returned for that portion.
// If hostaddr contains an ipv6 address, the IP address portion must be
// wrapped in brackets on input, and the brackets will still be present on
// output.
func SplitHostOptionalPort(hostaddr string) (string, int, error) {
	if len(hostaddr) == 0 {
		return "", 0, errors.New("Cannot parse blank host address")
	}

	// ipv6 without port, or ipv4 or hostname without port
	if (hostaddr[0] == '[' && hostaddr[len(hostaddr)-1] == ']') || len(strings.Split(hostaddr, ":")) == 1 {
		return hostaddr, 0, nil
	}

	host, portString, err := net.SplitHostPort(hostaddr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		return "", 0, err
	} else if port < 1 {
		return "", 0, fmt.Errorf("invalid port %d supplied", port)
	}

	// ipv6 with port: add the brackets back in -- net.SplitHostPort removes them,
	// but we still need them to form a valid DSN later
	if hostaddr[0] == '[' && host[0] != '[' {
		host = fmt.Sprintf("[%s]", host)
	}

	return host, port, nil
}

var reParseTablespace = regexp.MustCompile(`[)] /\*!50100 TABLESPACE ` + "`((?:[^`]|``)+)`" + ` \*/ ENGINE=`)

// ParseCreateTablespace parses a TABLESPACE clause out of a CREATE TABLE
// statement.
func ParseCreateTablespace(createStmt string) string {
	matches := reParseTablespace.FindStringSubmatch(createStmt)
	if matches != nil {
		return matches[1]
	}
	return ""
}

var reParseCreateAutoInc = regexp.MustCompile(`[)/] ENGINE=\w+ (AUTO_INCREMENT=(\d+) )DEFAULT CHARSET=`)

// ParseCreateAutoInc parses a CREATE TABLE statement, formatted in the same
// manner as SHOW CREATE TABLE, and removes the table-level next-auto-increment
// clause if present. The modified CREATE TABLE will be returned, along with
// the next auto-increment value if one was found.
func ParseCreateAutoInc(createStmt string) (string, uint64) {
	matches := reParseCreateAutoInc.FindStringSubmatch(createStmt)
	if matches == nil {
		return createStmt, 0
	}
	nextAutoInc, _ := strconv.ParseUint(matches[2], 10, 64)
	newStmt := strings.Replace(createStmt, matches[1], "", 1)
	return newStmt, nextAutoInc
}

var reParseCreatePartitioning = regexp.MustCompile(`(?is)(\s*(?:/\*!?\d*)?\s*partition\s+by .*)$`)

// ParseCreatePartitioning parses a CREATE TABLE statement, formatted in the
// same manner as SHOW CREATE TABLE, and splits out the base CREATE clauses from
// the partioning clause.
func ParseCreatePartitioning(createStmt string) (base, partitionClause string) {
	matches := reParseCreatePartitioning.FindStringSubmatch(createStmt)
	if matches == nil {
		return createStmt, ""
	}
	return createStmt[0 : len(createStmt)-len(matches[1])], matches[1]
}

// reformatCreateOptions converts a value obtained from
// information_schema.tables.create_options to the formatting used in SHOW
// CREATE TABLE.
func reformatCreateOptions(input string) string {
	return strings.Join(splitAttributes(input), " ")
}

// splitAttributes converts a single string of table attributes (from
// information_schema.tables.create_options) or index attributes (from SHOW
// CREATE TABLE) into a slice of individual normalized attributes.
// TODO: currently this strips anything wrapped in /*!...*/ version-gate
// comments
func splitAttributes(input string) []string {
	attributes := []string{}
	if input == "" {
		return attributes
	}
	tokens := TokenizeString(input)
	for n := 0; n < len(tokens); n++ {
		var field string
		if tokens[n][0] == '`' {
			field = tokens[n] // user-supplied casing kept as-is for backquote-wrapped attribute names in MariaDB
		} else {
			field = strings.ToUpper(tokens[n])
		}
		if n < len(tokens)-2 && tokens[n+1] == "=" { // field=value
			value := tokens[n+2]
			if value[0] == '"' && value[len(value)-1] == '"' {
				// value was double-quote-wrapped, convert to single-quote-wrapped
				attributes = append(attributes, field+"='"+value[1:len(value)-1]+"'")
			} else {
				// value as-is
				attributes = append(attributes, field+"="+value)
			}
			n += 2 // skip past "=" and value tokens
		} else if field != "PARTITIONED" { // field without value; strip PARTITIONED as I_S special-case not in SHOW CREATE
			attributes = append(attributes, field)
		}
	}
	return attributes
}

var stripNonInnoRegexps = []struct {
	re          *regexp.Regexp
	replacement string
}{
	{re: regexp.MustCompile(" /\\*!50606 (STORAGE|COLUMN_FORMAT) (DISK|MEMORY|FIXED|DYNAMIC) \\*/"), replacement: ""},
	{re: regexp.MustCompile(" USING (HASH|BTREE)"), replacement: ""},
	{re: regexp.MustCompile("`\\) KEY_BLOCK_SIZE=\\d+"), replacement: "`)"},
}

// StripNonInnoAttributes adjusts the supplied CREATE TABLE statement to remove
// any no-op table options that are persisted in SHOW CREATE TABLE, but not
// reflected in information_schema and serve no purpose for InnoDB tables.
// This function is not guaranteed to be safe for non-InnoDB tables. The input
// string should be formatted like SHOW CREATE TABLE.
func StripNonInnoAttributes(createStmt string) string {
	for _, entry := range stripNonInnoRegexps {
		createStmt = entry.re.ReplaceAllString(createStmt, entry.replacement)
	}
	return createStmt
}

// baseDSN returns a DSN with the database (schema) name and params stripped.
// Currently only supports MySQL, via go-sql-driver/mysql's DSN format.
func baseDSN(dsn string) string {
	tokens := strings.SplitAfter(dsn, "/")
	return strings.Join(tokens[0:len(tokens)-1], "")
}

// paramMap builds a map representing all params in the DSN.
// This does not rely on mysql.ParseDSN because that handles some vars
// separately; i.e. mysql.Config's params field does NOT include all
// params that are passed in!
func paramMap(dsn string) map[string]string {
	parts := strings.Split(dsn, "?")
	if len(parts) == 1 {
		return make(map[string]string)
	}
	params := parts[len(parts)-1]
	values, _ := url.ParseQuery(params)

	// Convert values, which is map[string][]string, to single-valued map[string]string
	// i.e. if a param is present multiple times, we only keep the first value
	result := make(map[string]string, len(values))
	for key := range values {
		result[key] = values.Get(key)
	}
	return result
}

// MergeParamStrings combines any number of query-string-style formatted DB
// connection parameter strings. In case of conflicts for any given parameter,
// values from later args override earlier args.
// This is inefficient and should be avoided in hot paths; eventually we will
// move away from DSNs and use Connectors instead, which will remove the need
// for this logic.
func MergeParamStrings(params ...string) string {
	if len(params) == 0 {
		return ""
	} else if len(params) == 1 {
		return params[0]
	}
	base, _ := url.ParseQuery(params[0])
	for _, overrides := range params[1:] {
		v, _ := url.ParseQuery(overrides)
		for name := range v {
			base.Set(name, v.Get(name))
		}
	}
	return base.Encode()
}

// sqlModeFilter maps sql_mode values (which must be in all caps) to true values
// to indicate that these sql_mode values should be filtered out.
type sqlModeFilter map[string]bool

// IntrospectionBadSQLModes indicates which sql_mode values are problematic for
// schema introspection purposes.
var IntrospectionBadSQLModes = sqlModeFilter{
	"ANSI":                     true,
	"ANSI_QUOTES":              true,
	"NO_FIELD_OPTIONS":         true,
	"NO_KEY_OPTIONS":           true,
	"NO_TABLE_OPTIONS":         true,
	"IGNORE_BAD_TABLE_OPTIONS": true, // Only present in MariaDB
}

// NonPortableSQLModes indicates which sql_mode values are not available in all
// flavors.
var NonPortableSQLModes = sqlModeFilter{
	"NO_AUTO_CREATE_USER": true, // Not present in MySQL 8.0+
	"NO_FIELD_OPTIONS":    true, // Not present in MySQL 8.0+
	"NO_KEY_OPTIONS":      true, // Not present in MySQL 8.0+
	"NO_TABLE_OPTIONS":    true, // Not present in MySQL 8.0+
	"DB2":                 true, // Not present in MySQL 8.0+
	"MAXDB":               true, // Not present in MySQL 8.0+
	"MSSQL":               true, // Not present in MySQL 8.0+
	"MYSQL323":            true, // Not present in MySQL 8.0+
	"MYSQL40":             true, // Not present in MySQL 8.0+
	"ORACLE":              true, // Not present in MySQL 8.0+
	"POSTGRESQL":          true, // Not present in MySQL 8.0+

	"TIME_TRUNCATE_FRACTIONAL": true, // Only present in MySQL 8.0+

	"IGNORE_BAD_TABLE_OPTIONS": true, // Only present in MariaDB
	"EMPTY_STRING_IS_NULL":     true, // Only present in MariaDB 10.3+
	"SIMULTANEOUS_ASSIGNMENT":  true, // Only present in MariaDB 10.3+
	"TIME_ROUND_FRACTIONAL":    true, // Only present in MariaDB 10.4+
}

// FilterSQLMode splits the supplied comma-separated orig sql_mode value and
// filters out any sql_mode values which map to true values in the supplied
// sqlModeFilter mapping.
func FilterSQLMode(orig string, remove sqlModeFilter) string {
	if orig == "" {
		return ""
	}
	origModes := strings.Split(orig, ",")
	keepModes := filterSQLMode(origModes, remove)
	if len(keepModes) == len(origModes) {
		return orig
	}
	return strings.Join(keepModes, ",")
}

func filterSQLMode(origModes []string, remove sqlModeFilter) []string {
	keepModes := make([]string, 0, len(origModes))
	for _, mode := range origModes {
		if !remove[mode] {
			keepModes = append(keepModes, mode)
		}
	}
	return keepModes
}

// longestIncreasingSubsequence implements an algorithm useful in computing
// diffs for column order or trigger order.
func longestIncreasingSubsequence(input []int) []int {
	if len(input) < 2 {
		return input
	}
	candidateLists := make([][]int, 1, len(input))
	candidateLists[0] = []int{input[0]}
	for i := 1; i < len(input); i++ {
		comp := input[i]
		if comp < candidateLists[0][0] {
			candidateLists[0][0] = comp
		} else if longestList := candidateLists[len(candidateLists)-1]; comp > longestList[len(longestList)-1] {
			newList := make([]int, len(longestList)+1)
			copy(newList, longestList)
			newList[len(longestList)] = comp
			candidateLists = append(candidateLists, newList)
		} else {
			for j := len(candidateLists) - 2; j >= 0; j-- {
				if thisList, nextList := candidateLists[j], candidateLists[j+1]; comp > thisList[len(thisList)-1] {
					copy(nextList, thisList)
					nextList[len(nextList)-1] = comp
					break
				}
			}
		}
	}
	return candidateLists[len(candidateLists)-1]
}
