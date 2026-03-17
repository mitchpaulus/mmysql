package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type connOpts struct {
	user     string
	password string
	host     string
	database string
}

func addConnFlags(fs *flag.FlagSet, opts *connOpts) {
	fs.StringVar(&opts.user, "user", "", "")
	fs.StringVar(&opts.user, "u", "", "")
	fs.StringVar(&opts.password, "password", "", "")
	fs.StringVar(&opts.password, "p", "", "")
	fs.StringVar(&opts.host, "host", "", "")
	fs.StringVar(&opts.host, "H", "", "")
	fs.StringVar(&opts.database, "database", "", "")
	fs.StringVar(&opts.database, "d", "", "")
}

func (o *connOpts) applyEnv() {
	if o.user == "" {
		o.user = os.Getenv("MMYSQLUSER")
	}
	if o.password == "" {
		o.password = os.Getenv("MMYSQLPASSWORD")
	}
	if o.host == "" {
		o.host = os.Getenv("MMYSQLHOST")
	}
	if o.database == "" {
		o.database = os.Getenv("MMYSQLDATABASE")
	}
}

func (o *connOpts) open() (*sql.DB, error) {
	host := o.host
	if host == "" {
		host = "localhost"
	}
	if !strings.Contains(host, ":") {
		host = host + ":3306"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true",
		o.user, o.password, host, o.database)
	return sql.Open("mysql", dsn)
}

func connFlagsUsage() string {
	return `  -u, --user       MySQL user (default: $MMYSQLUSER)
  -p, --password   MySQL password (default: $MMYSQLPASSWORD)
  -H, --host       MySQL host (default: $MMYSQLHOST)
  -d, --database   MySQL database (default: $MMYSQLDATABASE)`
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "insert":
			cmdInsert(os.Args[2:])
			return
		case "upsert":
			cmdUpsert(os.Args[2:])
			return
		case "update":
			cmdUpdate(os.Args[2:])
			return
		}
	}
	cmdQuery()
}

func cmdQuery() {
	fs := flag.NewFlagSet("mmysql", flag.ExitOnError)
	fs.SetOutput(os.Stdout)
	var opts connOpts
	addConnFlags(fs, &opts)
	fs.Usage = func() {
		fmt.Fprintf(os.Stdout, "Usage: mmysql [options] <query>\n\n")
		fmt.Fprintf(os.Stdout, "Commands:\n")
		fmt.Fprintf(os.Stdout, "  insert    Insert JSON data into a table\n")
		fmt.Fprintf(os.Stdout, "  upsert    Insert or update JSON data in a table\n")
		fmt.Fprintf(os.Stdout, "  update    Update rows matching key columns\n\n")
		fmt.Fprintf(os.Stdout, "Options:\n")
		fmt.Fprintln(os.Stdout, connFlagsUsage())
	}
	fs.Parse(os.Args[1:])
	opts.applyEnv()

	query := strings.Join(fs.Args(), " ")
	if query == "" {
		fi, _ := os.Stdin.Stat()
		if fi.Mode()&os.ModeCharDevice != 0 {
			fmt.Fprintln(os.Stderr, "error: no query provided and stdin is a terminal")
			fmt.Fprintln(os.Stderr, "usage: mmysql [options] <query>")
			fmt.Fprintln(os.Stderr, "       echo 'SELECT 1' | mmysql [options]")
			os.Exit(1)
		}
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			fatal("reading stdin: %v", err)
		}
		query = strings.TrimSpace(string(b))
		if query == "" {
			fatal("empty query from stdin")
		}
	}

	db, err := opts.open()
	if err != nil {
		fatal("%v", err)
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		fatal("%v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		fatal("%v", err)
	}

	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			fatal("%v", err)
		}
		row := make(map[string]any, len(columns))
		for i, col := range columns {
			v := values[i]
			if b, ok := v.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = v
			}
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		fatal("%v", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		fatal("%v", err)
	}
}

func interpolateSQL(query string, vals []any) string {
	var b strings.Builder
	vi := 0
	for i := 0; i < len(query); i++ {
		if query[i] == '?' && vi < len(vals) {
			v := vals[vi]
			vi++
			switch val := v.(type) {
			case nil:
				b.WriteString("NULL")
			case string:
				b.WriteByte('\'')
				b.WriteString(strings.ReplaceAll(strings.ReplaceAll(val, `\`, `\\`), `'`, `\'`))
				b.WriteByte('\'')
			case float64:
				b.WriteString(fmt.Sprintf("%v", val))
			case bool:
				if val {
					b.WriteString("1")
				} else {
					b.WriteString("0")
				}
			default:
				b.WriteString(fmt.Sprintf("'%v'", val))
			}
		} else {
			b.WriteByte(query[i])
		}
	}
	return b.String()
}

// parseJSONRows reads a table name and JSON data from args/stdin,
// returning the table name and parsed rows.
func parseTableAndJSON(cmdName string, args []string) (string, []map[string]any) {
	if len(args) == 0 {
		fatal("table name required\nusage: mmysql %s [options] <table> [json string]", cmdName)
	}
	table := args[0]
	remaining := args[1:]

	var jsonData string
	if len(remaining) > 0 {
		jsonData = strings.Join(remaining, " ")
	} else {
		fi, _ := os.Stdin.Stat()
		if fi.Mode()&os.ModeCharDevice != 0 {
			fmt.Fprintln(os.Stderr, "error: no JSON data provided and stdin is a terminal")
			fmt.Fprintf(os.Stderr, "usage: mmysql %s [options] <table> [json string]\n", cmdName)
			fmt.Fprintf(os.Stderr, "       echo '{\"col\":\"val\"}' | mmysql %s [options] <table>\n", cmdName)
			os.Exit(1)
		}
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			fatal("reading stdin: %v", err)
		}
		jsonData = strings.TrimSpace(string(b))
		if jsonData == "" {
			fatal("empty JSON from stdin")
		}
	}

	var rows []map[string]any
	jsonData = strings.TrimSpace(jsonData)
	if strings.HasPrefix(jsonData, "[") {
		if err := json.Unmarshal([]byte(jsonData), &rows); err != nil {
			fatal("invalid JSON array: %v", err)
		}
	} else if strings.HasPrefix(jsonData, "{") {
		var single map[string]any
		if err := json.Unmarshal([]byte(jsonData), &single); err != nil {
			fatal("invalid JSON object: %v", err)
		}
		rows = []map[string]any{single}
	} else {
		fatal("JSON must be an object or array of objects")
	}

	if len(rows) == 0 {
		fatal("no rows to insert")
	}

	return table, rows
}

type stmtInfo struct {
	sql  string
	vals []any
}

func quoteTable(name string) string {
	if i := strings.IndexByte(name, '.'); i >= 0 {
		return "`" + name[:i] + "`.`" + name[i+1:] + "`"
	}
	return "`" + name + "`"
}

func buildStatements(table string, rows []map[string]any, ignore bool, upsert bool) []stmtInfo {
	const chunkSize = 1000

	type keyGroup struct {
		cols []string
		rows []map[string]any
	}
	groups := make(map[string]*keyGroup)
	var groupOrder []string
	for _, row := range rows {
		cols := make([]string, 0, len(row))
		for k := range row {
			cols = append(cols, k)
		}
		sort.Strings(cols)
		key := strings.Join(cols, "\x00")
		if _, ok := groups[key]; !ok {
			groups[key] = &keyGroup{cols: cols}
			groupOrder = append(groupOrder, key)
		}
		groups[key].rows = append(groups[key].rows, row)
	}

	var stmts []stmtInfo

	for _, key := range groupOrder {
		g := groups[key]
		insertKw := "INSERT"
		if ignore {
			insertKw = "INSERT IGNORE"
		}

		quotedCols := make([]string, len(g.cols))
		for j, c := range g.cols {
			quotedCols[j] = "`" + c + "`"
		}
		placeholderRow := "(" + strings.Repeat("?, ", len(g.cols)-1) + "?)"

		var upsertClause string
		if upsert {
			updates := make([]string, len(g.cols))
			for j, c := range quotedCols {
				updates[j] = fmt.Sprintf("%s=VALUES(%s)", c, c)
			}
			upsertClause = " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ", ")
		}

		for i := 0; i < len(g.rows); i += chunkSize {
			end := i + chunkSize
			if end > len(g.rows) {
				end = len(g.rows)
			}
			chunk := g.rows[i:end]

			allPlaceholders := make([]string, len(chunk))
			for j := range chunk {
				allPlaceholders[j] = placeholderRow
			}

			stmt := fmt.Sprintf("%s INTO %s (%s) VALUES %s%s",
				insertKw, quoteTable(table),
				strings.Join(quotedCols, ", "),
				strings.Join(allPlaceholders, ", "),
				upsertClause)

			vals := make([]any, 0, len(chunk)*len(g.cols))
			for _, row := range chunk {
				for _, c := range g.cols {
					vals = append(vals, row[c])
				}
			}

			stmts = append(stmts, stmtInfo{sql: stmt, vals: vals})
		}
	}

	return stmts
}

func execStatements(opts *connOpts, stmts []stmtInfo, dryRun bool) {
	if dryRun {
		for _, s := range stmts {
			fmt.Printf("%s;\n", interpolateSQL(s.sql, s.vals))
		}
		return
	}

	db, err := opts.open()
	if err != nil {
		fatal("%v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		fatal("%v", err)
	}

	var totalAffected int64
	for _, s := range stmts {
		result, err := tx.Exec(s.sql, s.vals...)
		if err != nil {
			tx.Rollback()
			fatal("%v", err)
		}
		affected, _ := result.RowsAffected()
		totalAffected += affected
	}

	if err := tx.Commit(); err != nil {
		fatal("%v", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(map[string]any{"rows_affected": totalAffected})
}

func cmdInsert(args []string) {
	fs := flag.NewFlagSet("mmysql insert", flag.ExitOnError)
	fs.SetOutput(os.Stdout)
	var opts connOpts
	addConnFlags(fs, &opts)
	var ignore, dryRun bool
	fs.BoolVar(&ignore, "ignore", false, "")
	fs.BoolVar(&ignore, "I", false, "")
	fs.BoolVar(&dryRun, "dry-run", false, "")
	fs.BoolVar(&dryRun, "n", false, "")
	fs.Usage = func() {
		fmt.Fprintf(os.Stdout, "Usage: mmysql insert [options] <table> [json string]\n\n")
		fmt.Fprintf(os.Stdout, "Options:\n")
		fmt.Fprintln(os.Stdout, connFlagsUsage())
		fmt.Fprintf(os.Stdout, "  -I, --ignore     Use INSERT IGNORE\n")
		fmt.Fprintf(os.Stdout, "  -n, --dry-run    Print SQL without executing\n")
	}
	fs.Parse(args)
	opts.applyEnv()

	table, rows := parseTableAndJSON("insert", fs.Args())
	stmts := buildStatements(table, rows, ignore, false)
	execStatements(&opts, stmts, dryRun)
}

type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ", ") }
func (s *stringSlice) Set(val string) error {
	*s = append(*s, val)
	return nil
}

func parseWhere(expr string) (string, []string) {
	var buf strings.Builder
	var paramCols []string
	for i := 0; i < len(expr); i++ {
		if expr[i] == '~' {
			j := i + 1
			for j < len(expr) && (expr[j] >= 'a' && expr[j] <= 'z' || expr[j] >= 'A' && expr[j] <= 'Z' || expr[j] >= '0' && expr[j] <= '9' || expr[j] == '_') {
				j++
			}
			if j > i+1 {
				paramCols = append(paramCols, expr[i+1:j])
				buf.WriteByte('?')
				i = j - 1
				continue
			}
		}
		buf.WriteByte(expr[i])
	}
	return buf.String(), paramCols
}

func buildUpdateStatements(table string, keys []string, whereSQL string, whereParamCols []string, rows []map[string]any) ([]stmtInfo, error) {
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		keySet[k] = true
	}

	var stmts []stmtInfo
	for i, row := range rows {
		for _, k := range keys {
			if _, ok := row[k]; !ok {
				return nil, fmt.Errorf("row %d: missing key column %q", i, k)
			}
		}
		for _, col := range whereParamCols {
			if _, ok := row[col]; !ok {
				return nil, fmt.Errorf("row %d: missing ~%s column referenced in --where", i, col)
			}
		}

		setCols := make([]string, 0, len(row))
		for col := range row {
			if !keySet[col] {
				setCols = append(setCols, col)
			}
		}
		sort.Strings(setCols)
		if len(setCols) == 0 {
			return nil, fmt.Errorf("row %d: no columns to SET (all columns are keys)", i)
		}

		setParts := make([]string, len(setCols))
		vals := make([]any, 0, len(setCols)+len(keys)+len(whereParamCols))
		for j, col := range setCols {
			setParts[j] = "`" + col + "` = ?"
			vals = append(vals, row[col])
		}
		for _, k := range keys {
			vals = append(vals, row[k])
		}

		whereParts := make([]string, len(keys))
		for j, k := range keys {
			whereParts[j] = "`" + k + "` = ?"
		}
		where := strings.Join(whereParts, " AND ")

		if whereSQL != "" {
			for _, col := range whereParamCols {
				vals = append(vals, row[col])
			}
			where = "(" + where + ") AND (" + whereSQL + ")"
		}

		stmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s", quoteTable(table), strings.Join(setParts, ", "), where)
		stmts = append(stmts, stmtInfo{sql: stmt, vals: vals})
	}
	return stmts, nil
}

func cmdUpdate(args []string) {
	fs := flag.NewFlagSet("mmysql update", flag.ExitOnError)
	fs.SetOutput(os.Stdout)
	var opts connOpts
	addConnFlags(fs, &opts)
	var dryRun bool
	var keys stringSlice
	var whereExpr string
	fs.BoolVar(&dryRun, "dry-run", false, "")
	fs.BoolVar(&dryRun, "n", false, "")
	fs.Var(&keys, "k", "")
	fs.Var(&keys, "key", "")
	fs.StringVar(&whereExpr, "where", "", "")
	fs.Usage = func() {
		fmt.Fprintf(os.Stdout, "Usage: mmysql update [options] -k <key> [--where '<expr>'] <table> [json string]\n\n")
		fmt.Fprintf(os.Stdout, "Options:\n")
		fmt.Fprintln(os.Stdout, connFlagsUsage())
		fmt.Fprintf(os.Stdout, "  -k, --key        Key column for WHERE match (repeatable, required)\n")
		fmt.Fprintf(os.Stdout, "      --where      Additional WHERE condition (use ~col for row values)\n")
		fmt.Fprintf(os.Stdout, "  -n, --dry-run    Print SQL without executing\n")
	}
	fs.Parse(args)
	opts.applyEnv()

	if len(keys) == 0 {
		fatal("at least one -k/--key flag is required")
	}

	var whereSQL string
	var whereParamCols []string
	if whereExpr != "" {
		whereSQL, whereParamCols = parseWhere(whereExpr)
	}

	table, rows := parseTableAndJSON("update", fs.Args())
	stmts, err := buildUpdateStatements(table, keys, whereSQL, whereParamCols, rows)
	if err != nil {
		fatal("%v", err)
	}
	execStatements(&opts, stmts, dryRun)
}

func cmdUpsert(args []string) {
	fs := flag.NewFlagSet("mmysql upsert", flag.ExitOnError)
	fs.SetOutput(os.Stdout)
	var opts connOpts
	addConnFlags(fs, &opts)
	var dryRun bool
	fs.BoolVar(&dryRun, "dry-run", false, "")
	fs.BoolVar(&dryRun, "n", false, "")
	fs.Usage = func() {
		fmt.Fprintf(os.Stdout, "Usage: mmysql upsert [options] <table> [json string]\n\n")
		fmt.Fprintf(os.Stdout, "Options:\n")
		fmt.Fprintln(os.Stdout, connFlagsUsage())
		fmt.Fprintf(os.Stdout, "  -n, --dry-run    Print SQL without executing\n")
	}
	fs.Parse(args)
	opts.applyEnv()

	table, rows := parseTableAndJSON("upsert", fs.Args())
	stmts := buildStatements(table, rows, false, true)
	execStatements(&opts, stmts, dryRun)
}
