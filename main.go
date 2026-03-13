package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	flag.CommandLine.SetOutput(os.Stdout)
	flag.Usage = func() {
		fmt.Fprintf(os.Stdout, "Usage: mmysql [options] <query>\n\n")
		fmt.Fprintf(os.Stdout, "Options:\n")
		fmt.Fprintf(os.Stdout, "  -u, --user       MySQL user (default: $MMYSQLUSER)\n")
		fmt.Fprintf(os.Stdout, "  -p, --password   MySQL password (default: $MMYSQLPASSWORD)\n")
		fmt.Fprintf(os.Stdout, "  -H, --host       MySQL host (default: $MMYSQLHOST)\n")
		fmt.Fprintf(os.Stdout, "  -d, --database   MySQL database (default: $MMYSQLDATABASE)\n")
	}
	var user, password, host, database string
	flag.StringVar(&user, "user", "", "")
	flag.StringVar(&user, "u", "", "")
	flag.StringVar(&password, "password", "", "")
	flag.StringVar(&password, "p", "", "")
	flag.StringVar(&host, "host", "", "")
	flag.StringVar(&host, "H", "", "")
	flag.StringVar(&database, "database", "", "")
	flag.StringVar(&database, "d", "", "")
	flag.Parse()

	if user == "" {
		user = os.Getenv("MMYSQLUSER")
	}
	if password == "" {
		password = os.Getenv("MMYSQLPASSWORD")
	}
	if host == "" {
		host = os.Getenv("MMYSQLHOST")
	}
	if database == "" {
		database = os.Getenv("MMYSQLDATABASE")
	}

	query := strings.Join(flag.Args(), " ")
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
			fmt.Fprintf(os.Stderr, "error reading stdin: %v\n", err)
			os.Exit(1)
		}
		query = strings.TrimSpace(string(b))
		if query == "" {
			fmt.Fprintln(os.Stderr, "error: empty query from stdin")
			os.Exit(1)
		}
	}

	if host == "" {
		host = "localhost"
	}
	if !strings.Contains(host, ":") {
		host = host + ":3306"
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true",
		user, password, host, database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
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
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
