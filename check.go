package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/go-sql-driver/mysql"
)

// checkFailed marks the process exit code without printing anything itself.
var checkFailed bool

func cmdCheck(args []string) {
	fs := flag.NewFlagSet("mmysql check", flag.ExitOnError)
	fs.SetOutput(os.Stdout)
	var opts connOpts
	addConnFlags(fs, &opts)
	var fingerprint bool
	fs.BoolVar(&fingerprint, "fingerprint", false, "")
	addListEncodingsFlag(fs)
	fs.Usage = func() {
		fmt.Fprintf(os.Stdout, "Usage: mmysql check [options]\n\n")
		fmt.Fprintf(os.Stdout, "Tests the connection in stages (settings, DNS, TCP, login) and prints\n")
		fmt.Fprintf(os.Stdout, "a hint for the first stage that fails. The password is never printed.\n\n")
		fmt.Fprintf(os.Stdout, "Options:\n")
		fmt.Fprintln(os.Stdout, connFlagsUsage())
		fmt.Fprintf(os.Stdout, "      --fingerprint  Also print a short SHA-256 prefix of the password\n")
		fmt.Fprintf(os.Stdout, "                     so two people can confirm they hold the same value\n")
		fmt.Fprintf(os.Stdout, "      --list-encodings\n")
		fmt.Fprintf(os.Stdout, "                     Print all supported encodings and exit\n")
	}
	fs.Parse(args)
	opts.applyEnv()

	printSettings(&opts, fingerprint)
	fmt.Println()

	if !checkNetwork(&opts) {
		os.Exit(1)
	}
	if !checkLogin(&opts) {
		os.Exit(1)
	}
}

func printSettings(o *connOpts, fingerprint bool) {
	fmt.Println("Settings:")
	fmt.Printf("  user:      %s\n", describeValue(o.user, o.source["user"]))
	fmt.Printf("  password:  %s\n", describePassword(o.password, o.source["password"], fingerprint))
	fmt.Printf("  host:      %s\n", describeValue(o.addr(), o.source["host"]))
	fmt.Printf("  database:  %s\n", describeValue(o.database, o.source["database"]))
}

func describeValue(v, source string) string {
	if v == "" {
		return fmt.Sprintf("(not set)  [%s]", source)
	}
	s := fmt.Sprintf("%s  [%s]", v, source)
	if warn := whitespaceWarning(v); warn != "" {
		s += "  " + warn
	}
	return s
}

func describePassword(pw, source string, fingerprint bool) string {
	if pw == "" {
		return fmt.Sprintf("(not set)  [%s]", source)
	}
	parts := []string{fmt.Sprintf("set, %d chars", len([]rune(pw)))}
	if fingerprint {
		sum := sha256.Sum256([]byte(pw))
		parts = append(parts, "sha256:"+hex.EncodeToString(sum[:])[:8])
	}
	s := fmt.Sprintf("%s  [%s]", strings.Join(parts, ", "), source)
	for _, w := range passwordWarnings(pw) {
		s += "\n             " + w
	}
	return s
}

func whitespaceWarning(v string) string {
	if strings.TrimSpace(v) != v {
		return "WARNING: has leading or trailing whitespace"
	}
	return ""
}

func passwordWarnings(pw string) []string {
	var warns []string
	if w := whitespaceWarning(pw); w != "" {
		warns = append(warns, w)
	}
	if strings.ContainsAny(pw, "$!`\\\"'%^&|<>") {
		warns = append(warns, "note: contains shell-special characters; quote it with single quotes")
	}
	for _, r := range pw {
		if r > unicode.MaxASCII {
			warns = append(warns, "WARNING: contains non-ASCII characters (smart quotes from a document?)")
			break
		}
	}
	for _, r := range pw {
		if unicode.IsControl(r) {
			warns = append(warns, "WARNING: contains control characters (stray newline or tab?)")
			break
		}
	}
	return warns
}

// checkNetwork resolves the host and opens a plain TCP connection so that
// network failures are reported separately from authentication failures.
func checkNetwork(o *connOpts) bool {
	addr := o.addr()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		fmt.Printf("DNS:       FAIL  invalid host %q: %v\n", addr, err)
		return false
	}

	start := time.Now()
	ips, err := net.LookupHost(host)
	if err != nil {
		fmt.Printf("DNS:       FAIL  %v\n", err)
		fmt.Printf("           hint: the host name did not resolve. Check MMYSQLHOST or --host for typos.\n")
		return false
	}
	fmt.Printf("DNS:       ok    %s -> %s (%s)\n", host, strings.Join(ips, ", "), time.Since(start).Round(time.Millisecond))

	start = time.Now()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		fmt.Printf("TCP:       FAIL  %v\n", err)
		var nerr net.Error
		if errors.As(err, &nerr) && nerr.Timeout() {
			fmt.Printf("           hint: no response on port %s. A firewall is probably dropping the connection,\n", port)
			fmt.Printf("           or the server is not listening on this address.\n")
		} else {
			fmt.Printf("           hint: the host is reachable but nothing accepted the connection on port %s.\n", port)
			fmt.Printf("           Check that MySQL is running, the port is right, and bind-address allows remote clients.\n")
		}
		return false
	}
	conn.Close()
	fmt.Printf("TCP:       ok    connected to %s (%s)\n", addr, time.Since(start).Round(time.Millisecond))
	return true
}

func checkLogin(o *connOpts) bool {
	db, err := o.open()
	if err != nil {
		fmt.Printf("Login:     FAIL  %v\n", err)
		return false
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	if err := db.PingContext(ctx); err != nil {
		fmt.Printf("Login:     FAIL  %v\n", err)
		printLoginHint(err, o)
		return false
	}
	fmt.Printf("Login:     ok    (%s)\n", time.Since(start).Round(time.Millisecond))

	var currentUser, sessionUser, version string
	err = db.QueryRowContext(ctx, "SELECT CURRENT_USER(), USER(), VERSION()").Scan(&currentUser, &sessionUser, &version)
	if err != nil {
		fmt.Printf("Server:    ?     could not query session info: %v\n", err)
		return true
	}
	fmt.Printf("Server:    %s\n", version)
	fmt.Printf("Session:   connected as %s, matched account %s\n", sessionUser, currentUser)

	var name, cipher string
	if err := db.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&name, &cipher); err == nil {
		if cipher != "" {
			fmt.Printf("TLS:       on    %s\n", cipher)
		} else {
			fmt.Printf("TLS:       off   server does not offer TLS; traffic is unencrypted\n")
		}
	}

	if o.database != "" {
		var dbName *string
		if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&dbName); err == nil && dbName != nil {
			fmt.Printf("Database:  ok    using %s\n", *dbName)
		}
	} else {
		fmt.Printf("Database:  none selected; set MMYSQLDATABASE or --database to run queries against one\n")
	}
	return true
}

func printLoginHint(err error, o *connOpts) {
	var merr *mysql.MySQLError
	if !errors.As(err, &merr) {
		if strings.Contains(err.Error(), "TLS") || strings.Contains(err.Error(), "secure") {
			fmt.Printf("           hint: the server requires TLS for this account.\n")
		}
		return
	}
	p := func(format string, args ...any) { fmt.Printf("           "+format+"\n", args...) }
	switch merr.Number {
	case 1045:
		p("hint: the server was reached and an account matched user %q from this client's address,", o.user)
		p("but the login was rejected. Network and firewall are fine. Two causes give this exact error:")
		p("  1. The password does not match. Compare the length above with what you expect. Common")
		p("     causes are a stale MMYSQLPASSWORD in the environment, shell expansion of special")
		p("     characters, or a trailing space. Run with --fingerprint to compare values safely.")
		p("  2. The account has REQUIRE SSL or X509 and the server did not offer TLS, or the")
		p("     account requires a client certificate. Check with SHOW CREATE USER '%s'@'%%';", o.user)
	case 1130:
		p("hint: the server accepted the connection but no account for user %q allows this", o.user)
		p("client address. A DBA needs to create or grant 'user'@'%%' or 'user'@'<your-ip>'.")
	case 1044:
		p("hint: the login worked but the account has no privileges on database %q.", o.database)
	case 1049:
		p("hint: the login worked but database %q does not exist. Check MMYSQLDATABASE or --database.", o.database)
	case 1251, 2059:
		p("hint: the account uses an authentication plugin this client could not negotiate.")
		p("A DBA can run: ALTER USER ... IDENTIFIED WITH mysql_native_password BY '...'")
	case 3159:
		p("hint: the server requires a TLS connection for this account.")
	case 1226, 1040:
		p("hint: the server is at its connection limit. Try again later.")
	}
}
