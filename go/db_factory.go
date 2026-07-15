// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mariadb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	mariadb "github.com/go-sql-driver/mysql"
)

// MariaDBDBFactory provides MariaDB-specific database connection creation.
// It uses the MariaDB-compatible go-sql-driver/mysql Config struct for proper
// wire-protocol DSN formatting.
type MariaDBDBFactory struct{}

// NewMariaDBDBFactory creates a new MariaDBDBFactory.
func NewMariaDBDBFactory() *MariaDBDBFactory {
	return &MariaDBDBFactory{}
}

// CreateDB creates a *sql.DB using sql.Open with a MariaDB-specific DSN.
func (f *MariaDBDBFactory) CreateDB(ctx context.Context, driverName string, opts map[string]string, logger *slog.Logger) (*sql.DB, error) {
	dsn, err := f.BuildMariaDBDSN(opts)
	if err != nil {
		return nil, err
	}

	// Force UTC timezone for all connections to ensure consistent timestamp handling.
	dsn, err = f.forceUTCTimezone(dsn, logger)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(0)
	return db, nil
}

// forceUTCTimezone parses the DSN and overrides the time_zone and loc parameters to UTC
func (f *MariaDBDBFactory) forceUTCTimezone(dsn string, logger *slog.Logger) (string, error) {
	cfg, err := mariadb.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("failed to parse DSN for timezone override: %v", err)
	}

	if existingTz, exists := cfg.Params["time_zone"]; exists && existingTz != "'+00:00'" && existingTz != "'UTC'" {
		if logger != nil {
			logger.Warn("time_zone parameter is not supported, overriding to UTC",
				"requested_timezone", existingTz,
				"reason", "UTC is required for ADBC MariaDB driver")
		}
	}

	if existingLoc, exists := cfg.Params["loc"]; exists && existingLoc != "UTC" {
		if logger != nil {
			logger.Warn("loc parameter is not supported, overriding to UTC",
				"requested_loc", existingLoc,
				"reason", "UTC is required for ADBC MariaDB driver")
		}
	}

	if cfg.Params == nil {
		cfg.Params = make(map[string]string)
	}
	cfg.Params["time_zone"] = "'+00:00'"
	cfg.Params["loc"] = "UTC"

	return cfg.FormatDSN(), nil
}

// buildMariaDBDSN constructs a MariaDB DSN from the provided options.
// Handles the following scenarios:
//  1. MariaDB URI: "mariadb://user:pass@host:port/schema?params" → converted to DSN
//  2. Full DSN: "user:pass@tcp(host:port)/db" → returned as-is or credentials updated
//  3. Plain host + credentials: "localhost:3306" + username/password → converted to DSN
func (f *MariaDBDBFactory) BuildMariaDBDSN(opts map[string]string) (string, error) {
	baseURI := opts[adbc.OptionKeyURI]
	username := opts[adbc.OptionKeyUsername]
	password := opts[adbc.OptionKeyPassword]

	// If no base URI provided, this is an error
	if baseURI == "" {
		// Return plain Go error. sqlwrapper will catch and wrap it with ErrorHelper and turn it into adbc error
		return "", fmt.Errorf("missing required option %s", adbc.OptionKeyURI)
	}

	// Check if this is a MariaDB URI (mariadb://)
	if strings.HasPrefix(baseURI, "mariadb://") {
		return f.parseToMariaDBDSN(baseURI, username, password)
	}

	// URI schemes are deliberately strict.  The underlying protocol library is
	// shared with MySQL, but accepting mysql:// here makes configuration errors
	// very hard to spot and misrepresents the server this driver targets.
	if parsed, err := url.Parse(baseURI); err == nil && parsed.Scheme != "" {
		return "", fmt.Errorf("unsupported URI scheme %q: expected mariadb://", parsed.Scheme)
	}

	if username == "" && password == "" {
		return baseURI, nil
	}
	return f.buildFromNativeDSN(baseURI, username, password)
}

// parseToMariaDBDSN converts a MariaDB URI to MariaDB DSN format.
// Examples:
//
//	mariadb://root@localhost:3306/demo → root@tcp(localhost:3306)/demo
//	mariadb://user:pass@host/db?charset=utf8mb4 → user:pass@tcp(host:3306)/db?charset=utf8mb4
//	mariadb://user@(/path/to/socket.sock)/db → user@unix(/path/to/socket.sock)/db
func (f *MariaDBDBFactory) parseToMariaDBDSN(mariadbURI, username, password string) (string, error) {
	u, err := url.Parse(mariadbURI)
	if err != nil {
		return "", fmt.Errorf("invalid MariaDB URI format: %v", err)
	}

	cfg := mariadb.NewConfig()

	if u.User != nil {
		cfg.User = u.User.Username()
		if pass, hasPass := u.User.Password(); hasPass {
			cfg.Passwd = pass
		}
	}

	if username != "" {
		cfg.User = username
	}
	if password != "" {
		cfg.Passwd = password
	}

	var dbPath string

	// MariaDB socket URIs have non-standard hostname patterns that require special handling after parsing.
	switch u.Hostname() {
	case "(":
		// Case 1: Socket with parentheses: mariadb://user@(/path/to/socket.sock)/db
		cfg.Net = "unix"

		closeParenIndex := strings.Index(u.Path, ")")
		if closeParenIndex == -1 {
			return "", fmt.Errorf("invalid MariaDB URI: missing closing ')' for socket path in %s", u.Path)
		}

		cfg.Addr = u.Path[:closeParenIndex]
		dbPath = u.Path[closeParenIndex+1:]

	case "":
		// Case 2: Empty host is invalid - hostname must be explicit
		// Use parentheses syntax for sockets: mariadb://user@(/path/to/socket)/db
		return "", fmt.Errorf("missing hostname in URI: %s. Use explicit hostname or socket syntax: mariadb://user@(socketpath)/db", mariadbURI)

	default:
		// Case 3: Regular TCP connection with a hostname
		cfg.Net = "tcp"
		if u.Port() != "" {
			cfg.Addr = u.Host
		} else {
			cfg.Addr = u.Host + ":3306"
		}
		dbPath = u.Path
	}

	// Extract database/schema from path
	if dbPath != "" && dbPath != "/" {
		// u.Path is already URL-decoded by url.Parse()
		// We just need to trim the leading slash.
		// cfg.FormatDSN() will correctly re-encode this if needed.
		cfg.DBName = strings.TrimPrefix(dbPath, "/")
	}

	dsn := cfg.FormatDSN()
	if u.RawQuery != "" {
		dsn += "?" + u.RawQuery
	}

	return dsn, nil
}

// buildFromNativeDSN handles MariaDB's native DSN format and plain host strings.
func (f *MariaDBDBFactory) buildFromNativeDSN(baseURI, username, password string) (string, error) {
	var cfg *mariadb.Config
	var err error

	if strings.Contains(baseURI, "@") || strings.Contains(baseURI, "/") {
		// Try to parse as existing MariaDB DSN
		cfg, err = mariadb.ParseDSN(baseURI)
		if err != nil {
			return "", fmt.Errorf("invalid MariaDB DSN format: %v", err)
		}
	} else {
		// Treat as plain host string
		cfg = mariadb.NewConfig()
		cfg.Addr = baseURI
		cfg.Net = "tcp"
	}

	// Override credentials if provided
	if username != "" {
		cfg.User = username
	}
	if password != "" {
		cfg.Passwd = password
	}

	return cfg.FormatDSN(), nil
}

var _ sqlwrapper.DBFactory = (*MariaDBDBFactory)(nil)
