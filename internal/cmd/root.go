// Copyright (c) 2025 Arc Engineering
// SPDX-License-Identifier: MIT

package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/yourorg/arc-sdk/db"
	"github.com/yourorg/arc-sdk/db/migrations"
)

// NewRootCmd creates the root command for arc-db.
func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "arc-db",
		Short: "Database operations",
		Long:  `Database operations including info, migrations, vacuum, and export.`,
		RunE:  func(cmd *cobra.Command, args []string) error { return cmd.Help() },
	}

	root.AddCommand(newInfoCmd())
	root.AddCommand(newMigrateCmd())
	root.AddCommand(newVacuumCmd())
	root.AddCommand(newExportCmd())
	root.AddCommand(newPathCmd())

	return root
}

func newInfoCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "info",
		Short: "Show database info and table counts",
		RunE: func(cmd *cobra.Command, args []string) error {
			path := db.DefaultDBPath()
			database, err := db.Open(path)
			if err != nil {
				return err
			}
			defer database.Close()

			fmt.Printf("DB path: %s\n", path)

			var ver string
			if err := database.QueryRow("SELECT sqlite_version();").Scan(&ver); err == nil {
				fmt.Printf("SQLite version: %s\n", ver)
			}

			fmt.Println()
			showCount := func(tbl string) {
				var cnt int
				err := database.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tbl)).Scan(&cnt)
				if err == nil {
					fmt.Printf("%-20s %d\n", tbl+":", cnt)
				}
			}

			showCount("schema_migrations")
			showCount("sessions")
			showCount("external_repos")
			showCount("env_backups")
			showCount("repo_dependencies")

			return nil
		},
	}
}

func newMigrateCmd() *cobra.Command {
	mc := &cobra.Command{
		Use:   "migrate",
		Short: "Migration commands",
		RunE:  func(cmd *cobra.Command, args []string) error { return cmd.Help() },
	}

	var pretty bool
	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show applied and available migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			path := db.DefaultDBPath()
			database, err := db.Open(path)
			if err != nil {
				return err
			}
			defer database.Close()

			fmt.Printf("DB path: %s\n\n", path)

			avail, _ := migrations.Embedded()
			applied, _ := migrations.Applied(database)

			if pretty {
				tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
				fmt.Fprintln(tw, "VERSION\tNAME\tAPPLIED")
				for _, m := range avail {
					appliedStr := "no"
					if _, ok := applied[m.Version]; ok {
						appliedStr = "yes"
					}
					fmt.Fprintf(tw, "%03d\t%s\t%s\n", m.Version, m.Name, appliedStr)
				}
				return tw.Flush()
			}

			fmt.Println("Applied:")
			if len(applied) == 0 {
				fmt.Println("  (none)")
			}
			keys := make([]int, 0, len(applied))
			for v := range applied {
				keys = append(keys, v)
			}
			sort.Ints(keys)
			for _, v := range keys {
				fmt.Printf("  %03d %s\n", v, applied[v])
			}

			fmt.Println("\nAvailable:")
			for _, m := range avail {
				mark := ""
				if _, ok := applied[m.Version]; ok {
					mark = " (applied)"
				}
				fmt.Printf("  %03d %s%s\n", m.Version, m.Name, mark)
			}
			return nil
		},
	}
	statusCmd.Flags().BoolVar(&pretty, "pretty", false, "Show migrations in a formatted table")
	mc.AddCommand(statusCmd)

	mc.AddCommand(&cobra.Command{
		Use:   "up",
		Short: "Apply pending migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			database, err := db.Open(db.DefaultDBPath())
			if err != nil {
				return err
			}
			defer database.Close()

			if err := migrations.RunMigrations(database); err != nil {
				return err
			}
			fmt.Println("Migrations applied (if any).")
			return nil
		},
	})

	return mc
}

func newVacuumCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "vacuum",
		Short: "Run VACUUM on the database",
		RunE: func(cmd *cobra.Command, args []string) error {
			path := db.DefaultDBPath()
			database, err := db.Open(path)
			if err != nil {
				return err
			}
			defer database.Close()

			if _, err := database.Exec("VACUUM"); err != nil {
				return err
			}
			fmt.Printf("VACUUM completed for %s\n", path)
			return nil
		},
	}
}

func newExportCmd() *cobra.Command {
	var tablesCSV string
	var outPath string

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export tables to JSONL",
		Long:  `Export database tables to JSONL format (one JSON object per line).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			database, err := db.Open(db.DefaultDBPath())
			if err != nil {
				return err
			}
			defer database.Close()

			tables := parseTableList(tablesCSV)
			if len(tables) == 0 {
				tables = []string{"sessions", "external_repos", "env_backups", "repo_dependencies"}
			}

			out, cleanup, err := openOutput(outPath)
			if err != nil {
				return err
			}
			defer cleanup()

			enc := json.NewEncoder(out)
			for _, tbl := range tables {
				if err := exportTable(database, tbl, enc); err != nil {
					return fmt.Errorf("export %s: %w", tbl, err)
				}
			}

			if out != os.Stdout {
				fmt.Printf("Exported %d tables to %s\n", len(tables), outPath)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&tablesCSV, "tables", "", "Comma-separated table list")
	cmd.Flags().StringVar(&outPath, "out", "", "Output file path (default: stdout)")

	return cmd
}

func newPathCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "path",
		Short: "Print database file path",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(db.DefaultDBPath())
		},
	}
}

func parseTableList(csv string) []string {
	if strings.TrimSpace(csv) == "" {
		return nil
	}
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func openOutput(path string) (*os.File, func(), error) {
	if strings.TrimSpace(path) == "" {
		return os.Stdout, func() {}, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { f.Close() }, nil
}

func exportTable(database *sql.DB, table string, enc *json.Encoder) error {
	var cnt int
	if err := database.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&cnt); err != nil || cnt == 0 {
		return nil
	}

	rows, err := database.Query("SELECT * FROM " + table)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}

		row := map[string]any{}
		for i, c := range cols {
			switch v := vals[i].(type) {
			case []byte:
				row[c] = string(v)
			default:
				row[c] = v
			}
		}

		obj := map[string]any{"table": table, "row": row, "ts": time.Now().Unix()}
		if err := enc.Encode(obj); err != nil {
			return err
		}
	}

	return rows.Err()
}
