package main

import (
	"fmt"
	"os"
	"os/user"
	"strconv"
	"syscall"

	flags "github.com/jessevdk/go-flags"
	"golang.org/x/crypto/ssh/terminal"
	pg "gopkg.in/pg.v4"

	"github.com/aweris/pg_dump_sample/dump"
)

type Options struct {
	Host             string
	Port             int
	Username         string
	NoPasswordPrompt bool
	Password         string
	ManifestFile     string
	OutputFile       string
	Database         string
	UseTLS           bool
}

func parseArgs() (*Options, error) {
	var opts struct {
		Host             string `short:"h" long:"host" default:"/tmp" default-mask:"local socket" env:"PGHOST" description:"Database server host or socket directory"`
		Port             string `short:"p" long:"port" default:"5432" env:"PGPORT" description:"Database server port"`
		Username         string `short:"U" long:"username" default-mask:"current user" env:"PGUSER" description:"Database user name"`
		NoPasswordPrompt bool   `short:"w" long:"no-password" description:"Don't prompt for password"`
		ManifestFile     string `short:"f" long:"manifest-file" description:"Path to manifest file"`
		OutputFile       string `short:"o" long:"output-file" description:"Path to the output file"`
		UseTLS           bool   `short:"s" long:"tls" description:"Use SSL/TLS database connection"`
		Help             bool   `long:"help" description:"Show help"`
	}

	parser := flags.NewParser(&opts, flags.None)
	parser.Usage = "[options] database"

	args, err := parser.Parse()
	if err != nil {
		parser.WriteHelp(os.Stderr)
		return nil, err
	}

	if opts.Help {
		parser.WriteHelp(os.Stdout)
		os.Exit(0)
	}

	// Manifest file
	if opts.ManifestFile == "" {
		parser.WriteHelp(os.Stderr)
		return nil, fmt.Errorf("required flag `-f, --manifest-file` not specified")
	}

	// Username
	if opts.Username == "" {
		currentUser, err := user.Current()
		if err != nil {
			return nil, fmt.Errorf("failed to get current user")
		}
		opts.Username = currentUser.Username
	}

	// Port
	port, err := strconv.Atoi(opts.Port)
	if err != nil {
		parser.WriteHelp(os.Stderr)
		return nil, fmt.Errorf("port must be a number 0-65535")
	}

	// Database
	Database := ""
	if len(args) == 0 {
		Database = os.Getenv("PGDATABASE")
	} else if len(args) == 1 {
		Database = args[0]
	} else if len(args) > 1 {
		parser.WriteHelp(os.Stderr)
		return nil, fmt.Errorf("only one database may be specified at a time")
	}

	// Password
	Password := os.Getenv("PGPASSWORD")

	return &Options{
		Host:             opts.Host,
		Port:             port,
		Username:         opts.Username,
		NoPasswordPrompt: opts.NoPasswordPrompt,
		Password:         Password,
		ManifestFile:     opts.ManifestFile,
		OutputFile:       opts.OutputFile,
		UseTLS:           opts.UseTLS,
		Database:         Database,
	}, nil
}

func connectDB(opts *pg.Options) (*pg.DB, error) {
	db := pg.Connect(opts)
	var model []struct {
		X string
	}
	_, err := db.Query(&model, `SELECT 1 AS x`)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func readPassword(username string) (string, error) {
	fmt.Fprintf(os.Stderr, "Password for %s: ", username)
	password, err := terminal.ReadPassword(int(syscall.Stdin))
	fmt.Print("\n")
	return string(password), err
}

func main() {
	// Parse command-line arguments
	opts, err := parseArgs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Open output file
	output := os.Stdout
	if opts.OutputFile != "" {
		output, err = os.OpenFile(opts.OutputFile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}

	// Connect to the DB
	db, err := connectDB(&pg.Options{
		Addr:     fmt.Sprintf("%s:%d", opts.Host, opts.Port),
		Database: opts.Database,
		SSL:      opts.UseTLS,
		User:     opts.Username,
		Password: opts.Password,
	})
	if err != nil {
		password := opts.Password
		if !opts.NoPasswordPrompt {
			// Read database password from the terminal
			password, err = readPassword(opts.Username)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		}

		// Try again, this time with password
		db, err = connectDB(&pg.Options{
			Addr:     fmt.Sprintf("%s:%d", opts.Host, opts.Port),
			Database: opts.Database,
			SSL:      opts.UseTLS,
			User:     opts.Username,
			Password: password,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}

	// Get Manifest File
	manifest := dump.NewManifest(opts.ManifestFile)

	// Make the dump
	err = dump.MakeDump(db, manifest, output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
