package cmd

import (
	"runtime"
	"time"

	"github.com/hellofresh/klepto/pkg/dumper"
	"github.com/hellofresh/klepto/pkg/reader"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	// imports dumpers and readers
	_ "github.com/hellofresh/klepto/pkg/dumper/mysql"
	_ "github.com/hellofresh/klepto/pkg/dumper/postgres"
	_ "github.com/hellofresh/klepto/pkg/dumper/query"
	_ "github.com/hellofresh/klepto/pkg/reader/mysql"
	_ "github.com/hellofresh/klepto/pkg/reader/postgres"
)

type (
		// MirrorOptions represents the command options
		MirrorOptions struct {
			from string
			to   string
			concurrency int
			readOpts connOpts
			writeOpts connOpts
			srcDbPrefix string
			dstDbPrefix string
		}
	)

// NewMirrorCmd creates a new mirror command
func NewMirrorCmd() *cobra.Command {
	opts := new(MirrorOptions)
	cmd := &cobra.Command{
		Use:     "mirror",
		Short:   "Copies view definitions from one database to another.",
		PreRunE: initConfig,
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunMirror(opts)
		},
	}

	cmd.PersistentFlags().StringVarP(&opts.from, "from", "f", "root:root@tcp(localhost:3306)/klepto", "Database dsn to mirror from")
	cmd.PersistentFlags().StringVarP(&opts.to, "to", "t", "os://stdout/", "Database to output to (default writes to stdOut)")
	cmd.PersistentFlags().IntVar(&opts.concurrency, "concurrency", runtime.NumCPU(), "Sets the amount of dumps to be performed concurrently")
	cmd.PersistentFlags().StringVar(&opts.readOpts.timeout, "read-timeout", "5m", "Sets the timeout for read operations")
	cmd.PersistentFlags().StringVar(&opts.writeOpts.timeout, "write-timeout", "30s", "Sets the timeout for write operations")
	cmd.PersistentFlags().StringVar(&opts.readOpts.maxConnLifetime, "read-conn-lifetime", "0", "Sets the maximum amount of time a connection may be reused on the read database")
	cmd.PersistentFlags().IntVar(&opts.readOpts.maxConns, "read-max-conns", 5, "Sets the maximum number of open connections to the read database")
	cmd.PersistentFlags().IntVar(&opts.readOpts.maxIdleConns, "read-max-idle-conns", 0, "Sets the maximum number of connections in the idle connection pool for the read database")
	cmd.PersistentFlags().StringVar(&opts.writeOpts.maxConnLifetime, "write-conn-lifetime", "0", "Sets the maximum amount of time a connection may be reused on the write database")
	cmd.PersistentFlags().IntVar(&opts.writeOpts.maxConns, "write-max-conns", 5, "Sets the maximum number of open connections to the write database")
	cmd.PersistentFlags().IntVar(&opts.writeOpts.maxIdleConns, "write-max-idle-conns", 0, "Sets the maximum number of connections in the idle connection pool for the write database")
	cmd.PersistentFlags().StringVar(&opts.srcDbPrefix, "src-db-prefix", "", "Sets the source database prefix")
	cmd.PersistentFlags().StringVar(&opts.dstDbPrefix, "dst-db-prefix", "", "Sets the destination database prefix")

	return cmd
}

// RunMirror is the handler for the rootCmd
func RunMirror(opts *MirrorOptions) (err error) {
	readTimeout, err := time.ParseDuration(opts.readOpts.timeout)
	failOnError(err, "Failed to parse read timeout duration")

	writeTimeout, err := time.ParseDuration(opts.readOpts.timeout)
	failOnError(err, "Failed to parse write timeout duration")

	readMaxConnLifetime, err := time.ParseDuration(opts.readOpts.maxConnLifetime)
	failOnError(err, "Failed to parse the timeout duration")

	writeMaxConnLifetime, err := time.ParseDuration(opts.writeOpts.maxConnLifetime)
	failOnError(err, "Failed to parse the timeout duration")

	source, err := reader.Connect(reader.ConnOpts{
		DSN:             opts.from,
		Timeout:         readTimeout,
		MaxConnLifetime: readMaxConnLifetime,
		MaxConns:        opts.readOpts.maxConns,
		MaxIdleConns:    opts.readOpts.maxIdleConns,
	})
	failOnError(err, "Error connecting to reader")
	defer source.Close()

	target, err := dumper.NewDumper(dumper.ConnOpts{
		DSN:             opts.to,
		Timeout:         writeTimeout,
		MaxConnLifetime: writeMaxConnLifetime,
		MaxConns:        opts.writeOpts.maxConns,
		MaxIdleConns:    opts.writeOpts.maxIdleConns,
	}, source)
	failOnError(err, "Error creating dumper")
	defer target.Close()

	log.Info("Mirroring...")

	done := make(chan struct{})
	defer close(done)
	start := time.Now()
	failOnError(target.DumpViews(done, globalConfig, opts.srcDbPrefix, opts.dstDbPrefix), "Error while dumping")

	<- done
	log.WithField("total_time", time.Since(start)).Info("Done!")

	return nil
}