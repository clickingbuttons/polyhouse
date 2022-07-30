package main

import (
	"github.com/clickingbuttons/polyhouse/cmd/ingest"
	"github.com/clickingbuttons/polyhouse/cmd/schema"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

type RootCmd struct {
	logger *logrus.Entry
	viper  *viper.Viper
}

func main() {
	cmd := RootCmd{
		logger: logrus.NewEntry(logrus.StandardLogger()),
		viper:  viper.New(),
	}

	schemaCmd, err := schema.NewSchema(cmd.logger)
	if err != nil {
		logrus.WithError(err).Fatal("error during init for schema command")
	}

	ingestCmd, err := ingest.NewIngest(cmd.logger)
	if err != nil {
		logrus.WithError(err).Fatal("error during init for schema command")
	}

	rootCmd := &cobra.Command{
		Use:               "polyhouse",
		Short:             "polyhouse creates schema for Polygon data and optionally ingests from public APIs",
		PersistentPreRunE: cmd.persistentPreRun,
		RunE:              cmd.runE,
	}
	rootCmd.PersistentFlags().String("templates", "./templates/*", "glob for schema templates")
	rootCmd.PersistentFlags().String("database", "us_equities", "name of database")
	rootCmd.PersistentFlags().String("address", "127.0.0.1:9000", "clickhouse address")
	rootCmd.PersistentFlags().String("username", "default", "for clickhouse auth")
	rootCmd.PersistentFlags().String("password", "", "for clickhouse auth")
	rootCmd.PersistentFlags().Int("max-open-conns", 90, "clickhouse max open connections")
	rootCmd.PersistentFlags().Int("max-idle-conns", 5, "clickhouse max open connections")
	rootCmd.PersistentFlags().Bool("verbose", false, "log moar")
	rootCmd.PersistentFlags().String("cluster", "", "clickhouse cluster to make tables on")
	rootCmd.AddCommand(schemaCmd)
	rootCmd.AddCommand(ingestCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func (e *RootCmd) runE(cmd *cobra.Command, args []string) error {
	e.logger.Info("TODO: schemas + ingest")
	return nil
}

func (e *RootCmd) bindViperFlagsPreRun(cmd *cobra.Command, _ []string) error {
	if err := e.viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		return err
	}

	if err := e.viper.BindPFlags(cmd.Flags()); err != nil {
		return err
	}

	return nil
}

func (e *RootCmd) persistentPreRun(cmd *cobra.Command, args []string) error {
	if err := e.bindViperFlagsPreRun(cmd, args); err != nil {
		return err
	}

	if e.viper.GetBool("verbose") {
		logrus.SetLevel(logrus.TraceLevel)
	}

	logrus.SetOutput(cmd.ErrOrStderr())
	return nil
}
