package main

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/common"
)

type surgeryBucketOptions struct {
	surgeryBaseOptions
	bucketName string
}

func (o *surgeryBucketOptions) AddFlags(fs *pflag.FlagSet) {
	o.surgeryBaseOptions.AddFlags(fs)
	fs.StringVarP(&o.bucketName, "bucket", "b", o.bucketName, "the bucket name")
}

func (o *surgeryBucketOptions) Validate() error {
	// TODO
	return nil
}

func newSurgeryBucketsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "buckets <subcommand>",
		Short: "buckets related surgery commands",
	}

	cmd.AddCommand(newSurgeryBucketsClearCommand())

	return cmd
}

func newSurgeryBucketsClearCommand() *cobra.Command {
	var o surgeryBucketOptions
	clearCmd := &cobra.Command{
		Use:   "clear <bbolt-file> [options]",
		Short: "clears a given bucket",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errors.New("db file path not provided")
			}
			if len(args) > 1 {
				return errors.New("too many arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return surgeryBucketClearFunc(args[0], o)
		},
	}

	o.AddFlags(clearCmd.Flags())

	return clearCmd
}

func surgeryBucketClearFunc(srcDBPath string, cfg surgeryBucketOptions) error {
	if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}

	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[bucket clear] copy file failed: %w", err)
	}

	db, err := bolt.Open(cfg.outputDBFilePath, 0600, &bolt.Options{})
	if err != nil {
		return err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(cfg.bucketName))
		if err != nil {
			return fmt.Errorf("could not delete bucket %s: %w", cfg.bucketName, err)
		}
		_, err = tx.CreateBucket([]byte(cfg.bucketName))
		if err != nil {
			return fmt.Errorf("could not re-create bucket %s: %w", cfg.bucketName, err)
		}
		return err
	})

	if err != nil {
		return fmt.Errorf("could not clear bucket %s: %w", cfg.bucketName, err)
	}

	if err := db.Sync(); err != nil {
		return fmt.Errorf("could not sync clear bucket %s: %w", cfg.bucketName, err)
	}

	return db.Close()
}
