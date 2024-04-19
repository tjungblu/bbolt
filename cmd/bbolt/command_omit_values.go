package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"go.etcd.io/bbolt/internal/common"
	"log"

	bolt "go.etcd.io/bbolt"
)

func newOmitValuesCobraCommand() *cobra.Command {
	omitValuesCmd := &cobra.Command{
		Use:   "omitval",
		Short: "removes all values in the given database file and stores it in the destination",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return omitFunc(args[0], args[1])
		},
	}

	return omitValuesCmd
}

func omitFunc(srcDBPath, destination string) error {
	if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}

	if err := common.CopyFile(srcDBPath, destination); err != nil {
		return fmt.Errorf("copy file failed: %w", err)
	}

	logger := bolt.DefaultLogger{Logger: log.Default()}
	logger.EnableDebug()
	db, err := bolt.Open(destination, 0600, &bolt.Options{Logger: &logger})
	if err != nil {
		return err
	}
	defer db.Close()

	// replace all values with an empty (1 filled) byte slice of same size, we have to do it twice,
	// since each transaction keeps a copy of the last pages around for recovery
	for i := 0; i < 2; i++ {

		err = db.Update(func(tx *bolt.Tx) error {
			for _, bucket := range []string{"key", "lease"} {
				b := tx.Bucket([]byte(bucket))
				err = b.ForEach(func(k, v []byte) error {
					ones := make([]byte, len(v))
					for j := 0; j < len(v); j++ {
						ones[j] = 1
					}
					return b.Put(k, ones)
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return db.Sync()
}
