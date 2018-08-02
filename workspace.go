package main

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/skeema/mybase"
	"github.com/skeema/tengo"
)

type Workspace interface {
	Instance() *tengo.Instance
	SchemaName() string
	Cleanup() error
}

type TempSchema struct {
	config *mybase.Config
	inst   *tengo.Instance
	lockTx *sql.Tx
}

func NewTempSchema(config *mybase.Config, instance *tengo.Instance) (Workspace, error) {
	ts := &TempSchema{
		config: config,
		inst:   instance,
	}
	tempSchemaName := ts.SchemaName()

	lockName := fmt.Sprintf("skeema.%s", tempSchemaName)
	var err error
	if ts.lockTx, err = getLock(instance, lockName, 30*time.Second); err != nil {
		return nil, fmt.Errorf("Unable to lock temporary schema on %s: %s", instance, err)
	}

	if has, err := instance.HasSchema(tempSchemaName); err != nil {
		return nil, fmt.Errorf("Unable to check for existence of temp schema on %s: %s", instance, err)
	} else if has {
		// Attempt to drop any tables already present in tempSchema, but fail if
		// any of them actually have 1 or more rows
		if err := instance.DropTablesInSchema(tempSchemaName, true); err != nil {
			return nil, fmt.Errorf("Cannot drop existing temp schema tables on %s: %s", instance, err)
		}
	} else {
		_, err = instance.CreateSchema(tempSchemaName, config.Get("default-character-set"), config.Get("default-collation"))
		if err != nil {
			return nil, fmt.Errorf("Cannot create temporary schema on %s: %s", instance, err)
		}
	}
	return ts, nil
}

func (ts *TempSchema) Instance() *tengo.Instance {
	return ts.inst
}

func (ts *TempSchema) SchemaName() string {
	return ts.config.Get("temp-schema")
}

func (ts *TempSchema) Cleanup() error {
	tempSchemaName := ts.SchemaName()
	if ts.config.GetBool("reuse-temp-schema") {
		if err := ts.inst.DropTablesInSchema(tempSchemaName, true); err != nil {
			return fmt.Errorf("Cannot drop tables in temporary schema on %s: %s", ts.inst, err)
		}
	} else {
		if err := ts.inst.DropSchema(tempSchemaName, true); err != nil {
			return fmt.Errorf("Cannot drop temporary schema on %s: %s", ts.inst, err)
		}
	}

	lockName := fmt.Sprintf("skeema.%s", tempSchemaName)
	err := releaseLock(ts.lockTx, lockName)
	ts.lockTx = nil
	return err
}

func getLock(instance *tengo.Instance, lockName string, maxWait time.Duration) (*sql.Tx, error) {
	db, err := instance.Connect("", "")
	if err != nil {
		return nil, err
	}
	lockTx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	var getLockResult int

	start := time.Now()
	for time.Since(start) < maxWait {
		// Only using a timeout of 1 sec on each query to avoid potential issues with
		// query killers, spurious slow query logging, etc
		err := lockTx.QueryRow("SELECT GET_LOCK(?, 1)", lockName).Scan(&getLockResult)
		if err == nil && getLockResult == 1 {
			return lockTx, nil
		}
	}
	return nil, errors.New("Unable to acquire lock")

}

func releaseLock(lockTx *sql.Tx, lockName string) error {
	var releaseLockResult int
	err := lockTx.QueryRow("SELECT RELEASE_LOCK(?)", lockName).Scan(&releaseLockResult)
	if err != nil || releaseLockResult != 1 {
		return errors.New("Failed to release lock, or connection holding lock already dropped")
	}
	return lockTx.Rollback()
}
