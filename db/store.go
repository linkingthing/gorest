package db

import (
	"fmt"
	"reflect"

	"github.com/linkingthing/gorest/resource"
)

type ResourceStore interface {
	Clean()
	Close()
	Begin() (Transaction, error)
	SetSchema(string)
	GetSchema() string
	DropSchemas(dropSchemas ...string) error
}

type Transaction interface {
	Insert(r resource.Resource) (resource.Resource, error)
	// Get return an slice of Resource which is a pointer to struct
	Get(typ ResourceType, cond map[string]interface{}) (interface{}, error)
	// GetOwned this is used for many to many relationship
	//which means there is a separate table which owner and owned resource in it
	//return an slice of Resource which is a pointer to struct
	GetOwned(owner ResourceType, ownerID string, owned ResourceType) (interface{}, error)
	Exists(typ ResourceType, cond map[string]interface{}) (bool, error)
	Count(typ ResourceType, cond map[string]interface{}) (int64, error)
	// Fill out should be an slice of Resource which is a pointer to struct
	Fill(cond map[string]interface{}, out interface{}) error
	Delete(typ ResourceType, cond map[string]interface{}) (int64, error)
	Update(typ ResourceType, nv map[string]interface{}, cond map[string]interface{}) (int64, error)
	// FillOwned Similar with GetOwned
	//out should be an slice of Resource which is a pointer to struct
	FillOwned(owner ResourceType, ownerID string, out interface{}) error

	GetEx(typ ResourceType, sql string, params ...interface{}) (interface{}, error)
	CountEx(typ ResourceType, sql string, params ...interface{}) (int64, error)
	FillEx(out interface{}, sql string, params ...interface{}) error
	Exec(sql string, params ...interface{}) (int64, error)
	// CopyFromEx The values should be in the same order as the columns
	CopyFromEx(typ ResourceType, columns []string, values [][]interface{}) (int64, error)
	// CopyFrom The values should be in the same order as the columns
	CopyFrom(typ ResourceType, values [][]interface{}) (int64, error)

	Commit() error
	Rollback() error
}

type Driver string

const (
	DriverPostgresql Driver = "postgresql"
	DriverOpenGauss  Driver = "openGauss"
	DriverMysql      Driver = "mysql"
)

func NewRStore(connStr string, meta *ResourceMeta, driver Driver, opts ...Option) (ResourceStore, error) {
	return NewPGStore(connStr, driver, meta, opts...)
}

func WithTx(store ResourceStore, f func(Transaction) error) error {
	tx, err := store.Begin()
	if err == nil {
		if err = f(tx); err == nil {
			tx.Commit()
		} else {
			tx.Rollback()
		}
	}
	return err
}

// GetResourceWithID out should be a slice of struct pointer
func GetResourceWithID(store ResourceStore, id string, out interface{}) (interface{}, error) {
	err := WithTx(store, func(tx Transaction) error {
		return tx.Fill(map[string]interface{}{IDField: id}, out)
	})
	if err != nil {
		return nil, err
	}

	sliceVal := reflect.ValueOf(out).Elem()
	if sliceVal.Len() == 1 {
		return sliceVal.Index(0).Interface(), nil
	} else {
		return nil, fmt.Errorf("not found")
	}
}
