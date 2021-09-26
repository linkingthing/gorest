package db

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/linkingthing/gorest/resource"
	"github.com/zdnscloud/cement/reflector"
)

type RStore struct {
	pool *pgxpool.Pool
	meta *ResourceMeta
}

type RStoreTx struct {
	pgx.Tx
	meta *ResourceMeta
}

func NewRStore(connStr string, meta *ResourceMeta) (ResourceStore, error) {
	pool, err := pgxpool.Connect(context.TODO(), connStr)
	if err != nil {
		return nil, err
	}

	for _, descriptor := range meta.GetDescriptors() {
		_, err := pool.Exec(context.TODO(), createTableSql(descriptor))
		if err != nil {
			pool.Close()
			return nil, err
		}
	}

	return &RStore{pool, meta}, nil
}

func (store *RStore) Close() {
	store.pool.Close()
}

func (store *RStore) Clean() {
	rs := store.meta.Resources()
	for i := len(rs); i > 0; i-- {
		tableName := resourceTableName(rs[i-1])
		store.pool.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableName+" CASCADE")
	}
}

func (store *RStore) Begin() (Transaction, error) {
	tx, err := store.pool.Begin(context.TODO())
	if err != nil {
		return nil, err
	} else {
		return RStoreTx{tx, store.meta}, nil
	}
}

func (tx RStoreTx) Insert(r resource.Resource) (resource.Resource, error) {
	r.SetCreationTimestamp(time.Now())
	sql, args, err := insertSqlArgsAndID(tx.meta, r)
	if err != nil {
		return nil, err
	}

	_, err = tx.Tx.Exec(context.TODO(), sql, args...)
	if err != nil {
		return nil, err
	} else {
		return r, err
	}
}

func (tx RStoreTx) GetOwned(owner ResourceType, ownerID string, owned ResourceType) (interface{}, error) {
	goTyp, err := tx.meta.GetGoType(owned)
	if err != nil {
		return nil, err
	}
	sp := reflector.NewSlicePointer(reflect.PtrTo(goTyp))
	sql, args, err := joinSelectSqlAndArgs(tx.meta, owner, owned, ownerID)
	if err != nil {
		return nil, err
	}

	err = tx.getWithSql(sql, args, sp)
	if err != nil {
		return nil, err
	} else {
		return reflect.ValueOf(sp).Elem().Interface(), nil
	}
}

func (tx RStoreTx) FillOwned(owner ResourceType, ownerID string, out interface{}) error {
	r, err := reflector.GetStructPointerInSlice(out)
	if err != nil {
		return err
	}

	sql, args, err := joinSelectSqlAndArgs(tx.meta, owner, ResourceDBType(r.(resource.Resource)), ownerID)
	if err != nil {
		return err
	}

	return tx.getWithSql(sql, args, out)
}

func (tx RStoreTx) Get(typ ResourceType, conds map[string]interface{}) (interface{}, error) {
	goTyp, err := tx.meta.GetGoType(typ)
	if err != nil {
		return nil, err
	}
	sp := reflector.NewSlicePointer(reflect.PtrTo(goTyp))
	err = tx.Fill(conds, sp)
	if err != nil {
		return nil, err
	} else {
		return reflect.ValueOf(sp).Elem().Interface(), nil
	}
}

func (tx RStoreTx) Fill(conds map[string]interface{}, out interface{}) error {
	r, err := reflector.GetStructPointerInSlice(out)
	if err != nil {
		return err
	}

	sql, args, err := selectSqlAndArgs(tx.meta, ResourceDBType(r.(resource.Resource)), conds)
	if err != nil {
		return err
	}
	return tx.getWithSql(sql, args, out)
}

func (tx RStoreTx) getWithSql(sql string, args []interface{}, out interface{}) error {
	rows, err := tx.Tx.Query(context.TODO(), sql, args...)
	if err != nil {
		return err
	}

	return rowsToResources(rows, out)
}

func (tx RStoreTx) Exists(typ ResourceType, conds map[string]interface{}) (bool, error) {
	sql, params, err := existsSqlAndArgs(tx.meta, typ, conds)
	if err != nil {
		return false, err
	}

	return tx.existsWithSql(sql, params...)
}

func (tx RStoreTx) existsWithSql(sql string, params ...interface{}) (bool, error) {
	rows, err := tx.Tx.Query(context.TODO(), sql, params...)
	if err != nil {
		return false, err
	}

	var exist bool
	//there should only one row
	for rows.Next() {
		if err := rows.Scan(&exist); err != nil {
			return false, err
		}
	}
	return exist, nil
}

func (tx RStoreTx) Count(typ ResourceType, conds map[string]interface{}) (int64, error) {
	sql, params, err := countSqlAndArgs(tx.meta, typ, conds)
	if err != nil {
		return 0, err
	}

	return tx.countWithSql(sql, params...)
}

func (tx RStoreTx) CountEx(typ ResourceType, sql string, params ...interface{}) (int64, error) {
	if tx.meta.Has(typ) == false {
		return 0, fmt.Errorf("unknown resource type %v", typ)
	}
	return tx.countWithSql(sql, params...)
}

func (tx RStoreTx) countWithSql(sql string, params ...interface{}) (int64, error) {
	rows, err := tx.Tx.Query(context.TODO(), sql, params...)
	if err != nil {
		return 0, err
	}

	var count int64
	//there should only one row
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, err
		}
	}

	return count, nil
}

func (tx RStoreTx) Update(typ ResourceType, nv map[string]interface{}, conds map[string]interface{}) (int64, error) {
	sql, args, err := updateSqlAndArgs(tx.meta, typ, nv, conds)
	if err != nil {
		return 0, err
	}

	return tx.Exec(sql, args...)
}

func (tx RStoreTx) Delete(typ ResourceType, conds map[string]interface{}) (int64, error) {
	sql, args, err := deleteSqlAndArgs(tx.meta, typ, conds)
	if err != nil {
		return 0, err
	}

	return tx.Exec(sql, args...)
}

func (tx RStoreTx) GetEx(typ ResourceType, sql string, params ...interface{}) (interface{}, error) {
	rt, err := tx.meta.GetGoType(typ)
	if err != nil {
		return nil, err
	}
	sp := reflector.NewSlicePointer(reflect.PtrTo(rt))
	err = tx.FillEx(sp, sql, params...)
	if err != nil {
		return nil, err
	} else {
		return reflect.ValueOf(sp).Elem().Interface(), nil
	}
}

func (tx RStoreTx) FillEx(out interface{}, sql string, params ...interface{}) error {
	return tx.getWithSql(sql, params, out)
}

func (tx RStoreTx) Exec(sql string, params ...interface{}) (int64, error) {
	result, err := tx.Tx.Exec(context.TODO(), sql, params...)
	if err != nil {
		return 0, err
	} else {
		return result.RowsAffected(), nil
	}
}

func (tx RStoreTx) Commit() error {
	return tx.Tx.Commit(context.TODO())
}

func (tx RStoreTx) Rollback() error {
	return tx.Tx.Rollback(context.TODO())
}

func (tx RStoreTx) BatchInsert(value interface{}) (int64, error) {
	reflectValue := reflect.Indirect(reflect.ValueOf(value))
	switch reflectValue.Kind() {
	case reflect.Array, reflect.Slice:
		reflectLen := reflectValue.Len()
		if reflectLen == 0 {
			return 0, nil
		} else if reflectLen > maxBatchSize {
			return 0, fmt.Errorf("the limit size of batch insert is %d", maxBatchSize)
		}

		return tx.prepareBatchInsert(reflectValue, reflectLen, value, batchSize)
	default:
		return 0, fmt.Errorf("only support pointer of slice")
	}
}

func (tx RStoreTx) prepareBatchInsert(reflectValue reflect.Value, reflectLen int, value interface{}, batchSize int) (int64, error) {
	r, err := reflector.GetStructPointerInSlice(value)
	if err != nil {
		return 0, err
	}
	descriptor, tableName, err := tx.getDescriptor(r.(resource.Resource))
	if err != nil {
		return 0, err
	}
	var effectRows int64
	fieldCount := len(descriptor.Fields) + len(descriptor.Owners) + len(descriptor.Refers)
	if fieldCount*reflectLen > batchSize {
		batchSize = batchSize / fieldCount
	}
	for i := 0; i < reflectLen; i += batchSize {
		ends := i + batchSize
		if ends > reflectLen {
			ends = reflectLen
		}
		if c, err := tx.execBatchInsert(descriptor, tableName, fieldCount, reflectValue.Slice(i, ends)); err != nil {
			return 0, err
		} else {
			effectRows += c
		}
	}
	return effectRows, nil
}

func (tx RStoreTx) execBatchInsert(descriptor *ResourceDescriptor, tableName string, fieldCount int, value reflect.Value) (int64, error) {
	var buf bytes.Buffer
	buf.WriteString("insert into ")
	buf.WriteString(tableName)
	buf.WriteString(" values ")
	args := make([]interface{}, 0, fieldCount)
	offset := 1
	valueLen := value.Len()
	for i := 0; i < valueLen; i++ {
		if sql, argOne, err := genBatchInsertSql(descriptor, value.Index(i).Interface(), fieldCount, offset); err != nil {
			return 0, err
		} else {
			buf.WriteString(sql)
			if i < valueLen-1 {
				buf.WriteString(",")
			}
			args = append(args, argOne...)
		}
		offset += fieldCount
	}

	if result, err := tx.Tx.Exec(context.TODO(), buf.String(), args...); err != nil {
		return 0, err
	} else {
		return result.RowsAffected(), err
	}
}

func (tx RStoreTx) getDescriptor(r resource.Resource) (*ResourceDescriptor, string, error) {
	typ := ResourceDBType(r)
	descriptor, err := tx.meta.GetDescriptor(typ)
	if err != nil {
		return nil, "", fmt.Errorf("get %v descriptor failed %v", typ, err.Error())
	}
	return descriptor, resourceTableName(typ), nil
}
