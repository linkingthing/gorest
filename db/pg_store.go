package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Kseleven/pgx/v5"
	"github.com/Kseleven/pgx/v5/pgxpool"
	"github.com/linkingthing/cement/reflector"
	"github.com/linkingthing/cement/stringtool"
	"github.com/linkingthing/gorest/resource"
)

type PGStore struct {
	schema string
	pool   *pgxpool.Pool
	meta   *ResourceMeta
	driver Driver
}

func NewPGStore(connStr string, driver Driver, meta *ResourceMeta, opts ...Option) (ResourceStore, error) {
	pool, err := pgxpool.New(context.TODO(), connStr)
	if err != nil {
		return nil, err
	}
	r := &PGStore{meta: meta, pool: pool, driver: driver, schema: DefaultSchemaName}

	if isRecovery, err := IsDBRecoveryMode(pool); err != nil {
		pool.Close()
		return nil, err
	} else if isRecovery {
		return r, nil
	}

	for _, opt := range opts {
		opt(r)
	}

	if err := r.InitSchema(); err != nil {
		pool.Close()
		return nil, fmt.Errorf("init schema failed: %v", err)
	}

	for _, descriptor := range meta.GetDescriptors() {
		cTable, cIndexes := r.createTableSql(descriptor)
		if _, err := pool.Exec(context.TODO(), cTable); err != nil {
			pool.Close()
			return nil, fmt.Errorf("create table %s error: %v", cTable, err)
		}

		for _, index := range cIndexes {
			if _, err := pool.Exec(context.TODO(), index); err != nil {
				pool.Close()
				return nil, fmt.Errorf("create index failed:%s", err.Error())
			}
		}
	}

	return r, nil
}

func (store *PGStore) createTableSql(descriptor *ResourceDescriptor) (string, []string) {
	var buf bytes.Buffer
	buf.WriteString("create table if not exists ")
	buf.WriteString(getTableName(store.schema, descriptor.Typ))
	buf.WriteString(" (")
	tableName := getTableNameWithoutSchema(store.schema, descriptor.Typ)

	var indexes []string
	var ginIndexes []string
	for _, field := range descriptor.Fields {
		buf.WriteString(field.Name)
		buf.WriteString(" ")
		buf.WriteString(postgresqlTypeMap[field.Type])

		if field.NotNull {
			buf.WriteString(" ")
			buf.WriteString("not null")
		}

		if field.Unique {
			buf.WriteString(" ")
			buf.WriteString("unique")
		}

		if field.Index {
			if field.Type == StringArray || field.Type == IPSlice || field.Type == IPNetSlice ||
				field.Type == SmallIntArray || field.Type == BigIntArray || field.Type == SuperIntArray ||
				field.Type == Float32Array {
				ginIndexes = append(ginIndexes, field.Name)
			} else {
				indexes = append(indexes, field.Name)
			}
		}

		if field.Check == Positive {
			buf.WriteString(" check(")
			buf.WriteString(field.Name)
			buf.WriteString(" > 0)")
		}
		buf.WriteString(",")
	}

	for _, owner := range descriptor.Owners {
		buf.WriteString(string(owner))
		buf.WriteString(" text not null references ")
		buf.WriteString(getTableName(store.schema, owner))
		buf.WriteString(" (id) on delete cascade")
		buf.WriteString(",")
	}

	for _, refer := range descriptor.Refers {
		buf.WriteString(string(refer))
		buf.WriteString(" text not null references ")
		buf.WriteString(getTableName(store.schema, refer))
		buf.WriteString(" (id) on delete restrict")
		buf.WriteString(",")
	}

	if len(descriptor.Pks) > 0 {
		buf.WriteString("primary key (")
		for i, pk := range descriptor.Pks {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(string(pk))
		}
		buf.WriteString("),")
	}

	if len(descriptor.Uks) > 0 {
		buf.WriteString("unique (")
		for i, uk := range descriptor.Uks {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(string(uk))
		}
		buf.WriteString("),")
	}

	var idxBuf bytes.Buffer
	var createIndexes []string
	if len(descriptor.Idxes) > 0 {
		idxBuf.WriteString("create index ")
		idxBuf.WriteString(" if not exists ")
		idxBuf.WriteString(IndexPrefix + tableName + "_" + strings.Join(descriptor.Idxes, "_"))
		idxBuf.WriteString(" on ")
		idxBuf.WriteString(getTableName(store.schema, descriptor.Typ))
		idxBuf.WriteString(" (")
		for i, idx := range descriptor.Idxes {
			idxBuf.WriteString(idx)
			if i < len(descriptor.Idxes)-1 {
				idxBuf.WriteString(",")
			}
		}
		idxBuf.WriteString(")")
		createIndexes = append(createIndexes, idxBuf.String())
		idxBuf.Reset()
	}

	if len(indexes) > 0 {
		for _, index := range indexes {
			idxBuf.WriteString("create index ")
			idxBuf.WriteString(" if not exists ")
			idxBuf.WriteString(IndexPrefix + tableName + "_" + index)
			idxBuf.WriteString(" on ")
			idxBuf.WriteString(getTableName(store.schema, descriptor.Typ))
			idxBuf.WriteString(" (")
			idxBuf.WriteString(index)
			idxBuf.WriteString(")")

			createIndexes = append(createIndexes, idxBuf.String())
			idxBuf.Reset()
		}
	}

	if len(ginIndexes) > 0 {
		for _, index := range ginIndexes {
			idxBuf.WriteString("create index ")
			idxBuf.WriteString(" if not exists ")
			idxBuf.WriteString(IndexPrefix + tableName + "_" + index)
			idxBuf.WriteString(" on ")
			idxBuf.WriteString(getTableName(store.schema, descriptor.Typ))
			if store.driver != DriverOpenGauss { //GaussDB not support gin index
				idxBuf.WriteString(" using gin")
			}
			idxBuf.WriteString(" (")
			idxBuf.WriteString(index)
			idxBuf.WriteString(")")

			createIndexes = append(createIndexes, idxBuf.String())
			idxBuf.Reset()
		}
	}

	return strings.TrimRight(buf.String(), ",") + ")", createIndexes
}

func (store *PGStore) Close() {
	store.pool.Close()
}

func (store *PGStore) Clean() {
	rs := store.meta.Resources()
	for i := len(rs); i > 0; i-- {
		tableName := getTableName(store.schema, rs[i-1])
		store.pool.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableName+" CASCADE")
	}
}

func (store *PGStore) Begin() (Transaction, error) {
	tx, err := store.pool.Begin(context.TODO())
	if err != nil {
		return nil, err
	} else {
		return PGStoreTx{tx, NewBaseTx(store.meta, store.schema)}, nil
	}
}

func (store *PGStore) SetSchema(s string) {
	store.schema = s
}

func (store *PGStore) GetSchema() string {
	return store.schema
}

func (store *PGStore) InitSchema() error {
	if store.driver == DriverOpenGauss {
		row := store.pool.QueryRow(context.Background(),
			"SELECT schema_name FROM information_schema.schemata where schema_name=$1;", store.GetSchema())
		var SchemaName string
		if err := row.Scan(&SchemaName); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				_, err := store.pool.Exec(context.Background(), fmt.Sprintf("create schema %s", store.GetSchema()))
				return err
			}
			return err
		}
		return nil
	}

	_, err := store.pool.Exec(context.TODO(), "create schema if not exists "+store.GetSchema())
	return err
}

func (store *PGStore) DropSchemas(dropSchemas ...string) error {
	for _, schemaName := range dropSchemas {
		if _, err := store.pool.Exec(context.TODO(), fmt.Sprintf(dropSchemaSql, schemaName)); err != nil {
			return err
		}
	}
	return nil
}

type PGStoreTx struct {
	pgx.Tx
	*BaseTx
}

func (tx PGStoreTx) Commit() error {
	return tx.Tx.Commit(context.TODO())
}

func (tx PGStoreTx) Rollback() error {
	return tx.Tx.Rollback(context.TODO())
}

func (tx PGStoreTx) Insert(r resource.Resource) (resource.Resource, error) {
	r.SetCreationTimestamp(time.Now())
	sql, args, err := tx.insertSqlArgsAndID(r)
	if err != nil {
		return nil, err
	}

	logSql(sql, args)
	_, err = tx.Tx.Exec(context.TODO(), sql, args...)
	if err != nil {
		return nil, err
	} else {
		return r, err
	}
}

func (tx PGStoreTx) GetOwned(owner ResourceType, ownerID string, owned ResourceType) (interface{}, error) {
	goTyp, err := tx.meta.GetGoType(owned)
	if err != nil {
		return nil, err
	}
	sp := reflector.NewSlicePointer(reflect.PointerTo(goTyp))
	sql, args, err := tx.joinSelectSqlAndArgs(owner, owned, ownerID)
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

func (tx PGStoreTx) FillOwned(owner ResourceType, ownerID string, out interface{}) error {
	r, err := reflector.GetStructPointerInSlice(out)
	if err != nil {
		return err
	}

	sql, args, err := tx.joinSelectSqlAndArgs(owner, ResourceDBType(r.(resource.Resource)), ownerID)
	if err != nil {
		return err
	}

	return tx.getWithSql(sql, args, out)
}

func (tx PGStoreTx) Get(typ ResourceType, cond map[string]interface{}) (interface{}, error) {
	goTyp, err := tx.meta.GetGoType(typ)
	if err != nil {
		return nil, err
	}
	sp := reflector.NewSlicePointer(reflect.PointerTo(goTyp))
	err = tx.Fill(cond, sp)
	if err != nil {
		return nil, err
	} else {
		return reflect.ValueOf(sp).Elem().Interface(), nil
	}
}

func (tx PGStoreTx) Fill(conds map[string]interface{}, out interface{}) error {
	r, err := reflector.GetStructPointerInSlice(out)
	if err != nil {
		return err
	}

	sql, args, err := tx.selectSqlAndArgs(ResourceDBType(r.(resource.Resource)), conds)
	if err != nil {
		return err
	}

	return tx.getWithSql(sql, args, out)
}

func (tx PGStoreTx) Exists(typ ResourceType, conds map[string]interface{}) (bool, error) {
	sql, params, err := tx.existsSqlAndArgs(typ, conds)
	if err != nil {
		return false, err
	}

	return tx.existsWithSql(sql, params...)
}

func (tx PGStoreTx) existsWithSql(sql string, params ...interface{}) (bool, error) {
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

func (tx PGStoreTx) Count(typ ResourceType, conds map[string]interface{}) (int64, error) {
	sql, params, err := tx.countSqlAndArgs(typ, conds)
	if err != nil {
		return 0, err
	}

	return tx.countWithSql(sql, params...)
}

func (tx PGStoreTx) CountEx(typ ResourceType, sql string, params ...interface{}) (int64, error) {
	if tx.meta.Has(typ) == false {
		return 0, fmt.Errorf("unknown resource type %v", typ)
	}
	return tx.countWithSql(sql, params...)
}

func (tx PGStoreTx) countWithSql(sql string, params ...interface{}) (int64, error) {
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

func (tx PGStoreTx) Update(typ ResourceType, nv map[string]interface{}, conds map[string]interface{}) (int64, error) {
	sql, args, err := tx.updateSqlAndArgs(typ, nv, conds)
	if err != nil {
		return 0, err
	}

	return tx.Exec(sql, args...)
}

func (tx PGStoreTx) Delete(typ ResourceType, cond map[string]interface{}) (int64, error) {
	sql, args, err := tx.deleteSqlAndArgs(typ, cond)
	if err != nil {
		return 0, err
	}

	return tx.Exec(sql, args...)
}

func (tx PGStoreTx) GetEx(typ ResourceType, sql string, params ...interface{}) (interface{}, error) {
	rt, err := tx.meta.GetGoType(typ)
	if err != nil {
		return nil, err
	}
	sp := reflector.NewSlicePointer(reflect.PointerTo(rt))
	err = tx.FillEx(sp, sql, params...)
	if err != nil {
		return nil, err
	} else {
		return reflect.ValueOf(sp).Elem().Interface(), nil
	}
}

func (tx PGStoreTx) FillEx(out interface{}, sql string, params ...interface{}) error {
	return tx.getWithSql(sql, params, out)
}

func (tx PGStoreTx) Exec(sql string, params ...interface{}) (int64, error) {
	logSql(sql, params...)
	result, err := tx.Tx.Exec(context.TODO(), sql, params...)
	if err != nil {
		return 0, err
	} else {
		return result.RowsAffected(), nil
	}
}

func (tx PGStoreTx) CopyFromEx(typ ResourceType, columns []string, values [][]interface{}) (int64, error) {
	descriptor, err := tx.meta.GetDescriptor(typ)
	if err != nil {
		return 0, fmt.Errorf("get descriptor for %v failed %v", typ, err.Error())
	}
	if len(values) == 0 {
		return 0, nil
	}

	c, err := tx.Tx.CopyFrom(context.Background(),
		pgx.Identifier{tx.schema, getTableNameWithoutSchema(tx.schema, descriptor.Typ)},
		columns,
		pgx.CopyFromRows(values))
	return c, err
}

func (tx PGStoreTx) CopyFrom(typ ResourceType, values [][]interface{}) (int64, error) {
	descriptor, err := tx.meta.GetDescriptor(typ)
	if err != nil {
		return 0, fmt.Errorf("get descriptor for %v failed %v", typ, err.Error())
	}

	columns := make([]string, 0, len(descriptor.Fields))
	for _, field := range descriptor.Fields {
		columns = append(columns, field.Name)
	}
	if len(values) == 0 || len(columns) == 0 {
		return 0, nil
	}

	c, err := tx.Tx.CopyFrom(context.Background(),
		pgx.Identifier{tx.schema, getTableNameWithoutSchema(tx.schema, descriptor.Typ)},
		columns,
		pgx.CopyFromRows(values))
	return c, err
}

func (tx PGStoreTx) getWithSql(sql string, args []interface{}, out interface{}) error {
	logSql(sql, args)
	rows, err := tx.Tx.Query(context.TODO(), sql, args...)
	if err != nil {
		return err
	}

	return tx.rowsToResources(rows, out)
}

func (tx PGStoreTx) rowsToResources(rows pgx.Rows, out interface{}) error {
	goTyp := reflect.TypeOf(out)
	if goTyp.Kind() != reflect.Ptr || goTyp.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("output isn't a pointer to slice")
	}

	slice := reflect.Indirect(reflect.ValueOf(out))
	if slice.Type().Elem().Kind() != reflect.Ptr {
		return fmt.Errorf("output isn't a pointer to slice of pointer")
	}
	typ := slice.Type().Elem().Elem()

	for rows.Next() {
		elem := reflect.New(typ)
		fd := rows.FieldDescriptions()
		fields := make([]interface{}, 0, len(fd))
		var id string
		var createTime time.Time
		for _, d := range fd {
			if string(d.Name) == IDField {
				fields = append(fields, &id)
			} else if string(d.Name) == CreateTimeField {
				fields = append(fields, &createTime)
			} else {
				fieldName := stringtool.ToUpperCamel(d.Name)
				fields = append(fields, elem.Elem().FieldByName(fieldName).Addr().Interface())
			}
		}
		err := rows.Scan(fields...)
		if err != nil {
			return err
		}
		r, ok := elem.Interface().(resource.Resource)
		if !ok {
			return fmt.Errorf("output isn't a pointer to slice of resource")
		}
		r.SetID(id)
		r.SetCreationTimestamp(createTime)
		slice.Set(reflect.Append(slice, elem))
	}
	return nil
}
