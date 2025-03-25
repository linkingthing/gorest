package db

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "gitee.com/opengauss/openGauss-connector-go-pq"
	pq "gitee.com/opengauss/openGauss-connector-go-pq"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/linkingthing/cement/reflector"
	"github.com/linkingthing/cement/stringtool"
	"github.com/linkingthing/cement/uuid"

	"github.com/linkingthing/gorest/resource"
)

type GaussStore struct {
	conn   *sql.DB
	schema string
	meta   *ResourceMeta
}

var showLog bool

func SetDebug(t bool) {
	showLog = t
}

func logSql(query string, args ...any) {
	if showLog {
		fmt.Printf("*** exec sql: %s args:%s\n", query, args)
	}
}

func NewGaussStore(connStr string, meta *ResourceMeta, opts ...Option) (ResourceStore, error) {
	db, err := sql.Open("opengauss", parseGaussConnDsn(connStr))
	if err != nil {
		return nil, err
	}
	g := &GaussStore{meta: meta, conn: db, schema: DefaultSchemaName}

	if isRecovery, err := g.DBIsRecoveryMode(); err != nil {
		g.Close()
		return nil, err
	} else if isRecovery {
		return g, nil
	}

	for _, opt := range opts {
		opt(g)
	}

	if err := g.InitSchema(); err != nil {
		g.Close()
		return nil, err
	}

	for _, descriptor := range meta.GetDescriptors() {
		cTable, cIndexes := g.createTableSql(descriptor)
		if _, err := g.conn.Exec(cTable); err != nil {
			g.Close()
			return nil, err
		}

		for _, index := range cIndexes {
			_, err := g.conn.Exec(index)
			if err != nil {
				g.conn.Close()
				return nil, err
			}
		}
	}

	return g, nil
}

func parseGaussConnDsn(connStr string) string {
	pairs := strings.Split(connStr, " ")
	var out []string
	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 && kv[0] != "pool_max_conns" { //GaussDB not support pool_max_conn
			out = append(out, kv[0]+"="+kv[1])
		}
	}
	return strings.Join(out, " ")
}

func (g *GaussStore) DBIsRecoveryMode() (bool, error) {
	rows, err := g.conn.Query("SELECT pg_is_in_recovery()")
	if err != nil {
		return false, err
	}

	var rs []*recovery
	for rows.Next() {
		var r recovery
		if err := rows.Scan(&r.PgIsInRecovery); err != nil {
			return false, err
		} else {
			rs = append(rs, &r)
		}
	}

	return len(rs) == 1 && rs[0].PgIsInRecovery, nil
}

func (g *GaussStore) InitSchema() error {
	if c, err := g.conn.Query(
		"SELECT schema_name FROM information_schema.schemata where schema_name=$1;", g.GetSchema()); err != nil {
		return err
	} else if rows, _ := c.Columns(); len(rows) == 0 {
		_, err := g.conn.Exec(fmt.Sprintf("create schema %s", g.GetSchema()))
		return err
	}

	return nil
}

func (g *GaussStore) SetSchema(s string) {
	g.schema = s
}

func (g *GaussStore) GetSchema() string {
	return g.schema
}

func (g *GaussStore) DropSchemas(dropSchemas ...string) error {
	for _, schemaName := range dropSchemas {
		if _, err := g.conn.ExecContext(context.TODO(), fmt.Sprintf(dropSchemaSql, schemaName)); err != nil {
			return err
		}
	}
	return nil
}

func (g *GaussStore) createTableSql(descriptor *ResourceDescriptor) (string, []string) {
	var buf bytes.Buffer
	buf.WriteString("create table if not exists ")
	buf.WriteString(getTableName(g.schema, descriptor.Typ))
	buf.WriteString(" (")
	tableName := getTableNameWithoutSchema(descriptor.Typ)

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
		buf.WriteString(getTableName(g.schema, owner))
		buf.WriteString(" (id) on delete cascade")
		buf.WriteString(",")
	}

	for _, refer := range descriptor.Refers {
		buf.WriteString(string(refer))
		buf.WriteString(" text not null references ")
		buf.WriteString(getTableName(g.schema, refer))
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
		idxBuf.WriteString(tableName)
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
			idxBuf.WriteString(tableName)
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
			idxBuf.WriteString(tableName)
			//GaussDB not support gin
			//idxBuf.WriteString(" using gin")
			idxBuf.WriteString(" (")
			idxBuf.WriteString(index)
			idxBuf.WriteString(")")

			createIndexes = append(createIndexes, idxBuf.String())
			idxBuf.Reset()
		}
	}

	return strings.TrimRight(buf.String(), ",") + ")", createIndexes
}

func (g *GaussStore) Clean() {
	rs := g.meta.Resources()
	for i := len(rs); i > 0; i-- {
		tableName := getTableName(g.schema, rs[i-1])
		if _, err := g.conn.Exec("DROP TABLE IF EXISTS " + tableName + " CASCADE"); err != nil {
			fmt.Printf("failed to drop table %s, err: %v\n", tableName, err)
		}
	}
}

func (g *GaussStore) Close() {
	g.conn.Close()
}

func (g *GaussStore) Begin() (Transaction, error) {
	tx, err := g.conn.Begin()
	if err != nil {
		return nil, err
	}
	return GaussStoreTx{Tx: tx, BaseTx: NewBaseTx(g.meta, g.schema)}, nil
}

type GaussStoreTx struct {
	*sql.Tx
	*BaseTx
}

func (tx GaussStoreTx) Commit() error {
	return tx.Tx.Commit()
}

func (tx GaussStoreTx) Rollback() error {
	return tx.Tx.Rollback()
}

func (tx GaussStoreTx) Insert(r resource.Resource) (resource.Resource, error) {
	r.SetCreationTimestamp(time.Now())
	query, args, err := tx.insertSqlArgsAndID(tx.meta, r)
	if err != nil {
		return nil, err
	}
	logSql(query, args...)

	if _, err = tx.Tx.ExecContext(context.TODO(), query, args...); err != nil {
		return nil, err
	}
	return r, err
}

func (tx GaussStoreTx) Get(typ ResourceType, cond map[string]interface{}) (interface{}, error) {
	goTyp, err := tx.meta.GetGoType(typ)
	if err != nil {
		return nil, err
	}

	sp := reflector.NewSlicePointer(reflect.PointerTo(goTyp))
	if err = tx.Fill(cond, sp); err != nil {
		return nil, err
	}
	return reflect.ValueOf(sp).Elem().Interface(), nil
}

func (tx GaussStoreTx) GetEx(typ ResourceType, sql string, params ...interface{}) (interface{}, error) {
	rt, err := tx.meta.GetGoType(typ)
	if err != nil {
		return nil, err
	}

	sp := reflector.NewSlicePointer(reflect.PointerTo(rt))
	if err = tx.FillEx(sp, sql, params...); err != nil {
		return nil, err
	}
	return reflect.ValueOf(sp).Elem().Interface(), nil
}

func (tx GaussStoreTx) GetOwned(owner ResourceType, ownerID string, owned ResourceType) (interface{}, error) {
	goTyp, err := tx.meta.GetGoType(owned)
	if err != nil {
		return nil, err
	}

	sp := reflector.NewSlicePointer(reflect.PointerTo(goTyp))
	query, args, err := tx.joinSelectSqlAndArgs(owner, owned, ownerID)
	if err != nil {
		return nil, err
	}
	if err = tx.getWithSql(query, args, sp); err != nil {
		return nil, err
	}
	return reflect.ValueOf(sp).Elem().Interface(), nil
}

func (tx GaussStoreTx) Fill(cond map[string]interface{}, out interface{}) error {
	r, err := reflector.GetStructPointerInSlice(out)
	if err != nil {
		return err
	}

	query, args, err := tx.selectSqlAndArgs(ResourceDBType(r.(resource.Resource)), cond)
	if err != nil {
		return err
	}
	return tx.getWithSql(query, args, out)
}

func (tx GaussStoreTx) FillEx(out interface{}, sql string, params ...interface{}) error {
	return tx.getWithSql(sql, params, out)
}

func (tx GaussStoreTx) FillOwned(owner ResourceType, ownerID string, out interface{}) error {
	r, err := reflector.GetStructPointerInSlice(out)
	if err != nil {
		return err
	}

	query, args, err := tx.joinSelectSqlAndArgs(owner, ResourceDBType(r.(resource.Resource)), ownerID)
	if err != nil {
		return err
	}
	return tx.getWithSql(query, args, out)
}

func (tx GaussStoreTx) getWithSql(sql string, args []interface{}, out interface{}) error {
	args = tx.filterQueryParams(args...)
	logSql(sql, args...)
	rows, err := tx.Tx.QueryContext(context.TODO(), sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return tx.rowsToResources(rows, out)
}

func (tx GaussStoreTx) filterQueryParams(params ...any) []any {
	out := make([]any, 0, len(params))
	for _, value := range params {
		fieldVal := reflect.ValueOf(value)
		switch {
		case fieldVal.Type() == reflect.TypeOf(netip.Addr{}):
			v := fieldVal.Interface().(netip.Addr)
			fieldVal = reflect.ValueOf(v.String())
		case fieldVal.Type() == reflect.TypeOf(netip.Prefix{}):
			v := fieldVal.Interface().(netip.Prefix)
			_, prefix, _ := net.ParseCIDR(v.String())
			fieldVal = reflect.ValueOf(prefix.String())
		case fieldVal.Type() == reflect.TypeOf(net.IP{}):
			v := fieldVal.Interface().(net.IP)
			fieldVal = reflect.ValueOf(v.String())
		case fieldVal.Type() == reflect.TypeOf(net.IPNet{}):
			v := fieldVal.Interface().(net.IPNet)
			_, prefix, _ := net.ParseCIDR(v.String())
			fieldVal = reflect.ValueOf(prefix.String())
		case fieldVal.Type().Kind() == reflect.Array || fieldVal.Type().Kind() == reflect.Slice:
			fieldVal = reflect.ValueOf(PQArray(value))
		}
		out = append(out, fieldVal.Interface())
	}

	return out
}

func (tx GaussStoreTx) Exists(typ ResourceType, cond map[string]interface{}) (bool, error) {
	query, params, err := tx.existsSqlAndArgs(typ, cond)
	if err != nil {
		return false, err
	}

	return tx.existsWithSql(query, params...)
}

func (tx GaussStoreTx) existsWithSql(sql string, params ...interface{}) (bool, error) {
	rows, err := tx.Tx.QueryContext(context.TODO(), sql, params...)
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

func (tx GaussStoreTx) Count(typ ResourceType, cond map[string]interface{}) (int64, error) {
	query, params, err := tx.countSqlAndArgs(typ, cond)
	if err != nil {
		return 0, err
	}

	return tx.countWithSql(query, params...)
}

func (tx GaussStoreTx) CountEx(typ ResourceType, sql string, params ...interface{}) (int64, error) {
	if tx.meta.Has(typ) == false {
		return 0, fmt.Errorf("unknown resource type %v", typ)
	}
	return tx.countWithSql(sql, params...)
}

func (tx GaussStoreTx) countWithSql(sql string, params ...interface{}) (int64, error) {
	rows, err := tx.Tx.QueryContext(context.TODO(), sql, params...)
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

func (tx GaussStoreTx) Delete(typ ResourceType, cond map[string]interface{}) (int64, error) {
	query, args, err := tx.deleteSqlAndArgs(typ, cond)
	if err != nil {
		return 0, err
	}

	return tx.Exec(query, args...)
}

func (tx GaussStoreTx) Update(typ ResourceType, nv map[string]interface{}, cond map[string]interface{}) (int64, error) {
	query, args, err := tx.updateSqlAndArgs(typ, nv, cond)
	if err != nil {
		return 0, err
	}

	return tx.Exec(query, args...)
}

func (tx GaussStoreTx) Exec(sql string, params ...interface{}) (int64, error) {
	logSql(sql, params...)
	result, err := tx.Tx.ExecContext(context.TODO(), sql, params...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (tx GaussStoreTx) CopyFromEx(typ ResourceType, columns []string, values [][]interface{}) (int64, error) {
	descriptor, err := tx.meta.GetDescriptor(typ)
	if err != nil {
		return 0, fmt.Errorf("get descriptor for %v failed %v", typ, err.Error())
	}
	if len(values) == 0 {
		return 0, nil
	}
	stmt, err := tx.Prepare(pq.CopyInSchema(tx.schema, getTableNameWithoutSchema(descriptor.Typ), columns...))
	if err != nil {
		return 0, err
	}
	for _, value := range values {
		if _, err := stmt.Exec(tx.filterCopyValues(value...)...); err != nil {
			return 0, err
		}
	}

	result, err := stmt.Exec()
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (tx GaussStoreTx) CopyFrom(typ ResourceType, values [][]interface{}) (int64, error) {
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

	stmt, err := tx.Prepare(pq.CopyInSchema(tx.schema, getTableNameWithoutSchema(descriptor.Typ), columns...))
	if err != nil {
		return 0, err
	}
	for _, value := range values {
		if _, err := stmt.Exec(tx.filterCopyValues(value...)...); err != nil {
			return 0, err
		}
	}

	result, err := stmt.Exec()
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (tx GaussStoreTx) insertSqlArgsAndID(meta *ResourceMeta, r resource.Resource) (string, []interface{}, error) {
	typ := ResourceDBType(r)
	descriptor, err := meta.GetDescriptor(typ)
	if err != nil {
		return "", nil, fmt.Errorf("get %v descriptor failed %v", typ, err.Error())
	}

	tableName := getTableName(tx.schema, descriptor.Typ)
	fieldCount := len(descriptor.Fields) + len(descriptor.Owners) + len(descriptor.Refers)
	markers := make([]string, 0, fieldCount)
	for i := 1; i <= fieldCount; i++ {
		markers = append(markers, "$"+strconv.Itoa(i))
	}

	columns := make([]string, 0, fieldCount)
	for _, field := range descriptor.Fields {
		columns = append(columns, field.Name)
	}

	args := make([]interface{}, 0, fieldCount)

	id := r.GetID()
	if id == "" {
		id, _ = uuid.Gen()
		r.SetID(id)
	}

	val, isOk := reflector.GetStructFromPointer(r)
	if isOk == false {
		return "", nil, fmt.Errorf("%v is not pointer to resource", reflect.TypeOf(r).Kind().String())
	}

	for _, field := range descriptor.Fields {
		if field.Name == IDField {
			args = append(args, id)
		} else if field.Name == CreateTimeField {
			args = append(args, r.GetCreationTimestamp())
		} else {
			fieldVal := val.FieldByName(stringtool.ToUpperCamel(field.Name))
			args = append(args, tx.filterValue(fieldVal).Interface())
		}
	}

	for _, owner := range descriptor.Owners {
		args = append(args, val.FieldByName(stringtool.ToUpperCamel(string(owner))).Interface())
		columns = append(columns, string(owner))
	}
	for _, refer := range descriptor.Refers {
		args = append(args, val.FieldByName(stringtool.ToUpperCamel(string(refer))).Interface())
		columns = append(columns, string(refer))
	}

	return strings.Join(
		[]string{"insert into",
			tableName,
			"(" + strings.Join(columns, ","), ")",
			"values(", strings.Join(markers, ","), ")"}, " "), args, nil
}

func (tx GaussStoreTx) filterCopyValues(values ...any) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		fieldVal := reflect.ValueOf(value)
		out = append(out, tx.filterValue(fieldVal).Interface())
	}

	return out
}

func (tx GaussStoreTx) filterValue(fieldVal reflect.Value) reflect.Value {
	switch {
	case fieldVal.Type() == reflect.TypeOf(netip.Addr{}):
		v := fieldVal.Interface().(netip.Addr)
		fieldVal = reflect.ValueOf(v.String())
	case fieldVal.Type() == reflect.TypeOf(netip.Prefix{}):
		v := fieldVal.Interface().(netip.Prefix)
		_, prefix, _ := net.ParseCIDR(v.String())
		fieldVal = reflect.ValueOf(prefix.String())
	case fieldVal.Type() == reflect.TypeOf(net.IP{}):
		v := fieldVal.Interface().(net.IP)
		fieldVal = reflect.ValueOf(v.String())
	case fieldVal.Type() == reflect.TypeOf(net.IPNet{}):
		v := fieldVal.Interface().(net.IPNet)
		_, prefix, _ := net.ParseCIDR(v.String())
		fieldVal = reflect.ValueOf(prefix.String())
	case fieldVal.Type().Kind() == reflect.Array || fieldVal.Type().Kind() == reflect.Slice:
		fieldVal = reflect.ValueOf(PQArray(fieldVal.Interface()))
	}
	return fieldVal
}

func (tx GaussStoreTx) rowsToResources(rows *sql.Rows, out interface{}) error {
	goTyp := reflect.TypeOf(out)
	if goTyp.Kind() != reflect.Ptr || goTyp.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("output isn't a pointer to slice")
	}

	slice := reflect.Indirect(reflect.ValueOf(out))
	if slice.Type().Elem().Kind() != reflect.Ptr {
		return fmt.Errorf("output isn't a pointer to slice of pointer")
	}
	typ := slice.Type().Elem().Elem()
	fd, err := rows.Columns()
	if err != nil {
		return err
	}
	m := pgtype.NewMap()

	for rows.Next() {
		elem := reflect.New(typ)
		fields := make([]any, 0, len(fd))
		var id string
		var createTime time.Time
		specField := make(map[int]string)
		for i, d := range fd {
			if string(d) == IDField {
				fields = append(fields, &id)
			} else if string(d) == CreateTimeField {
				fields = append(fields, &createTime)
			} else {
				fieldName := stringtool.ToUpperCamel(d)
				field := elem.Elem().FieldByName(fieldName)
				switch {
				case field.Type() == reflect.TypeOf(netip.Addr{}) ||
					field.Type() == reflect.TypeOf(netip.Prefix{}) ||
					field.Type() == reflect.TypeOf(net.IP{}) ||
					field.Type() == reflect.TypeOf(net.IPNet{}):
					fields = append(fields, new(string))
					specField[i] = fieldName
				case field.Kind() == reflect.Slice || field.Kind() == reflect.Array:
					fields = append(fields, PQArray(field.Addr().Interface()))
				default:
					fields = append(fields, elem.Elem().FieldByName(fieldName).Addr().Interface())
				}
			}
		}
		if err := rows.Scan(fields...); err != nil {
			return err
		}

		for i, fieldName := range specField {
			field := elem.Elem().FieldByName(fieldName)
			switch {
			case field.Type() == reflect.TypeOf(netip.Addr{}) ||
				field.Type() == reflect.TypeOf(netip.Prefix{}) ||
				field.Type() == reflect.TypeOf(net.IP{}) ||
				field.Type() == reflect.TypeOf(net.IPNet{}):
				if err := m.Scan(pgtype.InetOID, pgtype.TextFormatCode,
					[]byte(*(fields[i].(*string))), field.Addr().Interface()); err != nil {
					return err
				}
			}
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
