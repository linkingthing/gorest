package db

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"text/template"

	"github.com/linkingthing/cement/reflector"
	"github.com/linkingthing/cement/stringtool"
	"github.com/linkingthing/cement/uuid"
	"github.com/linkingthing/gorest/resource"
)

type BaseTx struct {
	meta        *ResourceMeta
	schema      string
	tablePrefix string
}

const (
	DefaultSchemaName             = "lx"
	DefaultTablePrefix            = "gr_"
	dropSchemaSql                 = "drop schema if exists %s cascade"
	joinSqlTemplateContent string = "select {{.OwnedTable}}.* from {{.OwnedTable}} inner join {{.RelTable}} on ({{.OwnedTable}}.id={{.RelTable}}.{{.Owned}} and {{.RelTable}}.{{.Owner}}=$1)"
)

var joinSqlTemplate *template.Template

func init() {
	joinSqlTemplate, _ = template.New("").Parse(joinSqlTemplateContent)
}

func NewBaseTx(meta *ResourceMeta, schema string) *BaseTx {
	if schema == "" {
		schema = DefaultSchemaName
	}
	return &BaseTx{meta: meta, schema: schema}
}

func getTableName(schema string, typ ResourceType) string {
	if schema != DefaultSchemaName {
		return schema + "." + string(typ)
	}
	return schema + "." + DefaultTablePrefix + string(typ)
}

func getTableNameWithoutSchema(typ ResourceType) string {
	return DefaultTablePrefix + string(typ)
}

func (b *BaseTx) insertSqlArgsAndID(r resource.Resource) (string, []interface{}, error) {
	typ := ResourceDBType(r)
	descriptor, err := b.meta.GetDescriptor(typ)
	if err != nil {
		return "", nil, fmt.Errorf("get %v descriptor failed %v", typ, err.Error())
	}

	tableName := getTableName(b.schema, descriptor.Typ)
	fieldCount := len(descriptor.Fields) + len(descriptor.Owners) + len(descriptor.Refers)
	markers := make([]string, 0, fieldCount)
	for i := 1; i <= fieldCount; i++ {
		markers = append(markers, "$"+strconv.Itoa(i))
	}
	sql := strings.Join([]string{"insert into", tableName, "values(", strings.Join(markers, ","), ")"}, " ")
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
			args = append(args, fieldVal.Interface())
		}
	}

	for _, owner := range descriptor.Owners {
		args = append(args, val.FieldByName(stringtool.ToUpperCamel(string(owner))).Interface())
	}

	for _, refer := range descriptor.Refers {
		args = append(args, val.FieldByName(stringtool.ToUpperCamel(string(refer))).Interface())
	}

	return sql, args, nil
}

func (b *BaseTx) selectSqlAndArgs(typ ResourceType, conds map[string]any) (string, []any, error) {
	descriptor, err := b.meta.GetDescriptor(typ)
	if err != nil {
		return "", nil, fmt.Errorf("get descriptor for %v failed %v", typ, err.Error())
	}

	orderStat := "order by id"
	if order_, ok := conds["orderby"]; ok == true {
		if order, ok := order_.(string); ok == false {
			return "", nil, fmt.Errorf("order argument isn't string:%v", order_)
		} else {
			orderStat = fmt.Sprintf("order by %s", stringtool.ToSnake(order))
			delete(conds, "orderby")
		}
	}

	limitStat := ""
	if limit_, ok := conds["limit"]; ok == true {
		if offset_, ok := conds["offset"]; ok == true {
			limit, _ := limit_.(int)
			offset, _ := offset_.(int)
			delete(conds, "limit")
			delete(conds, "offset")
			limitStat = fmt.Sprintf("limit %d offset %d", limit, offset)
		}
	}

	whereState, args, err := getSqlWhereState(conds)
	if err != nil {
		return "", nil, err
	} else if whereState == "" {
		return strings.Join([]string{"select * from ", getTableName(b.schema, descriptor.Typ), orderStat, limitStat}, " "), nil, nil
	} else {
		return strings.Join([]string{"select * from", getTableName(b.schema, descriptor.Typ), "where", whereState, orderStat, limitStat}, " "), args, nil
	}
}

func (b *BaseTx) deleteSqlAndArgs(typ ResourceType, conds map[string]interface{}) (string, []interface{}, error) {
	descriptor, err := b.meta.GetDescriptor(typ)
	if err != nil {
		return "", nil, fmt.Errorf("get descriptor for %v failed %v", typ, err.Error())
	}

	if len(conds) == 0 {
		return "delete from " + getTableName(b.schema, descriptor.Typ), nil, nil
	}

	whereState := make([]string, 0, len(conds))
	args := make([]interface{}, 0, len(conds))
	markerSeq := 1
	for k, v := range conds {
		if vf, ok := v.(FillValue); ok {
			s, arg, err := vf.buildSql(k, markerSeq)
			if err != nil {
				return "", nil, err
			}
			whereState = append(whereState, s)
			args = append(args, arg)
			markerSeq += 1
		} else {
			whereState = append(whereState, stringtool.ToSnake(k)+"=$"+strconv.Itoa(markerSeq))
			args = append(args, v)
			markerSeq += 1
		}
	}
	whereSeq := strings.Join(whereState, " and ")
	return strings.Join([]string{"delete from", getTableName(b.schema, descriptor.Typ), "where", whereSeq}, " "), args, nil
}

// select count(*) from zc_zone where zdnsuser=$1
func (b *BaseTx) existsSqlAndArgs(typ ResourceType, conds map[string]any) (string, []any, error) {
	descriptor, err := b.meta.GetDescriptor(typ)
	if err != nil {
		return "", nil, fmt.Errorf("get descriptor for %v failed %v", typ, err.Error())
	}

	if len(conds) == 0 {
		return "select (exists (select 1 from " + getTableName(b.schema, descriptor.Typ) + " limit 1))", nil, nil
	}

	whereState := make([]string, 0, len(conds))
	args := make([]interface{}, 0, len(conds))
	markerSeq := 1

	for k, v := range conds {
		if vf, ok := v.(FillValue); ok {
			s, arg, err := vf.buildSql(k, markerSeq)
			if err != nil {
				return "", nil, err
			}
			whereState = append(whereState, s)
			args = append(args, arg)
			markerSeq += 1
		} else {
			whereState = append(whereState, stringtool.ToSnake(k)+"=$"+strconv.Itoa(markerSeq))
			args = append(args, v)
			markerSeq += 1
		}
	}

	whereSeq := strings.Join(whereState, " and ")
	return strings.Join([]string{"select (exists (select 1 from ", getTableName(b.schema, descriptor.Typ), "where", whereSeq, "limit 1))"}, " "), args, nil
}

// select count(*) from zc_zone where zdnsuser=$1
func (b *BaseTx) countSqlAndArgs(typ ResourceType, conds map[string]any) (string, []any, error) {
	descriptor, err := b.meta.GetDescriptor(typ)
	if err != nil {
		return "", nil, fmt.Errorf("get descriptor for %v failed %v", typ, err.Error())
	}

	whereState, args, err := getSqlWhereState(conds)
	if err != nil {
		return "", nil, err
	} else if whereState == "" {
		return "select count(*) from " + getTableName(b.schema, descriptor.Typ), nil, nil
	} else {
		return strings.Join([]string{"select count(*) from", getTableName(b.schema, descriptor.Typ), "where", whereState}, " "), args, nil
	}
}

// UPDATE films SET kind = 'Dramatic' WHERE kind = 'Drama';
func (b *BaseTx) updateSqlAndArgs(typ ResourceType, newVals map[string]any, conds map[string]any) (string, []any, error) {
	descriptor, err := b.meta.GetDescriptor(typ)
	if err != nil {
		return "", nil, fmt.Errorf("get descriptor for %v failed %v", typ, err.Error())
	}

	setState := make([]string, 0, len(newVals))
	whereState := make([]string, 0, len(conds))
	args := make([]interface{}, 0, len(newVals)+len(conds))
	markerSeq := 1
	for k, v := range newVals {
		setState = append(setState, stringtool.ToSnake(k)+"=$"+strconv.Itoa(markerSeq))
		args = append(args, v)
		markerSeq += 1

	}

	for k, v := range conds {
		if vf, ok := v.(FillValue); ok {
			s, arg, err := vf.buildSql(k, markerSeq)
			if err != nil {
				return "", nil, err
			}
			whereState = append(whereState, s)
			args = append(args, arg)
			markerSeq += 1
		} else {
			whereState = append(whereState, stringtool.ToSnake(k)+"=$"+strconv.Itoa(markerSeq))
			args = append(args, v)
			markerSeq += 1
		}
	}

	setSeq := strings.Join(setState, ",")
	whereSeq := strings.Join(whereState, " and ")
	return strings.Join([]string{"update", getTableName(b.schema, descriptor.Typ), "set", setSeq, "where", whereSeq}, " "), args, nil
}

type joinSqlParams struct {
	OwnedTable string
	RelTable   string
	Owned      string
	Owner      string
}

func (b *BaseTx) joinSelectSqlAndArgs(ownerTyp ResourceType, ownedTyp ResourceType, ownerID string) (string, []any, error) {
	relationTyp := strings.ToLower(string(ownerTyp)) + "_" + strings.ToLower(string(ownedTyp))
	ownedDescriptor, err := b.meta.GetDescriptor(ownedTyp)
	if err != nil {
		return "", nil, fmt.Errorf("get descriptor for %v failed %v", ownedTyp, err.Error())
	}

	relationDescriptor, err := b.meta.GetDescriptor(ResourceType(relationTyp))
	if err != nil {
		return "", nil, fmt.Errorf("get descriptor for %v failed %v", relationTyp, err.Error())
	}

	params := &joinSqlParams{getTableName(b.schema, ownedDescriptor.Typ),
		getTableName(b.schema, relationDescriptor.Typ),
		string(ownedTyp),
		string(ownerTyp)}

	var buf bytes.Buffer
	if err := joinSqlTemplate.Execute(&buf, params); err != nil {
		return "", nil, fmt.Errorf("build join sql failed:%s", err.Error())
	}
	return buf.String(), []interface{}{ownerID}, nil
}

func getSqlWhereState(conds map[string]any) (string, []any, error) {
	if len(conds) == 0 {
		return "", nil, nil
	}

	var searchKeys []string
	if keys_, ok := conds["search"]; ok {
		if keys, ok := keys_.(string); ok {
			searchKeys = strings.Split(keys, ",")
		}
		delete(conds, "search")
	}

	var matchListKeys []string
	if keys_, ok := conds["match_list"]; ok {
		if keys, ok := keys_.(string); ok {
			matchListKeys = strings.Split(keys, ",")
		}
		delete(conds, "match_list")
	}

	whereState := make([]string, 0, len(conds))
	args := make([]interface{}, 0, len(conds))
	markerSeq := 1
	for k, v := range conds {
		isSearchKey := false
		for _, sk := range searchKeys {
			if k == sk {
				isSearchKey = true
				break
			}
		}

		isMatchListKey := false
		for _, mk := range matchListKeys {
			if k == mk {
				isMatchListKey = true
				break
			}
		}

		if isSearchKey {
			whereState = append(whereState, stringtool.ToSnake(k)+" like $"+strconv.Itoa(markerSeq))
			if sv, ok := v.(string); ok == true {
				args = append(args, "%"+sv+"%")
				markerSeq += 1
			} else {
				return "", nil, fmt.Errorf("search condition isn't string, but %v", v)
			}
		} else if isMatchListKey {
			if sv, ok := v.(string); ok == true {
				var orStatSegs []string
				matchList := strings.Split(sv, ",")
				for _, mv := range matchList {
					orStatSegs = append(orStatSegs, fmt.Sprintf("%s=$%d", stringtool.ToSnake(k), markerSeq))
					markerSeq += 1
					args = append(args, mv)
				}
				whereState = append(whereState, "( "+strings.Join(orStatSegs, " or ")+")")
			} else {
				return "", nil, fmt.Errorf("match condition isn't string, but %v", v)
			}
		} else {
			if vf, ok := v.(FillValue); ok {
				s, arg, err := vf.buildSql(k, markerSeq)
				if err != nil {
					return "", nil, err
				}
				whereState = append(whereState, s)
				args = append(args, arg)
				markerSeq += 1
			} else {
				whereState = append(whereState, stringtool.ToSnake(k)+"=$"+strconv.Itoa(markerSeq))
				args = append(args, v)
				markerSeq += 1
			}
		}
	}

	return strings.Join(whereState, " and "), args, nil
}
