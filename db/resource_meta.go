package db

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/linkingthing/cement/stringtool"

	"github.com/linkingthing/gorest/resource"
)

type Datatype int

const (
	SmallInt Datatype = iota
	BigInt
	SuperInt
	Float32
	Bool
	String
	Time
	IP
	IPNet
	SmallIntArray
	BigIntArray
	SuperIntArray
	Float32Array
	StringArray
	IPSlice
	IPNetSlice
)

var postgresqlTypeMap = map[Datatype]string{
	Bool:          "boolean",
	SmallInt:      "integer",
	BigInt:        "bigint",
	SuperInt:      "numeric",
	Float32:       "float4",
	String:        "text",
	Time:          "timestamp with time zone",
	IP:            "inet",
	IPNet:         "inet",
	SmallIntArray: "integer[]",
	BigIntArray:   "bigint[]",
	SuperIntArray: "numeric[]",
	Float32Array:  "float4[]",
	StringArray:   "text[]",
	IPSlice:       "inet[]",
	IPNetSlice:    "inet[]",
}

const EmbedResource string = "ResourceBase"
const DBTag string = "db"

type Check string

const (
	NoCheck  Check = ""
	Positive Check = "positive"
)

type ResourceField struct {
	Name    string
	Type    Datatype
	Unique  bool
	Check   Check
	NotNull bool
}

type ResourceDescriptor struct {
	Typ            ResourceType
	Fields         []ResourceField
	Pks            []ResourceType
	Uks            []ResourceType
	Owners         []ResourceType
	Refers         []ResourceType
	IsRelationship bool
}

type ResourceRelationship struct {
	Typ   ResourceType
	Owner ResourceType
	Refer ResourceType
}

type ResourceMeta struct {
	resources   []ResourceType //resources has dependencies, resources to store their order
	descriptors map[ResourceType]*ResourceDescriptor
	goTypes     map[ResourceType]reflect.Type
}

func NewResourceMeta(rs []resource.Resource) (*ResourceMeta, error) {
	meta := &ResourceMeta{
		resources:   []ResourceType{},
		descriptors: make(map[ResourceType]*ResourceDescriptor),
		goTypes:     make(map[ResourceType]reflect.Type),
	}

	for _, r := range rs {
		if err := meta.Register(r); err != nil {
			return nil, err
		}
	}
	return meta, nil
}

func (meta *ResourceMeta) Clear() {
	for _, r := range meta.resources {
		delete(meta.descriptors, r)
		delete(meta.goTypes, r)
	}
}

func (meta *ResourceMeta) Has(typ ResourceType) bool {
	return meta.descriptors[typ] != nil
}

func (meta *ResourceMeta) GetGoType(typ ResourceType) (reflect.Type, error) {
	if gtyp, ok := meta.goTypes[typ]; !ok {
		return nil, fmt.Errorf("model %v is unknown", typ)
	} else {
		return gtyp, nil
	}
}

func (meta *ResourceMeta) Register(r resource.Resource) error {
	typ := ResourceDBType(r)
	if meta.Has(typ) {
		return fmt.Errorf("duplicate model:%v", typ)
	}

	descriptor, err := genDescriptor(r)
	if err != nil {
		return err
	}

	for _, m := range append(descriptor.Owners, descriptor.Refers...) {
		_, ok := meta.descriptors[m]
		if ok == false {
			return fmt.Errorf("model %v refer to %v is unknown", typ, m)
		}
	}

	meta.resources = append(meta.resources, typ)
	meta.descriptors[typ] = descriptor
	meta.goTypes[typ] = reflect.TypeOf(r).Elem()
	return nil
}

func parseField(name string, typ reflect.Type) (*ResourceField, error) {
	kind := typ.Kind()
	switch kind {
	case reflect.Int8, reflect.Int16, reflect.Uint8, reflect.Uint16, reflect.Int32:
		return &ResourceField{Name: name, Type: SmallInt}, nil
	case reflect.Int, reflect.Uint32, reflect.Int64:
		return &ResourceField{Name: name, Type: BigInt}, nil
	case reflect.Uint, reflect.Uint64:
		return &ResourceField{Name: name, Type: SuperInt}, nil
	case reflect.Float32:
		return &ResourceField{Name: name, Type: Float32}, nil
	case reflect.String:
		return &ResourceField{Name: name, Type: String}, nil
	case reflect.Bool:
		return &ResourceField{Name: name, Type: Bool}, nil
	case reflect.Struct:
		switch typ.String() {
		case "time.Time":
			return &ResourceField{Name: name, Type: Time}, nil
		case "net.IPNet":
			return &ResourceField{Name: name, Type: IPNet}, nil
		default:
			return nil, fmt.Errorf("type of field %s isn't supported:%v", name, typ.String())
		}
	case reflect.Array, reflect.Slice:
		if typ.String() == "net.IP" {
			return &ResourceField{Name: name, Type: IP}, nil
		}

		elemKind := typ.Elem().Kind()
		switch elemKind {
		case reflect.Int8, reflect.Int16, reflect.Uint8, reflect.Uint16, reflect.Int32:
			return &ResourceField{Name: name, Type: SmallIntArray}, nil
		case reflect.Int, reflect.Uint32, reflect.Int64:
			return &ResourceField{Name: name, Type: BigIntArray}, nil
		case reflect.Uint, reflect.Uint64:
			return &ResourceField{Name: name, Type: SuperIntArray}, nil
		case reflect.Float32:
			return &ResourceField{Name: name, Type: Float32Array}, nil
		case reflect.String:
			return &ResourceField{Name: name, Type: StringArray}, nil
		default:
			elemType := typ.Elem().String()
			if elemType == "net.IP" {
				return &ResourceField{Name: name, Type: IPSlice}, nil
			} else if elemType == "net.IPNet" {
				return &ResourceField{Name: name, Type: IPNetSlice}, nil
			} else {
				return nil, fmt.Errorf("type of field %s isn't supported:[%v]", name, elemKind.String())
			}
		}
	default:
		return nil, fmt.Errorf("type of field %s isn't supported:%v", name, typ.String())
	}
}

func genDescriptor(r resource.Resource) (*ResourceDescriptor, error) {
	fields := []ResourceField{
		ResourceField{Name: IDField, Type: String},
		ResourceField{Name: CreateTimeField, Type: Time},
	}
	pks := []ResourceType{IDField}
	var uks []ResourceType
	var owners []ResourceType
	var refers []ResourceType

	goTyp := reflect.TypeOf(r)
	if goTyp.Kind() != reflect.Ptr || goTyp.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("need structure pointer but get %s", goTyp.String())
	}
	goTyp = goTyp.Elem()
	fieldSet := make(map[string]struct{})
	for i := 0; i < goTyp.NumField(); i++ {
		field := goTyp.Field(i)
		if field.Name == EmbedResource {
			continue
		}

		fieldName := stringtool.ToSnake(field.Name)
		if fieldName == IDField || fieldName == CreateTimeField {
			return nil, fmt.Errorf("id or createTime field has exists in resource base")
		}

		fieldTag := field.Tag.Get(DBTag)
		if tagContains(fieldTag, "-") {
			continue
		}

		if tagContains(fieldTag, "embed") {
			fieldValue := reflect.New(field.Type)
			rt := reflect.ValueOf(fieldValue.Interface())

			embedType := reflect.Indirect(rt).Type()
			if embedType.Kind() != reflect.Ptr && embedType.Kind() != reflect.Struct {
				return nil, fmt.Errorf("embed only support [structure or pointer] but get %s", embedType.String())
			}
			if embedType.Kind() == reflect.Ptr {
				embedType = embedType.Elem()
			}
			for j := 0; j < embedType.NumField(); j++ {
				embedField := embedType.Field(j)
				embedFieldTag := embedField.Tag.Get(DBTag)
				embedFieldName := stringtool.ToSnake(embedField.Name)
				if embedFieldName == IDField || embedFieldName == CreateTimeField {
					return nil, fmt.Errorf("id or createTime field has exists in resource base")
				}

				if tagContains(embedFieldTag, "-") {
					continue
				}

				if tagContains(embedFieldTag, "embed") {
					fmt.Println("!!! not support multi embed", embedType)
					break
				}

				if tagContains(embedFieldTag, "ownby") {
					owners = append(owners, ResourceType(embedFieldName))
				} else if tagContains(embedFieldTag, "referto") {
					refers = append(refers, ResourceType(embedFieldName))
				} else {
					if newField, err := parseResourceField(embedFieldTag, embedFieldName, embedField.Type); err != nil {
						fmt.Println(err.Error())
					} else {
						if _, ok := fieldSet[newField.Name]; ok {
							return nil, fmt.Errorf("!!! field %s is duplicate\n", newField.Name)
						}
						fields = append(fields, *newField)
						fieldSet[newField.Name] = struct{}{}
					}
				}

				if tagContains(embedFieldTag, "pk") {
					pks = append(pks, ResourceType(embedFieldName))
				} else if tagContains(embedFieldTag, "uk") {
					uks = append(uks, ResourceType(embedFieldName))
				}
			}

			continue
		}

		if tagContains(fieldTag, "ownby") {
			owners = append(owners, ResourceType(fieldName))
		} else if tagContains(fieldTag, "referto") {
			refers = append(refers, ResourceType(fieldName))
		} else {
			if newField, err := parseResourceField(fieldTag, fieldName, field.Type); err != nil {
				fmt.Println(err.Error())
			} else {
				if _, ok := fieldSet[newField.Name]; ok {
					return nil, fmt.Errorf("!!! field %s is duplicate", field.Name)
				}

				fields = append(fields, *newField)
				fieldSet[newField.Name] = struct{}{}
			}
		}

		if tagContains(fieldTag, "pk") {
			pks = append(pks, ResourceType(fieldName))
		} else if tagContains(fieldTag, "uk") {
			uks = append(uks, ResourceType(fieldName))
		}
	}

	return &ResourceDescriptor{
		Typ:            ResourceDBType(r),
		Fields:         fields,
		Pks:            pks,
		Uks:            uks,
		Owners:         owners,
		Refers:         refers,
		IsRelationship: len(fields) == 1 && len(owners) == 1 && len(refers) == 1,
	}, nil
}

func parseResourceField(fieldTag, name string, typ reflect.Type) (*ResourceField, error) {
	newField, err := parseField(name, typ)
	if err != nil {
		return nil, fmt.Errorf("!!!! warning, field %s parse failed %s\n", name, err.Error())
	}

	if tagContains(fieldTag, "suk") {
		newField.Unique = true
	} else {
		newField.Unique = false
	}

	if tagContains(fieldTag, "positive") {
		newField.Check = Positive
	}

	if tagContains(fieldTag, "not null") {
		newField.NotNull = true
	}

	return newField, nil
}

func (meta *ResourceMeta) GetDescriptor(typ ResourceType) (*ResourceDescriptor, error) {
	if meta.Has(typ) {
		return meta.descriptors[typ], nil
	} else {
		return nil, fmt.Errorf("model %v is unknown", typ)
	}
}

func (meta *ResourceMeta) GetDescriptors() []*ResourceDescriptor {
	descriptors := []*ResourceDescriptor{}
	for _, r := range meta.resources {
		descriptors = append(descriptors, meta.descriptors[r])
	}
	return descriptors
}

func (meta *ResourceMeta) Resources() []ResourceType {
	return meta.resources
}

//borrow from encoding/json/tags.go
func tagContains(o string, optionName string) bool {
	if len(o) == 0 {
		return false
	}
	s := string(o)
	for s != "" {
		var next string
		i := strings.Index(s, ",")
		if i >= 0 {
			s, next = s[:i], s[i+1:]
		}
		if s == optionName {
			return true
		}
		s = next
	}
	return false
}

func (descriptor *ResourceDescriptor) GetRelationship() *ResourceRelationship {
	if descriptor.IsRelationship == true {
		return &ResourceRelationship{descriptor.Typ, descriptor.Owners[0], descriptor.Refers[0]}
	} else {
		return nil
	}
}
