package db

import (
	"fmt"

	"github.com/linkingthing/gorest/resource"
	"github.com/zdnscloud/cement/reflector"
	"github.com/zdnscloud/cement/stringtool"
)

const (
	IDField         = "id"
	CreateTimeField = "create_time"
	maxBatchSize    = 65535
)

type ResourceType string

func ResourceDBType(r resource.Resource) ResourceType {
	n, err := reflector.StructName(r)
	if err != nil {
		panic(fmt.Sprintf("%v doesn't point to a struct implement resource:%s", r, err.Error()))
	}
	return ResourceType(stringtool.ToSnake(n))
}

func ResourceToMap(r resource.Resource) (map[string]interface{}, error) {
	v, ok := reflector.GetStructFromPointer(r)
	if ok == false {
		return nil, fmt.Errorf("need structure pointer but get %v", v.Kind().String())
	}

	m := make(map[string]interface{})
	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		f := typ.Field(i)
		n := f.Name
		if n == EmbedResource {
			continue
		}

		if tagContains(f.Tag.Get(DBTag), "-") {
			continue
		}

		n = stringtool.ToSnake(n)
		if n == IDField || n == CreateTimeField {
			continue
		}
		m[n] = v.Field(i).Interface()
	}
	return m, nil
}
