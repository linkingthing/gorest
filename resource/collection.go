package resource

import (
	"fmt"
	"math"
	"reflect"
)

type ResourceCollection struct {
	Type         string                            `json:"type,omitempty"`
	ResourceType string                            `json:"resourceType,omitempty"`
	Links        map[ResourceLinkType]ResourceLink `json:"links,omitempty"`
	Pagination   *Pagination                       `json:"pagination"`
	Resources    []Resource                        `json:"data"`

	collection Resource `json:"-"`
}

type Pagination struct {
	PageTotal int `json:"pageTotal,omitempty"`
	PageNum   int `json:"pageNum,omitempty"`
	PageSize  int `json:"pageSize,omitempty"`
	Total     int `json:"total,omitempty"`
}

func NewResourceCollection(ctx *Context, i interface{}) (*ResourceCollection, error) {
	typ := ctx.Resource.GetType()
	rs, err := interfaceToResourceCollection(typ, i)
	if err != nil {
		return nil, err
	} else {
		resources, pagination := applyPagination(ctx.GetPagination(), rs)
		return &ResourceCollection{
			Type:         "collection",
			ResourceType: typ,
			Pagination:   pagination,
			Resources:    resources,
			collection:   ctx.Resource,
		}, nil
	}
}

func interfaceToResourceCollection(typ string, i interface{}) ([]Resource, error) {
	if i == nil {
		return []Resource{}, nil
	}

	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("list handler doesn't return slice but %v", v.Kind())
	}
	l := v.Len()
	if l == 0 {
		return []Resource{}, nil
	}

	resources := make([]Resource, 0, l)
	for i := 0; i < l; i++ {
		if r, err := valueToResource(typ, v.Index(i)); err != nil {
			return nil, err
		} else {
			r.SetType(typ)
			resources = append(resources, r)
		}
	}

	return resources, nil
}

func valueToResource(typ string, e reflect.Value) (Resource, error) {
	if e.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("resource isn't pointer but %v", e.Kind())
	}

	if e.IsNil() {
		return nil, fmt.Errorf("resource is nil")
	}

	if rk, ok := e.Elem().Interface().(ResourceKind); ok == false {
		return nil, fmt.Errorf("resource isn't a pointer to ResourceKind but %v", e)
	} else if DefaultKindName(rk) != typ {
		return nil, fmt.Errorf("resource with kind %v isn't same with the collection %v ", DefaultKindName(rk), typ)
	}

	r, ok := e.Interface().(Resource)
	if ok == false {
		return nil, fmt.Errorf("resource %v doesn't implement Resource interface", e.Kind())
	}

	return r, nil
}

func (rc *ResourceCollection) SetLinks(links map[ResourceLinkType]ResourceLink) {
	rc.Links = links
}

func (rc *ResourceCollection) GetLinks() map[ResourceLinkType]ResourceLink {
	return rc.Links
}

func (rc *ResourceCollection) GetCollection() Resource {
	return rc.collection
}

func (rc *ResourceCollection) GetResources() []Resource {
	return rc.Resources
}

func applyPagination(pagination *Pagination, resources []Resource) ([]Resource, *Pagination) {
	resourcesLen := len(resources)
	if resourcesLen == 0 || pagination == nil ||
		pagination.PageSize <= 0 || pagination.PageNum <= 0 || pagination.PageTotal != 0 {
		return resources, pagination
	}

	pageSize := pagination.PageSize
	pageNum := pagination.PageNum
	if pageSize > resourcesLen {
		pageSize = resourcesLen
	}

	pageTotal := int(math.Ceil(float64(resourcesLen) / float64(pageSize)))
	if pageNum > pageTotal {
		pageNum = pageTotal
	}

	startIndex := (pageNum - 1) * pageSize
	endIndex := startIndex + pageSize
	if endIndex >= resourcesLen {
		endIndex = resourcesLen
	}

	return resources[startIndex:endIndex],
		&Pagination{PageTotal: pageTotal, PageNum: pageNum, PageSize: pageSize, Total: resourcesLen}
}
