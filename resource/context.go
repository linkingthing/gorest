package resource

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/zdnscloud/gorest/error"
)

const (
	Eq      Modifier = "eq"
	Ne      Modifier = "ne"
	Lt      Modifier = "lt"
	Gt      Modifier = "gt"
	Lte     Modifier = "lte"
	Gte     Modifier = "gte"
	Prefix  Modifier = "prefix"
	Suffix  Modifier = "suffix"
	Like    Modifier = "like"
	NotLike Modifier = "notlike"
	Null    Modifier = "null"
	NotNull Modifier = "notnull"

	FilterNamePageSize = "page_size"
	FilterNamePageNum  = "page_num"
)

type Context struct {
	Schemas    SchemaManager
	Request    *http.Request
	Response   http.ResponseWriter
	Resource   Resource
	Method     string
	params     map[string]interface{}
	filters    []Filter
	pagination *Pagination
}

type Filter struct {
	Name     string
	Modifier Modifier
	Values   []string
}

type Modifier string

func NewContext(resp http.ResponseWriter, req *http.Request, schemas SchemaManager) (*Context, *error.APIError) {
	r, err := schemas.CreateResourceFromRequest(req)
	if err != nil {
		return nil, err
	}

	filters, pagination := genFiltersAndPagination(req.URL)
	return &Context{
		Request:    req,
		Response:   resp,
		Resource:   r,
		Schemas:    schemas,
		Method:     req.Method,
		params:     make(map[string]interface{}),
		filters:    filters,
		pagination: pagination,
	}, nil
}

func (ctx *Context) Set(key string, value interface{}) {
	ctx.params[key] = value
}

func (ctx *Context) Get(key string) (interface{}, bool) {
	v, ok := ctx.params[key]
	return v, ok
}

func (ctx *Context) GetFilters() []Filter {
	return ctx.filters
}

func (ctx *Context) GetPagination() *Pagination {
	return ctx.pagination
}

func (ctx *Context) SetPagination(pagination *Pagination) {
	ctx.pagination = pagination
}

func genFiltersAndPagination(url *url.URL) ([]Filter, *Pagination) {
	filters := make([]Filter, 0)
	var pagination Pagination
	for k, v := range url.Query() {
		filter := Filter{
			Name:     k,
			Modifier: Eq,
			Values:   v,
		}
		i := strings.LastIndexAny(k, "_")
		if i >= 0 {
			filter.Modifier = VerifyModifier(k[i+1:])
			if filter.Modifier != Eq || k[i+1:] == "eq" {
				filter.Name = k[:i]
			}
		}

		switch filter.Name {
		case FilterNamePageSize:
			pagination.PageSize = filtersValuesToInt(filter.Values)
		case FilterNamePageNum:
			pagination.PageNum = filtersValuesToInt(filter.Values)
		default:
			filters = append(filters, filter)
		}
	}
	return filters, &pagination
}

func filtersValuesToInt(values []string) int {
	var i int
	for _, value := range values {
		if valueInt, err := strconv.Atoi(value); err == nil {
			i = valueInt
			break
		}
	}
	return i
}

func VerifyModifier(str string) Modifier {
	switch str {
	case "ne":
		return Ne
	case "lt":
		return Lt
	case "gt":
		return Gt
	case "lte":
		return Lte
	case "gte":
		return Gte
	case "prefix":
		return Prefix
	case "suffix":
		return Suffix
	case "like":
		return Like
	case "notlike":
		return NotLike
	case "null":
		return Null
	case "notnull":
		return NotNull
	default:
		return Eq
	}
}
