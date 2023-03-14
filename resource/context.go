package resource

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/linkingthing/gorest/error"
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
	filters, pagination, err := genFiltersAndPagination(req.URL)
	if err != nil {
		return nil, err.Localization(IsRequestAcceptLanguageZH(req))
	}

	r, err := schemas.CreateResourceFromRequest(req)
	if err != nil {
		return nil, err
	}

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

func (ctx *Context) IsAcceptLanguageZH() bool {
	return strings.HasPrefix(ctx.Request.Header.Get("accept-language"), "zh")
}

func IsRequestAcceptLanguageZH(request *http.Request) bool {
	return strings.HasPrefix(request.Header.Get("accept-language"), "zh")
}

func genFiltersAndPagination(requestUrl *url.URL) ([]Filter, *Pagination, *error.APIError) {
	valueMap, err_ := url.ParseQuery(requestUrl.RawQuery)
	if err_ != nil {
		return nil, nil, error.NewAPIError(error.InvalidFormat, error.ErrorMessage{MessageEN: err_.Error(),
			MessageCN: error.ErrorCHNameInvalidFormat + err_.Error()})
	}

	filters := make([]Filter, 0)
	var pagination Pagination
	var err *error.APIError
	for k, v := range valueMap {
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
			if pagination.PageSize, err = filtersValuesToInt(filter.Values); err != nil {
				return nil, nil, err
			}
		case FilterNamePageNum:
			if pagination.PageNum, err = filtersValuesToInt(filter.Values); err != nil {
				return nil, nil, err
			}
		default:
			filters = append(filters, filter)
		}
	}

	return filters, &pagination, nil
}

func filtersValuesToInt(values []string) (int, *error.APIError) {
	var i int
	for _, value := range values {
		if valueInt, err := strconv.Atoi(value); err != nil {
			return 0, error.NewAPIError(error.InvalidFormat,
				*error.NewErrorMessage("negative number",
					fmt.Sprintf(error.ErrorCHNameInvalidQuery, value)))
		} else if i = valueInt; i < 0 {
			return 0, error.NewAPIError(error.InvalidFormat,
				*error.NewErrorMessage("negative number",
					fmt.Sprintf(error.ErrorCHNameInvalidQuery, value)))
		} else {
			break
		}
	}

	return i, nil
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
