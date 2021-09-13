package resource

import (
	"encoding/json"
	"testing"

	ut "github.com/zdnscloud/cement/unittest"
)

func TestCollectionToJson(t *testing.T) {
	ctx := &Context{
		Resource: &dumbResource{
			Number: 10,
		},
	}

	rs, err := NewResourceCollection(ctx, nil)
	ut.Assert(t, err == nil, "")
	ut.Assert(t, rs.Resources != nil, "")
	d, _ := json.Marshal(rs)
	ut.Equal(t, string(d), `{"type":"collection","data":[]}`)

	rs2, err := NewResourceCollection(ctx, []*dumbResource{})
	ut.Assert(t, err == nil, "")
	ut.Assert(t, rs2.Resources != nil, "")
	ut.Equal(t, len(rs2.Resources), 0)
	d2, _ := json.Marshal(rs2)
	ut.Equal(t, string(d), string(d2))
}

func TestPagination(t *testing.T) {
	var rs []Resource
	for i := 0; i < 55; i++ {
		rs = append(rs, &dumbResource{
			Number: i,
		})
	}

	retrs, pagination := applyPagination(&Pagination{PageSize: 10, PageNum: 5}, rs)
	ut.Assert(t, len(retrs) == 10, "")
	ut.Assert(t, pagination.PageTotal == 6, "")
	ut.Assert(t, pagination.PageNum == 5, "")
	ut.Assert(t, pagination.Total == 55, "")
	ut.Assert(t, pagination.PageSize == 10, "")

	retrs, pagination = applyPagination(&Pagination{PageSize: 100, PageNum: 5}, rs)
	ut.Assert(t, len(retrs) == 55, "")
	ut.Assert(t, pagination.PageTotal == 1, "")
	ut.Assert(t, pagination.PageNum == 1, "")
	ut.Assert(t, pagination.Total == 55, "")
	ut.Assert(t, pagination.PageSize == 55, "")
}
