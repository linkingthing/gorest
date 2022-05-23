package resource

import (
	"encoding/json"
	"net/url"
	"testing"

	ut "github.com/linkingthing/cement/unittest"
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

func TestGenFiltersAndPagination(t *testing.T) {
	rawUrls := []string{
		"https://10.0.0.66/apis/linkingthing.com/organization/v1/organizations?action=create_subnode",
		"https://10.0.0.66/apis/linkingthing.com/organization/v1/organizations?page_size=10&page_num=1",
		"https://10.0.0.66/apis/linkingthing.com/organization/v1/organizations?vendor=abc",
		"https://10.0.0.66/apis/linkingthing.com/organization/v1/organizations?page_size=10&ttt=xy_z&prefix=2009::/64&prefix1=2001::&prefix2=10.0.0.0/24",
	}

	for _, rawUrl := range rawUrls {
		reqUrl, err := url.Parse(rawUrl)
		if err != nil {
			t.Error(err)
			continue
		}

		filters, pagination, restErr := genFiltersAndPagination(reqUrl)
		if restErr != nil {
			t.Error(restErr)
			continue
		}

		t.Log(filters, pagination)
	}

}
