package resource

import (
	"net/url"
	"testing"
)

func TestFilter(t *testing.T) {
	var datas = []*url.URL{
		&url.URL{RawQuery: "a=b"},
		&url.URL{RawQuery: "a= b"},
		&url.URL{RawQuery: "a=b "},
		&url.URL{RawQuery: "a=b d"},
		&url.URL{RawQuery: "a= b e "},
		&url.URL{RawQuery: "a= b,c,e "},
		&url.URL{RawQuery: "a= b,c,e&page_num=1&page_size=2"},
	}

	for _, data := range datas {
		t.Run(data.RawQuery, func(t *testing.T) {
			f, p, err := genFiltersAndPagination(data)
			if err != nil {
				t.Error(err)
			}
			t.Log(f, p)
		})
	}
}
