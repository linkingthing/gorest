package db

import (
	"net/netip"
	"strconv"
	"testing"

	"github.com/linkingthing/gorest/resource"
	"github.com/stretchr/testify/assert"
)

const FillValueConnStr string = "user=lx password=lx host=192.168.54.137 port=5432 database=lx sslmode=disable pool_max_conns=10"

type FillValueResource struct {
	resource.ResourceBase `json:",inline"`
	Name                  string     `json:"name" db:"nk"`
	Brief                 string     `json:"brief" db:"suk"`
	Age                   int        `json:"age" db:"uk"`
	ParentId              string     `json:"parentId" db:"nk"`
	Address               string     `json:"address" db:"uk"`
	IpAddress             netip.Addr `json:"ipAddress"`
	Street                string     `json:"street" db:"not null"`
	Friends               []string   `json:"friends" db:"snk"`
}

func TestFillValue(t *testing.T) {
	meta, err := NewResourceMeta([]resource.Resource{&FillValueResource{}})
	assert.NoError(t, err)
	store, err := NewRStore(FillValueConnStr, meta)
	assert.NoError(t, err)

	var preData []*FillValueResource
	address, err := netip.ParseAddr("10.0.0.1")
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		p := &FillValueResource{
			Name:      "name_" + strconv.Itoa(i),
			ParentId:  "parent_" + strconv.Itoa(i),
			Age:       i,
			Street:    "local",
			Brief:     "brief_" + strconv.Itoa(i),
			Address:   "address_" + strconv.Itoa(i),
			IpAddress: address,
			Friends:   []string{"j", strconv.Itoa(i)},
		}
		preData = append(preData, p)
		address = address.Next()
	}

	var result []*FillValueResource
	var datas = []struct {
		name        string
		f           func(Transaction) error
		expectCount int
	}{
		{
			name: "fill_eq(=)",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"name": FillValue{Value: "name_1", Operator: OperatorEq},
				}, &result)
			},
			expectCount: 1,
		},
		{
			name: "fill_lt(<)",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"age": FillValue{Value: 1, Operator: OperatorLt},
				}, &result)
			},
			expectCount: 1,
		},
		{
			name: "fill_lte(<=)",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"age": FillValue{Value: 1, Operator: OperatorLte},
				}, &result)
			},
			expectCount: 2,
		},
		{
			name: "fill_gt(>)",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"age": FillValue{Value: 9, Operator: OperatorGt},
				}, &result)
			},
			expectCount: 0,
		},
		{
			name: "fill_gte(>=)",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"age": FillValue{Value: 9, Operator: OperatorGte},
				}, &result)
			},
			expectCount: 1,
		},
		{
			name: "fill_ne(!=)",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"age": FillValue{Value: 9, Operator: OperatorNe},
				}, &result)
			},
			expectCount: 9,
		},
		{
			name: "fill_any",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"name": FillValue{Value: []string{"name_1", "name_2"}, Operator: OperatorAny},
				}, &result)
			},
			expectCount: 2,
		},
		{
			name: "fill_overlay(&&)",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"friends": FillValue{Value: []string{"j", "2"}, Operator: OperatorOverlap},
				}, &result)
			},
			expectCount: 10,
		},
		{
			name: "fill_like(%like%)",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"name": FillValue{Value: "me", Operator: OperatorLike},
				}, &result)
			},
			expectCount: 10,
		},
		{
			name: "fill_like_prefix(%like)",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"name": FillValue{Value: "1", Operator: OperatorLikePrefix},
				}, &result)
			},
			expectCount: 1,
		},
		{
			name: "fill_like_suffix(like%)",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"name": FillValue{Value: "name_1", Operator: OperatorLikeSuffix},
				}, &result)
			},
			expectCount: 1,
		},
		{
			name: "fill_address_contain_eq(>>=)",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"ip_address": FillValue{Value: "10.0.0.2", Operator: OperatorSubnetContainEq},
				}, &result)
			},
			expectCount: 1,
		},
		{
			name: "fill_address_contain_eq_by(<<=)",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"ip_address": FillValue{Value: "10.0.0.2", Operator: OperatorSubnetContainEqBy},
				}, &result)
			},
			expectCount: 1,
		},
		{
			name: "fill_address_contain_by(<<)",
			f: func(tx Transaction) error {
				return tx.Fill(map[string]interface{}{
					"ip_address": FillValue{Value: "10.0.0.0/24", Operator: OperatorSubnetContainBy},
				}, &result)
			},
			expectCount: 10,
		},
	}

	assert.NoError(t, WithTx(store, func(tx Transaction) error {
		for _, datum := range preData {
			_, err := tx.Insert(datum)
			assert.NoError(t, err)
		}

		for _, data := range datas {
			t.Run(data.name, func(t *testing.T) {
				assert.NoError(t, data.f(tx))
				assert.Equal(t, data.expectCount, len(result))
				result = make([]*FillValueResource, 0)
			})
		}

		return tx.Rollback()
	}))
}

func TestFillValueCount(t *testing.T) {
	meta, err := NewResourceMeta([]resource.Resource{&FillValueResource{}})
	assert.NoError(t, err)
	store, err := NewRStore(FillValueConnStr, meta)
	assert.NoError(t, err)

	var preData []*FillValueResource
	address, err := netip.ParseAddr("10.0.0.1")
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		p := &FillValueResource{
			Name:      "name_" + strconv.Itoa(i),
			ParentId:  "parent_" + strconv.Itoa(i),
			Age:       i,
			Street:    "local",
			Brief:     "brief_" + strconv.Itoa(i),
			Address:   "address_" + strconv.Itoa(i),
			IpAddress: address,
			Friends:   []string{"j", strconv.Itoa(i)},
		}
		preData = append(preData, p)
		address = address.Next()
	}

	IndexResourceDbType := ResourceDBType(&FillValueResource{})
	var datas = []struct {
		name        string
		f           func(Transaction) (int64, error)
		expectCount int64
	}{
		{
			name: "count_eq(=)",
			f: func(tx Transaction) (int64, error) {
				return tx.Count(IndexResourceDbType, map[string]interface{}{
					"name": FillValue{Value: "name_1", Operator: OperatorEq},
				})
			},
			expectCount: 1,
		},
		{
			name: "count_gt(>)",
			f: func(tx Transaction) (int64, error) {
				return tx.Count(IndexResourceDbType, map[string]interface{}{
					"name": FillValue{Value: "name_1", Operator: OperatorGt},
				})
			},
			expectCount: 8,
		},
	}

	assert.NoError(t, WithTx(store, func(tx Transaction) error {
		for _, datum := range preData {
			_, err := tx.Insert(datum)
			assert.NoError(t, err)
		}

		for _, data := range datas {
			t.Run(data.name, func(t *testing.T) {
				result, err := data.f(tx)
				assert.NoError(t, err)
				assert.Equal(t, data.expectCount, result)
			})
		}

		return tx.Rollback()
	}))
}

func TestFillValueUpdate(t *testing.T) {
	meta, err := NewResourceMeta([]resource.Resource{&FillValueResource{}})
	assert.NoError(t, err)
	store, err := NewRStore(FillValueConnStr, meta)
	assert.NoError(t, err)

	var preData []*FillValueResource
	address, err := netip.ParseAddr("10.0.0.1")
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		p := &FillValueResource{
			Name:      "name_" + strconv.Itoa(i),
			ParentId:  "parent_" + strconv.Itoa(i),
			Age:       i,
			Street:    "local",
			Brief:     "brief_" + strconv.Itoa(i),
			Address:   "address_" + strconv.Itoa(i),
			IpAddress: address,
			Friends:   []string{"j", strconv.Itoa(i)},
		}
		preData = append(preData, p)
		address = address.Next()
	}

	IndexResourceDbType := ResourceDBType(&FillValueResource{})
	var datas = []struct {
		name       string
		updateFunc func(Transaction) (int64, error)
		countFunc  func(Transaction) (int64, error)
	}{
		{
			name: "update_eq(=)",
			updateFunc: func(tx Transaction) (int64, error) {
				return tx.Update(IndexResourceDbType,
					map[string]interface{}{"name": "name_test"},
					map[string]interface{}{"name": FillValue{Value: "name_1", Operator: OperatorEq}})
			},
			countFunc: func(tx Transaction) (int64, error) {
				return tx.Count(IndexResourceDbType, map[string]interface{}{
					"name": FillValue{Value: "name_test", Operator: OperatorEq},
				})
			},
		},
	}

	assert.NoError(t, WithTx(store, func(tx Transaction) error {
		for _, datum := range preData {
			_, err := tx.Insert(datum)
			assert.NoError(t, err)
		}

		for _, data := range datas {
			t.Run(data.name, func(t *testing.T) {
				_, err := data.updateFunc(tx)
				assert.NoError(t, err)
				count, err := data.countFunc(tx)
				assert.NoError(t, err)
				assert.Equal(t, int64(1), count)
			})
		}

		return tx.Rollback()
	}))
}

func TestFillValueDelete(t *testing.T) {
	meta, err := NewResourceMeta([]resource.Resource{&FillValueResource{}})
	assert.NoError(t, err)
	store, err := NewRStore(FillValueConnStr, meta)
	assert.NoError(t, err)

	var preData []*FillValueResource
	address, err := netip.ParseAddr("10.0.0.1")
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		p := &FillValueResource{
			Name:      "name_" + strconv.Itoa(i),
			ParentId:  "parent_" + strconv.Itoa(i),
			Age:       i,
			Street:    "local",
			Brief:     "brief_" + strconv.Itoa(i),
			Address:   "address_" + strconv.Itoa(i),
			IpAddress: address,
			Friends:   []string{"j", strconv.Itoa(i)},
		}
		preData = append(preData, p)
		address = address.Next()
	}

	IndexResourceDbType := ResourceDBType(&FillValueResource{})
	var datas = []struct {
		name        string
		updateFunc  func(Transaction) (int64, error)
		countFunc   func(Transaction) (int64, error)
		expectCount int64
	}{
		{
			name: "delete_eq(=)",
			updateFunc: func(tx Transaction) (int64, error) {
				return tx.Delete(IndexResourceDbType,
					map[string]interface{}{"name": FillValue{Value: "name_1", Operator: OperatorEq}})
			},
			countFunc: func(tx Transaction) (int64, error) {
				return tx.Count(IndexResourceDbType, map[string]interface{}{
					"name": FillValue{Value: "name", Operator: OperatorLikeSuffix},
				})
			},
			expectCount: 9,
		},
	}

	assert.NoError(t, WithTx(store, func(tx Transaction) error {
		for _, datum := range preData {
			_, err := tx.Insert(datum)
			assert.NoError(t, err)
		}

		for _, data := range datas {
			t.Run(data.name, func(t *testing.T) {
				_, err := data.updateFunc(tx)
				assert.NoError(t, err)
				count, err := data.countFunc(tx)
				assert.NoError(t, err)
				assert.Equal(t, data.expectCount, count)
			})
		}

		return tx.Rollback()
	}))
}
