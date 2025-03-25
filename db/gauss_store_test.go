package db

import (
	"fmt"
	"github.com/linkingthing/gorest/resource"
	"github.com/stretchr/testify/assert"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"
)

const (
	GaussConnStr string = "user=lx password=Linking@201907^%$# host=10.0.0.67 port=25432 database=lx sslmode=disable  pool_max_conns=10"
	//GaussConnStr string = "user=lx password=Linking@201907^%$# host=1.95.184.177 port=25432 database=lx sslmode=disable  pool_max_conns=10"
)

type GaussResource struct {
	resource.ResourceBase `json:",inline"`
	Name                  string       `json:"name" db:"nk"`
	Brief                 string       `json:"brief" db:"suk"`
	Age                   int          `json:"age" db:"uk"`
	ParentId              string       `json:"parentId" db:"nk"`
	Address               string       `json:"address" db:"uk"`
	IpAddress             netip.Addr   `json:"ipAddress"`
	IpAddressV6           netip.Addr   `json:"ipAddressV6"`
	Prefix                netip.Prefix `json:"prefix"`
	PrefixV6              netip.Prefix `json:"prefixV6"`
	NetIP                 net.IP       `json:"netIP"`
	NetIPNet              net.IPNet    `json:"netIPNet"`
	Street                string       `json:"street" db:"not null"`
	Friends               []string     `json:"friends" db:"snk"`
}

func (idx *GaussResource) GenCopyValues() []any {
	return []any{
		idx.GetID(),
		time.Now(),
		idx.Name,
		idx.Brief,
		idx.Age,
		idx.ParentId,
		idx.Address,
		idx.IpAddress,
		idx.IpAddressV6,
		idx.Prefix,
		idx.PrefixV6,
		idx.NetIP,
		idx.NetIPNet,
		idx.Street,
		idx.Friends,
	}
}

var TableGaussResource = ResourceDBType(&GaussResource{})

func TestConnect(t *testing.T) {
	meta, err := NewResourceMeta([]resource.Resource{&GaussResource{}})
	assert.NoError(t, err)
	store, err := setup(GaussConnStr, meta)
	assert.NoError(t, err)
	t.Log(store)
}

func setup(conStr string, meta *ResourceMeta) (ResourceStore, error) {
	store, err := NewGaussStore(conStr, meta)
	if err != nil {
		return nil, fmt.Errorf("create store failed: %s", err.Error())
	}
	SetDebug(false)
	return store, nil
}

func setupGaussResource(conStr string) (ResourceStore, error) {
	meta, err := NewResourceMeta([]resource.Resource{&GaussResource{}})
	if err != nil {
		return nil, fmt.Errorf("create store failed: %s", err.Error())
	}
	store, err := NewGaussStore(conStr, meta)
	if err != nil {
		return nil, fmt.Errorf("create store failed: %s", err.Error())
	}
	SetDebug(false)
	return store, nil
}

func TestGaussInitSchema(t *testing.T) {
	meta, err := NewResourceMeta([]resource.Resource{&GaussResource{}})
	assert.NoError(t, err)
	store, err := NewGaussStore(GaussConnStr, meta, WithSchema("gr"))
	assert.NoError(t, err)
	t.Log(store)
}

func TestInsert(t *testing.T) {
	store, err := setupGaussResource(GaussConnStr)
	assert.NoError(t, err)

	_, netIPNet, err := net.ParseCIDR("192.168.0.0/24")
	assert.NoError(t, err)
	r := &GaussResource{
		Name:        "test",
		Brief:       "brief",
		Age:         18,
		ParentId:    "123",
		Address:     "localhost",
		IpAddress:   netip.MustParseAddr("127.0.0.1"),
		IpAddressV6: netip.MustParseAddr("240e:699:1:4a00:46a8:42ff:fe0b:a675"),
		Prefix:      netip.MustParsePrefix("192.168.0.0/24"),
		PrefixV6:    netip.MustParsePrefix("2001:1000::/80"),
		NetIP:       net.ParseIP("127.0.0.2"),
		NetIPNet:    *netIPNet,
		Street:      "no street",
		Friends:     []string{"friend1", "friend2"},
	}
	r.SetID("test-1")

	assert.NoError(t, WithTx(store, func(tx Transaction) error {
		if _, err := tx.Delete(TableGaussResource, nil); err != nil {
			return err
		}
		if _, err := tx.Insert(r); err != nil {
			return err
		}
		return tx.Rollback()
	}))
}

func TestDelete(t *testing.T) {
	store, err := setupGaussResource(GaussConnStr)
	assert.NoError(t, err)

	_, netIPNet, err := net.ParseCIDR("192.168.0.0/24")
	assert.NoError(t, err)
	r := &GaussResource{
		Name:        "test",
		Brief:       "brief",
		Age:         18,
		ParentId:    "123",
		Address:     "localhost",
		IpAddress:   netip.MustParseAddr("127.0.0.1"),
		IpAddressV6: netip.MustParseAddr("240e:699:1:4a00:46a8:42ff:fe0b:a675"),
		Prefix:      netip.MustParsePrefix("192.168.0.0/24"),
		PrefixV6:    netip.MustParsePrefix("2001:1000::/80"),
		NetIP:       net.ParseIP("127.0.0.2"),
		NetIPNet:    *netIPNet,
		Street:      "no street",
		Friends:     []string{"friend1", "friend2"},
	}
	r.SetID("test-1")

	assert.NoError(t, WithTx(store, func(tx Transaction) error {
		if _, err := tx.Delete(TableGaussResource, nil); err != nil {
			return err
		}
		if _, err := tx.Insert(r); err != nil {
			return err
		}
		_, err = tx.Delete(TableGaussResource, map[string]interface{}{IDField: r.GetID()})
		return err
	}))
}

func TestFill(t *testing.T) {
	store, err := setupGaussResource(GaussConnStr)
	assert.NoError(t, err)

	_, netIPNet, err := net.ParseCIDR("192.168.0.0/24")
	assert.NoError(t, err)
	r := &GaussResource{
		Name:        "test",
		Brief:       "brief",
		Age:         18,
		ParentId:    "123",
		Address:     "localhost",
		IpAddress:   netip.MustParseAddr("127.0.0.1"),
		IpAddressV6: netip.MustParseAddr("240e:699:1:4a00:46a8:42ff:fe0b:a675"),
		Prefix:      netip.MustParsePrefix("192.168.0.0/24"),
		PrefixV6:    netip.MustParsePrefix("2001:1000::/80"),
		NetIP:       net.ParseIP("127.0.0.2"),
		NetIPNet:    *netIPNet,
		Street:      "no street",
		Friends:     []string{"friend1", "friend2"},
	}
	r.SetID("test-1")

	var gasses []*GaussResource
	assert.NoError(t, WithTx(store, func(tx Transaction) error {
		if _, err := tx.Delete(TableGaussResource, map[string]interface{}{IDField: r.GetID()}); err != nil {
			return fmt.Errorf("delete failed: %s", err.Error())
		}
		if _, err := tx.Insert(r); err != nil {
			return fmt.Errorf("insert failed: %s", err.Error())
		}
		return tx.Fill(map[string]interface{}{IDField: r.GetID()}, &gasses)
	}))

	assert.Equal(t, len(gasses), 1)
}

func TestFillEx(t *testing.T) {
	store, err := setupGaussResource(GaussConnStr)
	assert.NoError(t, err)

	_, netIPNet, err := net.ParseCIDR("192.168.0.0/24")
	assert.NoError(t, err)
	r := &GaussResource{
		Name:        "test",
		Brief:       "brief",
		Age:         18,
		ParentId:    "123",
		Address:     "localhost",
		IpAddress:   netip.MustParseAddr("127.0.0.1"),
		IpAddressV6: netip.MustParseAddr("240e:699:1:4a00:46a8:42ff:fe0b:a675"),
		Prefix:      netip.MustParsePrefix("192.168.0.0/24"),
		PrefixV6:    netip.MustParsePrefix("2001:1000::/80"),
		NetIP:       net.ParseIP("127.0.0.2"),
		NetIPNet:    *netIPNet,
		Street:      "no street",
		Friends:     []string{"friend1", "friend2"},
	}
	r.SetID("test-1")

	var gasses []*GaussResource
	assert.NoError(t, WithTx(store, func(tx Transaction) error {
		if _, err := tx.Delete(TableGaussResource, map[string]interface{}{IDField: r.GetID()}); err != nil {
			return fmt.Errorf("delete failed: %s", err.Error())
		}

		_, err := tx.Insert(r)
		assert.NoError(t, err)

		assert.NoError(t, tx.FillEx(&gasses, "SELECT * FROM gr_gauss_resource WHERE id = $1", r.GetID()))
		assert.NoError(t, tx.FillEx(&gasses, "SELECT * FROM gr_gauss_resource WHERE name = ANY($1::TEXT[])", []string{"test"}))
		assert.NoError(t, tx.FillEx(&gasses, "SELECT * FROM gr_gauss_resource WHERE ip_address = $1", "127.0.0.1"))
		assert.NoError(t, tx.FillEx(&gasses, "SELECT * FROM gr_gauss_resource WHERE ip_address_v6 = $1", r.IpAddressV6))
		assert.NoError(t, tx.FillEx(&gasses, "SELECT * FROM gr_gauss_resource WHERE prefix = $1", r.Prefix))
		return nil
	}))
}

func TestGaussCopyFrom(t *testing.T) {
	store, err := setupGaussResource(GaussConnStr)
	assert.NoError(t, err)

	_, netIPNet, err := net.ParseCIDR("192.168.0.0/24")
	assert.NoError(t, err)
	var copyValues [][]any
	for i := 0; i < 3; i++ {
		idx := &GaussResource{
			Name:        "name-" + strconv.Itoa(i),
			Brief:       "brief-" + strconv.Itoa(i),
			Age:         i,
			ParentId:    strconv.Itoa(i),
			Address:     "address:" + strconv.Itoa(i),
			Street:      "",
			IpAddress:   netip.MustParseAddr("127.0.0.1"),
			IpAddressV6: netip.MustParseAddr("240e:699:1:4a00:46a8:42ff:fe0b:a675"),
			Prefix:      netip.MustParsePrefix("192.168.0.0/24"),
			PrefixV6:    netip.MustParsePrefix("2001:1000::/80"),
			NetIP:       net.ParseIP("127.0.0.2"),
			NetIPNet:    *netIPNet,
			Friends:     []string{"friend1", "friend2"},
		}

		idx.SetID(strconv.Itoa(i))
		copyValues = append(copyValues, idx.GenCopyValues())
	}

	var gasses []*GaussResource
	assert.NoError(t, WithTx(store, func(tx Transaction) error {
		if _, err := tx.Exec("DELETE FROM gr_gauss_resource"); err != nil {
			return fmt.Errorf("clean table failed: %s", err.Error())
		}

		if _, err := tx.CopyFrom(TableGaussResource, copyValues); err != nil {
			return fmt.Errorf("copy failed: %s", err.Error())
		}
		return tx.Fill(nil, &gasses)
	}))
	assert.Equal(t, len(gasses), len(copyValues))
}

func TestGaussCopyFromEx(t *testing.T) {
	store, err := setupGaussResource(GaussConnStr)
	assert.NoError(t, err)

	_, netIPNet, err := net.ParseCIDR("192.168.0.0/24")
	assert.NoError(t, err)
	var copyValues [][]any
	for i := 0; i < 3; i++ {
		idx := &GaussResource{
			Name:        "name-" + strconv.Itoa(i),
			Brief:       "brief-" + strconv.Itoa(i),
			Age:         i,
			ParentId:    strconv.Itoa(i),
			Address:     "address:" + strconv.Itoa(i),
			Street:      "",
			IpAddress:   netip.MustParseAddr("127.0.0.1"),
			IpAddressV6: netip.MustParseAddr("240e:699:1:4a00:46a8:42ff:fe0b:a675"),
			Prefix:      netip.MustParsePrefix("192.168.0.0/24"),
			PrefixV6:    netip.MustParsePrefix("2001:1000::/80"),
			NetIP:       net.ParseIP("127.0.0.2"),
			NetIPNet:    *netIPNet,
			Friends:     []string{"friend1", "friend2"},
		}

		idx.SetID(strconv.Itoa(i))
		copyValues = append(copyValues, idx.GenCopyValues())
	}

	var gasses []*GaussResource
	SqlColumns := []string{"id", "create_time", "name", "brief", "age",
		"parent_id", "address", "ip_address", "ip_address_v6", "prefix",
		"prefix_v6", "net_i_p", "net_i_p_net", "street", "friends"}
	assert.NoError(t, WithTx(store, func(tx Transaction) error {
		if _, err := tx.Exec("DELETE FROM gr_gauss_resource"); err != nil {
			return fmt.Errorf("clean table failed: %s", err.Error())
		}

		if _, err := tx.CopyFromEx(TableGaussResource, SqlColumns, copyValues); err != nil {
			return fmt.Errorf("copy failed: %s", err.Error())
		}
		return tx.Fill(nil, &gasses)
	}))
	assert.Equal(t, len(gasses), len(copyValues))
}

func TestGaussMultiToMultiRelationship(t *testing.T) {
	meta, err := NewResourceMeta([]resource.Resource{&Mother{}, &Child{}, &MotherChild{}})
	assert.NoError(t, err)
	store, err := setup(GaussConnStr, meta)
	assert.NoError(t, err)

	initChild(store)
	initMother(store)
	initMotherChild(store)

	tx, _ := store.Begin()
	result, err := tx.GetOwned(ResourceType("mother"), "m1", ResourceType("child"))
	assert.NoError(t, err)
	assert.Equal(t, len(result.([]*Child)), 1)
	tx.Rollback()

	//insert unknown mother should fail
	tx, _ = store.Begin()
	_, err = tx.Insert(&MotherChild{
		Mother: "m2",
		Child:  "c1",
	})
	assert.Error(t, err)
	tx.Rollback()

	//delete used child should fail
	tx, _ = store.Begin()
	_, err = tx.Delete("child", map[string]interface{}{
		"name": "ben",
	})
	assert.Error(t, err)
	tx.Rollback()

	store.Clean()
	store.Close()
}

type GaussArray struct {
	resource.ResourceBase `json:",inline"`
	Name                  string      `json:"name" db:"nk"`
	IntArray              []int       `json:"intArray"`
	Uint64Array           []uint64    `json:"uint64Array"`
	Uint32Array           []uint32    `json:"uint32Array"`
	IpArray               []net.IP    `json:"ipArray"`
	IpNetArray            []net.IPNet `json:"netIPNets"`
	Friends               []string    `json:"friends" db:"snk"`
}

func TestGaussArray(t *testing.T) {
	meta, err := NewResourceMeta([]resource.Resource{&GaussArray{}})
	assert.NoError(t, err)
	store, err := setup(GaussConnStr, meta)
	assert.NoError(t, err)

	_, netIPNet, err := net.ParseCIDR("192.168.0.0/24")
	assert.NoError(t, err)
	r := &GaussArray{
		Name:        "test",
		IntArray:    []int{1, 2, 3},
		Uint64Array: []uint64{100, 65535},
		Uint32Array: []uint32{10, 1000},
		IpArray:     []net.IP{net.ParseIP("192.168.0.0"), net.ParseIP("192.168.0.1")},
		IpNetArray:  []net.IPNet{*netIPNet},
		Friends:     []string{"friend1", "friend2"},
	}
	r.SetID("test")

	var gasses []*GaussArray
	assert.NoError(t, WithTx(store, func(tx Transaction) error {
		if _, err := tx.Delete(ResourceDBType(&GaussArray{}), map[string]interface{}{IDField: r.GetID()}); err != nil {
			return fmt.Errorf("delete failed: %s", err.Error())
		}
		if _, err := tx.Insert(r); err != nil {
			return fmt.Errorf("insert failed: %s", err.Error())
		}

		assert.NoError(t, tx.Fill(nil, &gasses))
		assert.NoError(t, tx.FillEx(&gasses, "SELECT * FROM gr_gauss_array WHERE name = ANY($1::TEXT[])", []string{"test"}))
		assert.NoError(t, tx.FillEx(&gasses, "SELECT * FROM gr_gauss_array WHERE $1 = ANY(int_array::bigint[])", 1))
		return nil
	}))
}
