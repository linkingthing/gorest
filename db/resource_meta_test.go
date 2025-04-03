package db

import (
	"context"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	ut "github.com/linkingthing/cement/unittest"
	"github.com/linkingthing/gorest/resource"
)

func TestNewResourceMeta(t *testing.T) {
	type Student struct {
		resource.ResourceBase
		Name      string
		Ip        netip.Addr
		IpsP      []*netip.Addr
		Ips       []netip.Addr
		Prefix    netip.Prefix
		PrefixesP []*netip.Prefix
		Prefixes  []netip.Prefix
		Inets     []*net.IPNet
	}

	_, ipnet1, _ := net.ParseCIDR("2001:1000::/64")
	_, ipnet2, _ := net.ParseCIDR("2001:2000::/64")
	addr1 := netip.MustParseAddr("192.168.100.1")
	prefix := netip.MustParsePrefix("192.168.100.0/24")
	var students = []*Student{
		{
			Name:      "joker",
			Ip:        netip.MustParseAddr("192.168.100.1"),
			IpsP:      []*netip.Addr{&addr1},
			Ips:       []netip.Addr{addr1},
			Prefix:    netip.MustParsePrefix("192.168.100.0/24"),
			PrefixesP: []*netip.Prefix{&prefix},
			Prefixes:  []netip.Prefix{prefix},
			Inets:     []*net.IPNet{ipnet2, ipnet1}},
	}

	meta, err := NewResourceMeta([]resource.Resource{&Student{}})
	ut.Assert(t, err == nil, "")

	store, err := NewPGStore(ConnStr, meta)
	ut.Assert(t, err == nil, "err str is %v", err)

	tx, _ := store.Begin()
	defer func() {
		tx.Exec("drop table gr_student")
		tx.Commit()
	}()

	_, err = tx.Insert(students[0])
	ut.Assert(t, err == nil, "insert failed:%v", err)

	var queryStudents []*Student
	err = tx.Fill(nil, &queryStudents)
	ut.Assert(t, err == nil, "fill failed:%v", err)

	t.Logf("student:%+v", queryStudents[0])
}

func TestBatchInsert(t *testing.T) {
	type Student struct {
		resource.ResourceBase
		Name    string
		Age     int
		Address netip.Addr
	}

	batchNum := 10000
	students := make([]*Student, 0, batchNum)
	copyValues := make([][]interface{}, 0, batchNum)
	for i := 0; i < batchNum; i++ {
		students = append(students, &Student{
			Name:    strconv.Itoa(i),
			Age:     i,
			Address: netip.MustParseAddr("192.168.100.1"),
		})

	}

	meta, err := NewResourceMeta([]resource.Resource{&Student{}})
	ut.Assert(t, err == nil, "")

	pool, err := pgxpool.New(context.Background(), ConnStr)
	if err != nil {
		t.Error(err)
		return
	}
	defer pool.Close()

	//tx, _ := pool.Begin(context.TODO())
	//defer func() {
	//	//tx.Exec(context.TODO(), "drop table gr_student")
	//	tx.Commit(context.TODO())
	//}()

	//for _, descriptor := range meta.GetDescriptors() {
	//	if _, err := pool.Exec(context.TODO(), createTableSql(descriptor)); err != nil {
	//		t.Error(err)
	//		return
	//	}
	//}

	baseTx := NewBaseTx(meta, DefaultSchemaName)
	batch := &pgx.Batch{}
	for _, r := range students {
		r.SetCreationTimestamp(time.Now())
		sql, args, err := baseTx.insertSqlArgsAndID(r)
		if err != nil {
			t.Error(err)
			return
		}
		batch.Queue(sql, args...)

		copyValues = append(copyValues, []interface{}{
			r.GetID(),
			time.Now(),
			r.Name,
			r.Age,
			r.Address,
		})
	}

	//batch insert
	//begin := time.Now()
	//br := pool.SendBatch(context.TODO(), batch)
	//for i := 0; i < len(students); i++ {
	//	_, err := br.Exec()
	//	if err != nil {
	//		t.Error(err)
	//		return
	//	}
	//}
	//br.Close()
	//t.Logf("insert %d  effectRows %d time: %s", batchNum, batchNum, time.Now().Sub(begin))
	//insert 1000000  effectRows 1 time: 2m31.68629418s
	//insert 100000  effectRows 1 time: 12.145371662s
	//insert 10000  effectRows 1 time: 916.096859ms

	//copy
	begin := time.Now()
	descriptor, err := meta.GetDescriptor(ResourceDBType(&Student{}))
	if err != nil {
		t.Error(err)
		return
	}
	c, err := pool.CopyFrom(context.Background(),
		pgx.Identifier{DefaultSchemaName, getTableNameWithoutSchema(descriptor.Typ)},
		[]string{"id", "create_time", "name", "age", "address"}, pgx.CopyFromRows(copyValues))
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("insert %d  effectRows %d time: %s", batchNum, c, time.Now().Sub(begin))
	////insert 1000000  effectRows 1000000 time: 18.564405286s
	////insert 100000  effectRows 100000 time: 1.398430575s
	////insert 10000  effectRows 10000 time: 108.018819ms
}
