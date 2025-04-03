package db

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"strconv"
	"strings"

	pq "gitee.com/opengauss/openGauss-connector-go-pq"
)

// PQArray is implement from pq Array, And add Int Uint32 Uint64 net.IP net.IPNet support
func PQArray(a interface{}) interface {
	driver.Valuer
	sql.Scanner
} {
	switch a := a.(type) {
	case []bool:
		return (*pq.BoolArray)(&a)
	case []float64:
		return (*pq.Float64Array)(&a)
	case []float32:
		return (*pq.Float32Array)(&a)
	case []int64:
		return (*pq.Int64Array)(&a)
	case []int32:
		return (*pq.Int32Array)(&a)
	case []string:
		return (*pq.StringArray)(&a)
	case [][]byte:
		return (*pq.ByteaArray)(&a)
	case *[]bool:
		return (*pq.BoolArray)(a)
	case *[]float64:
		return (*pq.Float64Array)(a)
	case *[]float32:
		return (*pq.Float32Array)(a)
	case *[]int64:
		return (*pq.Int64Array)(a)
	case *[]int32:
		return (*pq.Int32Array)(a)
	case *[]string:
		return (*pq.StringArray)(a)
	case *[][]byte:
		return (*pq.ByteaArray)(a)
	case []int:
		return (*IntArray)(&a)
	case *[]int:
		return (*IntArray)(a)
	case []uint32:
		return (*Uint32Array)(&a)
	case *[]uint32:
		return (*Uint32Array)(a)
	case []uint64:
		return (*Uint64Array)(&a)
	case *[]uint64:
		return (*Uint64Array)(a)
	case []net.IP:
		return (*IPArray)(&a)
	case *[]net.IP:
		return (*IPArray)(a)
	case []net.IPNet:
		return (*IPNetArray)(&a)
	case *[]net.IPNet:
		return (*IPNetArray)(a)
	}

	return pq.GenericArray{A: a}
}

type IntArray []int

func (a IntArray) Value() (driver.Value, error) {
	if len(a) == 0 {
		return "{}", nil
	}

	strArr := make([]string, len(a))
	for i, v := range a {
		strArr[i] = strconv.Itoa(v)
	}
	return "{" + strings.Join(strArr, ",") + "}", nil
}

func (a *IntArray) Scan(src interface{}) error {
	if src == nil {
		*a = nil
		return nil
	}

	var int64Arr pq.Int64Array
	if err := int64Arr.Scan(src); err != nil {
		return err
	}

	intArr := make([]int, len(int64Arr))
	for i, v := range int64Arr {
		intArr[i] = int(v)
	}

	*a = intArr
	return nil
}

type Uint64Array []uint64

func (a Uint64Array) Value() (driver.Value, error) {
	if len(a) == 0 {
		return "{}", nil
	}

	strArr := make([]string, len(a))
	for i, v := range a {
		strArr[i] = strconv.FormatUint(v, 10)
	}
	return "{" + strings.Join(strArr, ",") + "}", nil
}

func (a *Uint64Array) Scan(src interface{}) error {
	if src == nil {
		*a = nil
		return nil
	}

	var int64Arr pq.Int64Array
	if err := int64Arr.Scan(src); err != nil {
		return err
	}

	// 转换 []int64 -> []uint64
	uint64Arr := make([]uint64, len(int64Arr))
	for i, v := range int64Arr {
		if v < 0 {
			return fmt.Errorf("invalid value: expected unsigned integer but got negative %d", v)
		}
		uint64Arr[i] = uint64(v)
	}

	*a = uint64Arr
	return nil
}

// Uint32Array represents a one-dimensional array of the PostgreSQL integer types.
type Uint32Array []uint32

// Scan implements the sql.Scanner interface.
func (a *Uint32Array) Scan(src interface{}) error {
	if src == nil {
		*a = nil
		return nil
	}

	var int32Arr pq.Int32Array
	if err := int32Arr.Scan(src); err != nil {
		return err
	}

	uint32Arr := make([]uint32, len(int32Arr))
	for i, v := range int32Arr {
		if v < 0 {
			return fmt.Errorf("invalid value: expected unsigned integer but got negative %d", v)
		}
		uint32Arr[i] = uint32(v)
	}

	*a = uint32Arr
	return nil
}

// Value implements the driver.Valuer interface.
func (a Uint32Array) Value() (driver.Value, error) {
	if len(a) == 0 {
		return "{}", nil
	}

	strArr := make([]string, len(a))
	for i, v := range a {
		strArr[i] = strconv.FormatUint(uint64(v), 10)
	}
	return "{" + strings.Join(strArr, ",") + "}", nil
}

type IPArray []net.IP

func (a IPArray) Value() (driver.Value, error) {
	if len(a) == 0 {
		return "{}", nil
	}

	strArr := make([]string, len(a))
	for i, ip := range a {
		strArr[i] = fmt.Sprintf("\"%s\"", ip.String())
	}
	return "{" + strings.Join(strArr, ",") + "}", nil
}

func (a *IPArray) Scan(src interface{}) error {
	if src == nil {
		*a = nil
		return nil
	}

	var strArr pq.StringArray
	if err := strArr.Scan(src); err != nil {
		return err
	}

	ipArr := make(IPArray, len(strArr))
	for i, s := range strArr {
		ip := net.ParseIP(s)
		if ip == nil {
			return fmt.Errorf("invalid IP format: %s", s)
		}
		ipArr[i] = ip
	}

	*a = ipArr
	return nil
}

type IPNetArray []net.IPNet

func (a IPNetArray) Value() (driver.Value, error) {
	if len(a) == 0 {
		return "{}", nil
	}

	strArr := make([]string, len(a))
	for i, ipNet := range a {
		strArr[i] = fmt.Sprintf("\"%s\"", ipNet.String())
	}
	return "{" + strings.Join(strArr, ",") + "}", nil
}

func (a *IPNetArray) Scan(src interface{}) error {
	if src == nil {
		*a = nil
		return nil
	}

	var strArr pq.StringArray
	if err := strArr.Scan(src); err != nil {
		return err
	}

	ipNetArr := make(IPNetArray, len(strArr))
	for i, s := range strArr {
		_, ipNet, err := net.ParseCIDR(s)
		if err != nil {
			return fmt.Errorf("invalid CIDR format: %s", s)
		}
		ipNetArr[i] = *ipNet
	}

	*a = ipNetArr
	return nil
}
