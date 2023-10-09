package db

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/linkingthing/cement/stringtool"
)

type FillValue struct {
	Operator Operator
	Value    any
}

type Operator string

const (
	OperatorEq                = "="
	OperatorNe                = "!="
	OperatorLt                = "<"
	OperatorLte               = "<="
	OperatorGt                = ">"
	OperatorGte               = ">="
	OperatorLike              = "%like%"
	OperatorLikeSuffix        = "like%"
	OperatorLikePrefix        = "%like"
	OperatorAny               = "any"
	OperatorOverlap           = "&&"
	OperatorSubnetContain     = ">>"
	OperatorSubnetContainEq   = ">>="
	OperatorSubnetContainBy   = "<<"
	OperatorSubnetContainEqBy = "<<="
)

func (f FillValue) buildSql(key string, markerSeq int) (string, any, error) {
	switch f.Operator {
	case OperatorNe:
		return stringtool.ToSnake(key) + " != $" + strconv.Itoa(markerSeq), f.Value, nil
	case OperatorLt:
		return stringtool.ToSnake(key) + " < $" + strconv.Itoa(markerSeq), f.Value, nil
	case OperatorLte:
		return stringtool.ToSnake(key) + " <= $" + strconv.Itoa(markerSeq), f.Value, nil
	case OperatorGt:
		return stringtool.ToSnake(key) + " > $" + strconv.Itoa(markerSeq), f.Value, nil
	case OperatorGte:
		return stringtool.ToSnake(key) + " >= $" + strconv.Itoa(markerSeq), f.Value, nil
	case OperatorLike:
		return stringtool.ToSnake(key) + " ~ $" + strconv.Itoa(markerSeq), f.Value, nil
	case OperatorLikeSuffix:
		if sv, ok := f.Value.(string); ok == true {
			return stringtool.ToSnake(key) + " ~ $" + strconv.Itoa(markerSeq), "^" + sv, nil
		} else {
			return "", nil, fmt.Errorf("match condition isn't string, but %v", f.Value)
		}
	case OperatorLikePrefix:
		if sv, ok := f.Value.(string); ok == true {
			return stringtool.ToSnake(key) + " ~ $" + strconv.Itoa(markerSeq), sv + "$", nil
		} else {
			return "", nil, fmt.Errorf("match condition isn't string, but %v", f.Value)
		}
	case OperatorAny:
		if t := reflect.TypeOf(f.Value); t.Kind() != reflect.Slice {
			return "", nil, fmt.Errorf("any value should be slice, but %v", f.Value)
		}
		v := reflect.ValueOf(f.Value)
		var fKind = reflect.String
		var typStr string
		if v.Len() > 0 {
			fKind = v.Index(0).Kind()
			typStr = v.Index(0).Type().String()
		}

		switch fKind {
		case reflect.Int8, reflect.Int16, reflect.Uint8, reflect.Uint16, reflect.Int32:
			return stringtool.ToSnake(key) + " = ANY($" + strconv.Itoa(markerSeq) + "::integer[])", f.Value, nil
		case reflect.Int, reflect.Uint32, reflect.Int64:
			return stringtool.ToSnake(key) + " = ANY($" + strconv.Itoa(markerSeq) + "::bigint[])", f.Value, nil
		case reflect.Uint, reflect.Uint64:
			return stringtool.ToSnake(key) + " = ANY($" + strconv.Itoa(markerSeq) + "::numeric[])", f.Value, nil
		case reflect.Float32:
			return stringtool.ToSnake(key) + " = ANY($" + strconv.Itoa(markerSeq) + "::float4[])", f.Value, nil
		case reflect.Bool:
			return stringtool.ToSnake(key) + " = ANY($" + strconv.Itoa(markerSeq) + "::boolean[])", f.Value, nil
		case reflect.Struct:
			if typStr == "net.IPNet" || typStr == "netip.Addr" || typStr == "netip.Prefix" {
				return stringtool.ToSnake(key) + " = ANY($" + strconv.Itoa(markerSeq) + "::inet[])", f.Value, nil
			}
			return stringtool.ToSnake(key) + " = ANY($" + strconv.Itoa(markerSeq) + "::TEXT[])", f.Value, nil
		default:
			return stringtool.ToSnake(key) + " = ANY($" + strconv.Itoa(markerSeq) + "::TEXT[])", f.Value, nil
		}
	case OperatorOverlap:
		if t := reflect.TypeOf(f.Value); t.Kind() != reflect.Slice {
			return "", nil, fmt.Errorf("any value should be slice, but %v", f.Value)
		}
		return stringtool.ToSnake(key) + " && $" + strconv.Itoa(markerSeq), f.Value, nil
	case OperatorSubnetContain:
		return stringtool.ToSnake(key) + " >> $" + strconv.Itoa(markerSeq), f.Value, nil
	case OperatorSubnetContainEq:
		return stringtool.ToSnake(key) + " >>= $" + strconv.Itoa(markerSeq), f.Value, nil
	case OperatorSubnetContainBy:
		return stringtool.ToSnake(key) + " << $" + strconv.Itoa(markerSeq), f.Value, nil
	case OperatorSubnetContainEqBy:
		return stringtool.ToSnake(key) + " <<= $" + strconv.Itoa(markerSeq), f.Value, nil
	default:
		return stringtool.ToSnake(key) + " = $" + strconv.Itoa(markerSeq), f.Value, nil
	}
}
