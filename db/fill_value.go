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
		return stringtool.ToSnake(key) + " = ANY($" + strconv.Itoa(markerSeq) + ")", f.Value, nil
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
