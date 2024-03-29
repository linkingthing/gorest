package validator

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/linkingthing/gorest/util"
)

const minPrefix = "min="
const maxPrefix = "max="

type intRangeValidator struct {
	min *int64
	max *int64
}
type intRangeValidatorBuilder struct{}

func newIntRangeValidator(min, max *int64) Validator {
	return &intRangeValidator{
		min: min,
		max: max,
	}
}

func (v *intRangeValidator) Validate(val interface{}) error {
	value := reflect.ValueOf(val)
	kind := util.Inspect(value.Type())
	switch kind {
	case util.Int:
		return v.validateValueRange(value.Int())
	case util.Uint:
		return v.validateValueRange(int64(value.Uint()))
	default:
		return fmt.Errorf("int range apply to non-int type:%v", kind)
	}
}

func (v *intRangeValidator) validateValueRange(i int64) error {
	if v.min != nil && i < *v.min {
		return fmt.Errorf("exceed the range limit, (%v should >= %v)", i, *v.min)
	}

	if v.max != nil && i >= *v.max {
		return fmt.Errorf("exceed the range limit, (%v should < %v)", i, *v.max)
	}
	return nil
}

func (b *intRangeValidatorBuilder) FromTags(tags []string) (Validator, error) {
	var minStr, maxStr string
	for _, tag := range tags {
		if strings.HasPrefix(tag, minPrefix) {
			if minStr != "" {
				return nil, fmt.Errorf("int range has duplicate min tag")
			}
			minStr = strings.TrimPrefix(tag, minPrefix)
		} else if strings.HasPrefix(tag, maxPrefix) {
			if maxStr != "" {
				return nil, fmt.Errorf("int range has duplicate max tag")
			}
			maxStr = strings.TrimPrefix(tag, maxPrefix)
		}
	}

	if minStr == "" && maxStr == "" {
		return nil, nil
	}

	var min, max *int64
	if minStr != "" {
		min_, err := strconv.ParseInt(minStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("min value isn't valid int:%s", err.Error())
		}
		min = &min_
	}

	if maxStr != "" {
		max_, err := strconv.ParseInt(maxStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("max value isn't valid int:%s", err.Error())
		}
		max = &max_
	}

	if min != nil && max != nil && *min >= *max {
		return nil, fmt.Errorf("min value should smaller than max")
	}
	return newIntRangeValidator(min, max), nil
}

func (b *intRangeValidatorBuilder) SupportKind(kind util.Kind) bool {
	return kind == util.Int ||
		kind == util.Uint ||
		kind == util.IntSlice ||
		kind == util.UintSlice ||
		kind == util.StringIntMap ||
		kind == util.StringUintMap
}
