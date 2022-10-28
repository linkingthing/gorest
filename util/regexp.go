package util

import (
	"fmt"
	"regexp"
)

type NameRegexp struct {
	Regexp       *regexp.Regexp
	ErrMsg       string
	ExpectResult bool
}

var NameRegs = []*NameRegexp{
	{
		Regexp:       regexp.MustCompile(`^[0-9a-zA-Z-_/:.,\[\]()（）\s\p{Han}@*]+$`),
		ErrMsg:       "is not legal",
		ExpectResult: true,
	},
	{
		Regexp:       regexp.MustCompile(`(^-)|(^/)|(^,)|(^，)|(^、)`),
		ErrMsg:       "is not legal",
		ExpectResult: false,
	},
	{
		Regexp:       regexp.MustCompile(`-$|_$|/$|，$|、$|,$`),
		ErrMsg:       "is not legal",
		ExpectResult: false,
	},
}

func ValidateString(s string) error {
	if s != "" {
		for _, reg := range NameRegs {
			if ret := reg.Regexp.MatchString(s); ret != reg.ExpectResult {
				return fmt.Errorf("%s %s", s, reg.ErrMsg)
			}
		}
	}

	return nil
}
