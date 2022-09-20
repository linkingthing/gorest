package util

import (
	"testing"
)

func TestValidateString(t *testing.T) {
	data := []string{
		"default",
		"test123",
		"123",
		"你好",
		"Ab6e_a",
		"xx@ff",
		"@",
		"@xx",
		"xx@",
		"vv*",
		"*vv",
		"*",
		"xx,xx",
		"x-x",
		"x_x",
		"enable",
		"true",
		"2001::/64",
		"2001::1330",
		"2001::",
		"abc/24f",
		"10.0.0.0/24",
		"10.0.0.1",
		"www.baidu.com",
		"www.baidu.com.",
		".",
		"[2001::5]:53",
		"[2002::5]",
		"[]",
		"[",
		"]",
	}

	for _, tt := range data {
		if err := ValidateString(tt); err != nil {
			t.Errorf("ValidateString() error = %v ", err)
		}
	}
}
