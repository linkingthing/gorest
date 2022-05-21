package schema

import "testing"

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
	}

	for _, tt := range data {
		if err := ValidateString(tt); err != nil {
			t.Errorf("ValidateString() error = %v ", err)
		}
	}
}
