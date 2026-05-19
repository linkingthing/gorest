package generator

import (
	"strings"
	"unicode"
)

type Names struct {
	Raw     string // user_order
	Pascal  string // UserOrder
	Camel   string // userOrder
	Snake   string // user_order
	PkgName string
	Plural  string
}

func NewNames(input string) Names {
	words := splitToWords(input)
	pascal := toPascal(words)
	camel := toCamel(words)
	snake := strings.Join(words, "_")
	pkgName := strings.Join(words, "")

	return Names{
		Raw:     input,
		Pascal:  pascal,
		Camel:   camel,
		Snake:   snake,
		PkgName: pkgName,
		Plural:  snake + "s",
	}
}

func splitToWords(s string) []string {
	if strings.Contains(s, "_") {
		parts := strings.Split(s, "_")
		result := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				result = append(result, strings.ToLower(p))
			}
		}
		return result
	}

	var words []string
	var current []rune
	for i, r := range []rune(s) {
		if unicode.IsUpper(r) && i > 0 {
			if len(current) > 0 {
				words = append(words, strings.ToLower(string(current)))
			}
			current = []rune{r}
		} else {
			current = append(current, r)
		}
	}
	if len(current) > 0 {
		words = append(words, strings.ToLower(string(current)))
	}
	return words
}

func toPascal(words []string) string {
	var sb strings.Builder
	for _, w := range words {
		if len(w) > 0 {
			sb.WriteString(strings.ToUpper(w[:1]) + w[1:])
		}
	}
	return sb.String()
}

func toCamel(words []string) string {
	if len(words) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(words[0])
	for _, w := range words[1:] {
		if len(w) > 0 {
			sb.WriteString(strings.ToUpper(w[:1]) + w[1:])
		}
	}
	return sb.String()
}
