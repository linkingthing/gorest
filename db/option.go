package db

import "fmt"

type Option func(ResourceStore)

func WithSchema(schema string) Option {
	return func(r ResourceStore) {
		r.SetSchema(schema)
	}
}

func WithDebug(debug bool) Option {
	return func(r ResourceStore) {
		SetDebug(debug)
	}
}

func WithDropPublicSchema(dropSchemas ...string) Option {
	return func(r ResourceStore) {
		if err := r.DropSchemas(dropSchemas...); err != nil {
			panic(err)
		}
	}
}

var showLog bool

func SetDebug(t bool) {
	showLog = t
}

func logSql(query string, args ...any) {
	if showLog {
		fmt.Printf("*** exec sql: %s args:%s\n", query, args)
	}
}
