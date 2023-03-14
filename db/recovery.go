package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type recovery struct {
	PgIsInRecovery bool
}

func DBIsRecoveryMode(pool *pgxpool.Pool) (bool, error) {
	rows, err := pool.Query(context.TODO(), "select pg_is_in_recovery()")
	if err != nil {
		return false, err
	}

	var rs []*recovery
	for rows.Next() {
		var r recovery
		if err := rows.Scan(&r.PgIsInRecovery); err != nil {
			return false, err
		} else {
			rs = append(rs, &r)
		}
	}

	return len(rs) == 1 && rs[0].PgIsInRecovery, nil
}
