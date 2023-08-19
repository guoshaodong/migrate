package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"

	"powerlaw.ai/powerlib/migrate"
	"powerlaw.ai/powerlib/migrate/concrete"
)

func main() {
	db, err := sql.Open("mysql", "username:password@tcp(localhost:3306)/controller")
	if err != nil {
		panic(err)
	}

	goExecutor := concrete.NewGoExecutor([]concrete.GoHandler{
		concrete.NewGoHandler(2, func(ctx context.Context) error {
			// 这是方法 2
			fmt.Println("222")
			return nil
		}),
		concrete.NewGoHandler(3, func(ctx context.Context) error {
			// 这是方法 3
			fmt.Println("333")
			return nil
		}),
	}...)
	sqlExecutor := concrete.NewSQLExecutor(db, "./test/migration")

	client := migrate.New(db, migrate.WithTableName("schema_migrations"),
		migrate.WithExecutors([]migrate.Executor{goExecutor, sqlExecutor}...))
	err = client.Run(context.Background())
	if err != nil {
		fmt.Printf("%+v", err)
	}
}
