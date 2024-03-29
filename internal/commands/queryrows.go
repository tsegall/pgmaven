package commands

import (
	"database/sql"
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
	"time"
)

type QueryRows struct {
	datasource *dbutils.DataSource
}

func (c *QueryRows) Init(context utils.Context, ds *dbutils.DataSource) {
	c.datasource = ds
}

func (c *QueryRows) Execute(args ...string) {
	query := utils.OptionallyFromFile(args...)
	err := c.datasource.ExecuteQueryRows(query, nil, dump, c)
	if err != nil {
		log.Printf("ERROR: Database: %s, Query '%s' failed, error: %v\n", c.datasource.GetDBName(), args[0], err)
	}
}

func dump(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, sefl any) {
	if rowNumber == 1 {
		for i, columnType := range columnTypes {
			if i != 0 {
				fmt.Print("\t")
			}
			fmt.Print(columnType.Name())
		}
		fmt.Println()
	}
	for i := 0; i < len(values); i++ {
		if i != 0 {
			fmt.Print("\t")
		}
		printValue(values[i].(*interface{}))
	}
	fmt.Println()
}

func printValue(pval *interface{}) {
	switch v := (*pval).(type) {
	case nil:
		fmt.Print("NULL")
	case bool:
		if v {
			fmt.Print("1")
		} else {
			fmt.Print("0")
		}
	case []byte:
		fmt.Print(string(v))
	case time.Time:
		fmt.Print(v.Format("2006-01-02 15:04:05.999"))
	default:
		fmt.Print(v)
	}
}
