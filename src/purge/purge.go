package purge

import (
	"errors"
	"fmt"

	"github.com/lncrespo/dynamodbtools/src/aws"
)

const errMissingTableName = "Missing parameter \"table-name\""

func Purge(flagVals map[string]interface{}) error {
	tableName, err := getTableName(flagVals)

	if err != nil {
		return err
	}

	table := aws.Table{TableName: tableName}

	deleteCount, err := table.Purge()

	fmt.Printf("Deleted %d items\n", deleteCount)

	return err
}

func getTableName(flagVals map[string]interface{}) (string, error) {
	tableName := ""

	if val, ok := flagVals["table-name"].(*string); ok {
		tableName = *val
	}

	if tableName == "" {
		return "", errors.New(errMissingTableName)
	}

	return tableName, nil
}
