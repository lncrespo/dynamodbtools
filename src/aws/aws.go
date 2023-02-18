package aws

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Table struct {
	TableName  string
	PrimaryKey *KeyPair
}

type KeyPair struct {
	PartitionKey string
	SortKey      *string
}

var client *dynamodb.Client

func init() {
	cfg, err := config.LoadDefaultConfig(context.Background())

	if err != nil {
		panic(err)
	}

	client = dynamodb.NewFromConfig(cfg)
}

func (keyPair KeyPair) toDynamoDbApiKey(primaryKey KeyPair) (map[string]types.AttributeValue, error) {
	marshalledPartitionKey, err := attributevalue.Marshal(keyPair.PartitionKey)

	if err != nil {
		return nil, err
	}

	keyPairMap := map[string]types.AttributeValue{
		primaryKey.PartitionKey: marshalledPartitionKey,
	}

	if primaryKey.SortKey != nil {
		marshalledSortKey, err := attributevalue.Marshal(keyPair.SortKey)

		if err != nil {
			return nil, err
		}

		keyPairMap[*primaryKey.SortKey] = marshalledSortKey
	}

	return keyPairMap, nil
}

func (table *Table) Scan() ([]KeyPair, int32, error) {
	response, err := client.Scan(context.Background(), &dynamodb.ScanInput{
		TableName: &table.TableName,
	})

	if err != nil {
		return nil, 0, err
	}

	scannedItemCount := response.Count
	lastEvaluatedKey := response.LastEvaluatedKey
	scannedItemKeys := []KeyPair{}

	scannedItemKeys = appendChunkToKeyArray(*table.PrimaryKey, scannedItemKeys, response.Items)

	for {
		if lastEvaluatedKey == nil {
			break
		}

		response, err := client.Scan(context.Background(), &dynamodb.ScanInput{
			TableName:         &table.TableName,
			ExclusiveStartKey: lastEvaluatedKey,
		})

		if err != nil {
			return nil, 0, err
		}

		lastEvaluatedKey = response.LastEvaluatedKey
		scannedItemCount += response.Count

		scannedItemKeys = appendChunkToKeyArray(*table.PrimaryKey, scannedItemKeys, response.Items)
	}

	return scannedItemKeys, scannedItemCount, nil
}

func (table *Table) Purge() (int32, error) {
	err := table.AssignKeySchema()

	if err != nil {
		return 0, err
	}

	itemKeys, itemCount, err := table.Scan()

	if err != nil {
		return 0, err
	}

	wg := &sync.WaitGroup{}

	chunkedRecords := chunkRecords(itemKeys, 25)

	for _, chunkedRecord := range chunkedRecords {
		wg.Add(1)

		go deleteChunk(wg, table.TableName, *table.PrimaryKey, chunkedRecord)
	}

	wg.Wait()

	return itemCount, nil
}

func (table *Table) AssignKeySchema() error {
	info, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
		TableName: &table.TableName,
	})

	if err != nil {
		return err
	}

	keys := KeyPair{}

	for _, key := range info.Table.KeySchema {
		switch key.KeyType {
		case types.KeyTypeHash:
			keys.PartitionKey = *key.AttributeName

		case types.KeyTypeRange:
			keys.SortKey = key.AttributeName
		}
	}

	table.PrimaryKey = &keys

	return nil
}

func appendChunkToKeyArray(
	primaryKey KeyPair,
	keys []KeyPair,
	chunk []map[string]types.AttributeValue,
) []KeyPair {
	appendedKeys := keys

	if primaryKey.SortKey == nil {
		for _, record := range chunk {
			keyPair := KeyPair{}

			attributevalue.Unmarshal(record[primaryKey.PartitionKey], &keyPair.PartitionKey)

			appendedKeys = append(appendedKeys, keyPair)
		}

		return appendedKeys
	}

	for _, record := range chunk {
		keyPair := KeyPair{}

		attributevalue.Unmarshal(record[primaryKey.PartitionKey], &keyPair.PartitionKey)
		attributevalue.Unmarshal(record[*primaryKey.SortKey], &keyPair.SortKey)

		appendedKeys = append(appendedKeys, keyPair)
	}

	return appendedKeys
}

func chunkRecords(records []KeyPair, chunkSize int) [][]KeyPair {
	chunkedRecords := [][]KeyPair{}

	for i := 0; i < len(records); i += chunkSize {
		chunkEnd := i + chunkSize

		if chunkEnd > len(records) {
			chunkEnd = len(records)
		}

		chunkedRecords = append(chunkedRecords, records[i:chunkEnd])
	}

	return chunkedRecords
}

func deleteChunk(wg *sync.WaitGroup, tableName string, primaryKey KeyPair, chunk []KeyPair) {
	writeRequests := []types.WriteRequest{}

	for _, record := range chunk {
		apiKeyPair, err := record.toDynamoDbApiKey(primaryKey)

		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}

		writeRequests = append(writeRequests, types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{Key: apiKeyPair},
		})
	}

	_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: writeRequests,
		},
	})

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

	wg.Done()
}
