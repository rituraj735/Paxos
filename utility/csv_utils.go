package utility

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

type TxnSet struct {
	SetNumber string
	Txns      []string
	LiveNodes string
}

func ProcessNextTransactionsSet(set TxnSet) {
	fmt.Printf("%+v\n", set)
}

func ParseTxnSetsFromCSV(filePath string) ([]TxnSet, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	if _, err := reader.Read(); err != nil {
		if err == io.EOF {
			return []TxnSet{}, nil
		}
		return nil, fmt.Errorf("couldn't read header: %w", err)
	}

	var entireSets []TxnSet
	var currentSet *TxnSet

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading record: %w", err)
		}

		setNumber := record[0]
		txn := record[1]
		liveNodes := record[2]

		if setNumber != "" {
			if currentSet != nil {
				entireSets = append(entireSets, *currentSet)
			}
			currentSet = &TxnSet{
				SetNumber: setNumber,
				LiveNodes: strings.TrimSpace(liveNodes),
			}
		}

		if currentSet != nil && strings.TrimSpace(txn) != "" {
			currentSet.Txns = append(currentSet.Txns, strings.TrimSpace(txn))
		}
	}
	if currentSet != nil {
		entireSets = append(entireSets, *currentSet)
	}

	return entireSets, nil
}
