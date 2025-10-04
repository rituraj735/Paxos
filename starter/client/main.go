package main

import (
	"bufio"
	"fmt"
	"log"
	utilities "multipaxos/rituraj735/utility"
	"os"
	"strings"
)

func main() {
	fmt.Println("Welcome to Bank of Paxos")
	fmt.Println("==========================")
	var option int
	var fileName string
	fmt.Println("Please enter the test file path to start: ")
	filePathReader := bufio.NewReader(os.Stdin)
	filePath, err := filePathReader.ReadString('\n')
	if err != nil {
		log.Fatalf("Failed to read file path: %v", err)
	}
	filePath = strings.TrimSpace(filePath)
	sets, err := utilities.ParseTxnSetsFromCSV(filePath)
	currentSetIdx := 0
	if err != nil {
		log.Fatalf("Failed to parse CSV file: %v", err)
	}

	//write the logic of after reading the file here later
	fmt.Scanln(&fileName)
	fmt.Println("Choose an option:")
	for {
		if option == 6 {
			break
		}
		fmt.Println("1.Process next transactions set")
		fmt.Println("2.PrintLog")
		fmt.Println("3.PrintDB")
		fmt.Println("4.PrintStatus")
		fmt.Println("5.PrintView")
		fmt.Println("6.Exit")
		fmt.Scanln(&option)
		fmt.Println("You chose option:", option)
		switch option {
		case 1:
			if currentSetIdx >= len(sets) {
				fmt.Println("No more sets remaining to process.")
			} else {
				utilities.ProcessNextTransactionsSet(sets[currentSetIdx])
				currentSetIdx++
			}
		case 2:
			fmt.Printf("%+v\n", sets)
			fmt.Println("PrintLog()")
		case 3:
			fmt.Println("PrintDB()")
		case 4:
			fmt.Println("PrintStatus()")
		case 5:
			fmt.Println("PrintView()")
		case 6:
			fmt.Println("Exiting...")
		default:
			fmt.Println("Invalid option. Please try again.")
		}
	}

}
