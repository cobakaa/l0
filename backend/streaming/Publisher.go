package main

import (
	"fmt"
	stan "github.com/nats-io/stan.go"
	"io"
	"os"
)

func main() {

	sc, err := stan.Connect("test-cluster", "pub")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if len(os.Args) < 2 {
		fmt.Println("Usage: [program name] [filenames]")
		return
	}

	for _, filename := range os.Args[1:] {
		stat, e := os.Stat(filename)
		if e != nil {
			fmt.Println(e.Error())
			continue
		}
		if stat.IsDir() {
			fmt.Println("File can't be directory")
			continue
		}
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		fileContent, err := io.ReadAll(file)
		if err != nil {
			file.Close()
			fmt.Println(err.Error())
			continue
		}
		file.Close()

		sc.Publish("order", fileContent)

	}

	sc.Close()
}
