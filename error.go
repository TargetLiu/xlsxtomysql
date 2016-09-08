package main

import (
	"fmt"
	"os"
)

func checkerr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
