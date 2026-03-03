package main

import (
	"fmt"
	"os"

	"golang.org/x/term"
)

func main() {
	fmt.Printf("cachekv - v0.0.1\n")

	prompt := "Enter password: "
	fd := int(os.Stdin.Fd())
	if term.IsTerminal(fd) {
		fmt.Println("-- Terminal detected")
		fmt.Print(prompt)
		bPasswd, err := term.ReadPassword(fd)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		fmt.Printf("\n-- Given password: %s\n", string(bPasswd))
	} else {
		fmt.Println("-- Non-terminal detected. Exiting...")
		os.Exit(1)
	}
}
