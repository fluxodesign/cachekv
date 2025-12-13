package main

import (
	"fmt"
	"os"

	"golang.org/x/term"
)

func main() {
	fmt.Printf("cachekv - v0.0.1\n")

	fmt.Printf("Enter password: ")
	passwd, err := term.ReadPassword(int(os.Stdin.Fd()))
	/*if term.IsTerminal(syscall.Stdin) {
		fd = syscall.Stdin
	} else {
		tty, err := os.Open("/dev/pts/1")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		defer func(tty *os.File) {
			e := tty.Close()
			if e != nil {
				fmt.Println(e.Error())
				os.Exit(1)
			}
		}(tty)
		fd = int(tty.Fd())
	}
	bytePasswd, err := term.ReadPassword(fd)*/
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	fmt.Printf("-- Given password: %s\n", passwd)
}
