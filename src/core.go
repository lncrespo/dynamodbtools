package src

import (
	"fmt"
	"os"
)

const usage = `
usage: ddbtools [subcommand]
`

func Run() {
	if len(os.Args) == 1 {
		panic("missing subcommand")
	}

	subcmd, err := parseSubcommand(os.Args[1])

	if err != nil {
		errorWithUsage(err)
	}

	err = subcmd.entryFunc(subcmd.flagValues)

	if err != nil {
		panic(err)
	}
}

func errorWithUsage(err error) {
	fmt.Fprintln(os.Stderr, err)
	fmt.Fprintln(os.Stderr, usage)

	os.Exit(1)
}
