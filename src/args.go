package src

import (
	"errors"
	"flag"
	"os"

	"github.com/lncrespo/dynamodbtools/src/purge"
)

const flagInt = 0
const flagString = 1
const flagBool = 2

type Subcommand struct {
	name       string
	flags      []Flag
	flagValues map[string]interface{}
	flagset    *flag.FlagSet
	entryFunc  func(flagVals map[string]interface{}) error
}

type Flag struct {
	flagType    int
	short       string
	long        string
	description string
}

var subcommands []Subcommand
var subcmdMap map[string]int

func init() {
	subcommands = []Subcommand{
		{
			name: "purge",
			flags: []Flag{
				{flagType: flagString, short: "h", long: "help", description: "Invoke usage"},
			},
			entryFunc: purge.Purge,
		},
	}

	registerSubcommands()

	subcmdMap = buildSubcmdMap(subcommands)
}

func registerSubcommands() {
	for subcmdIndex := range subcommands {
		subcommand := &subcommands[subcmdIndex]
		flagset := flag.NewFlagSet(subcommand.name, flag.ExitOnError)
		subcommand.flagValues = make(map[string]interface{})
		subcommand.flagset = flagset

		for _, flag := range subcommand.flags {
			switch flag.flagType {
			case flagInt:
				subcommand.flagValues[flag.long] = flagset.Int(flag.long, 0, "")

				if val, ok := subcommand.flagValues[flag.long].(int); ok {
					flagset.IntVar(&val, flag.short, 0, "")
				}

			case flagString:
				subcommand.flagValues[flag.long] = flagset.String(flag.long, "", "")

				if val, ok := subcommand.flagValues[flag.long].(string); ok {
					flagset.StringVar(&val, flag.short, "", "")
				}

			case flagBool:
				subcommand.flagValues[flag.long] = flagset.Bool(flag.long, false, "")

				if val, ok := subcommand.flagValues[flag.long].(bool); ok {
					flagset.BoolVar(&val, flag.short, false, "")
				}
			}
		}
	}
}

func buildSubcmdMap(subcmds []Subcommand) map[string]int {
	subcmdMap := make(map[string]int)

	for i, subcmd := range subcmds {
		subcmdMap[subcmd.name] = i
	}

	return subcmdMap
}

func parseSubcommand(subcmd string) (*Subcommand, error) {
	index, ok := subcmdMap[subcmd]

	if !ok {
		return nil, errors.New("Unknown subcommand")
	}

	subcommands[index].flagset.Parse(os.Args[2:])

	return &subcommands[index], nil
}
