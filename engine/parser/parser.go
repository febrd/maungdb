package parser

import (
	"errors"
	"strings"
)

func Parse(input string) (*Command, error) {
	tokens := strings.Fields(input)
	if len(tokens) < 2 {
		return nil, errors.New("query teu valid")
	}

	switch tokens[0] {
	case "simpen":
		return parseInsert(tokens)
	case "tingali":
		return parseSelect(tokens)
	default:
		return nil, errors.New("paréntah teu dikenal")
	}
}

func parseInsert(tokens []string) (*Command, error) {
	if len(tokens) < 3 {
		return nil, errors.New("format simpen salah")
	}

	return &Command{
		Type:  CmdInsert,
		Table: tokens[1],
		Data:  tokens[2],
	}, nil
}

func parseSelect(tokens []string) (*Command, error) {
	cmd := &Command{
		Type:  CmdSelect,
		Table: tokens[1],
	}

	if len(tokens) > 2 {
		if tokens[2] != "dimana" || len(tokens) < 6 {
			return nil, errors.New("format dimana salah")
		}

		cmd.Where = &Condition{
			Field:    tokens[3],
			Operator: tokens[4],
			Value:    tokens[5],
		}
	}

	return cmd, nil
}
