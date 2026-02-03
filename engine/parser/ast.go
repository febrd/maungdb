package parser

type CommandType string

const (
	CmdInsert CommandType = "INSERT"
	CmdSelect CommandType = "SELECT"
)

type Command struct {
	Type  CommandType
	Table string
	Data  string      
	Where *Condition 
}

type Condition struct {
	Field    string
	Operator string
	Value    string
}
