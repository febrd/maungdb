package parser

type CommandType string

const (
	CmdCreate CommandType = "CREATE"
	CmdInsert CommandType = "INSERT"
	CmdSelect CommandType = "SELECT"
	CmdUpdate CommandType = "UPDATE"
	CmdDelete CommandType = "DELETE"
)

type Command struct {
	Type    CommandType
	Table   string
	Data    string    
	Updates map[string]string 
	Where   []Condition

	OrderBy   string 
	OrderDesc bool   
	Limit     int   
	Offset    int    
}

type Condition struct {
	Field    string
	Operator string
	Value    string
	LogicOp  string
}