package operation

// Op is the definition of the mongo command to run
type Op struct {
	ID string
	// Valid types are: 'insert', 'update' or 'remove'
	Type string
	// The namespace as defined by mongo. For example, "clever.events"
	Namespace   string
	EncodedBson string
}
