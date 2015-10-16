package operation

<<<<<<< HEAD
import "gopkg.in/mgo.v2/bson"

=======
>>>>>>> Move operation into its own package
// Op is the definition of the mongo command to run
type Op struct {
	ID string
	// Valid types are: 'insert', 'update' or 'remove'
	Type string
	// The namespace as defined by mongo. For example, "clever.events"
<<<<<<< HEAD
	Namespace string
	Obj       bson.M
=======
	Namespace   string
	EncodedBson string
>>>>>>> Move operation into its own package
}
