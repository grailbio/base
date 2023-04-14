package configlist

import (
	"fmt"

	"github.com/grailbio/base/config"
)

func Register[Elem any, List ~[]Elem](name string, def []string, doc string, _ ...List) {
	elemName := func(i int) string {
		if i == 0 {
			return name
		}
		return fmt.Sprintf("%s/%d", name, i)
	}
	for i, inst := range def {
		i, inst := i, inst
		config.RegisterGen(elemName(i), func(c *config.ConstructorGen[List]) {
			var (
				data Elem
				tail List
			)
			c.InstanceVar(&data, "data", inst, "")
			var next string
			if i < len(def)-1 {
				next = elemName(i + 1)
			}
			c.InstanceVar(&tail, "next", next, "")
			c.New = func() (List, error) {
				// List construction time complexity is quadratic in length.
				// We could avoid this by registering both a "list" and a "node" type, where the
				// list simply points to the initial node. However, this is visible to users;
				// they must define the "list" and "node" instances themselves if they want to
				// create a new list, and then arrange the indirections correctly. We expect
				// profile lists to be short, in general, such that the quadratic work is not an
				// issue, so we choose to do this to simplify UX.
				return append(List{data}, tail...), nil
			}
		})
	}
}
