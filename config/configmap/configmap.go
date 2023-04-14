package configmap

import (
	"fmt"
	"sort"

	"github.com/grailbio/base/config"
	"golang.org/x/exp/maps"
)

func Register[Key ~string, Elem any, Map ~map[Key]Elem](
	name string,
	def map[Key]string,
	doc string,
	_ ...Map,
) {
	elemName := func(i int) string {
		if i == 0 {
			return name
		}
		return fmt.Sprintf("%s/%d", name, i)
	}
	keys := maps.Keys(def)
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for i, key := range keys {
		i, key, inst := i, key, def[key]
		config.RegisterGen(elemName(i), func(c *config.ConstructorGen[Map]) {
			var (
				keyString = string(key)
				data      Elem
				tail      Map
			)
			c.StringVar(&keyString, "key", keyString, "")
			c.InstanceVar(&data, "data", inst, "")
			var next string
			if i < len(def)-1 {
				next = elemName(i + 1)
			}
			c.InstanceVar(&tail, "next", next, "")
			c.New = func() (Map, error) {
				// Map construction time complexity is quadratic in length.
				// See configlist.
				m := make(Map, len(tail)+1)
				m[Key(keyString)] = data
				maps.Copy(m, tail) // Conflicting tail keys override this key.
				return m, nil
			}
		})
	}
}
