package configlist_test

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/grailbio/base/config"
	"github.com/grailbio/base/config/configlist"
	"github.com/grailbio/base/must"
)

type (
	Fruit      interface{ IsFruit() }
	Apple      struct{}
	Orange     struct{}
	Strawberry struct{}
	Favorites  []Fruit
)

func (Apple) IsFruit()      {}
func (Orange) IsFruit()     {}
func (Strawberry) IsFruit() {}

func (fs Favorites) String() string {
	var names []string
	for _, f := range fs {
		names = append(names, reflect.TypeOf(f).Name())
	}
	return strings.Join(names, ", ")
}

func init() {
	config.RegisterGen("fruits/apple", func(c *config.ConstructorGen[Apple]) {
		c.Doc = "Some people like apples."
		c.New = func() (Apple, error) { return Apple{}, nil }
	})
	config.RegisterGen("fruits/orange", func(c *config.ConstructorGen[Orange]) {
		c.Doc = "Some people like oranges."
		c.New = func() (Orange, error) { return Orange{}, nil }
	})
	config.RegisterGen("fruits/strawberry", func(c *config.ConstructorGen[Strawberry]) {
		c.Doc = "Some people like strawberries."
		c.New = func() (Strawberry, error) { return Strawberry{}, nil }
	})
	configlist.Register(
		"apple-best",
		[]string{"fruits/apple", "fruits/orange"},
		"My favorite fruits.",
		Favorites(nil),
	)
	config.Default("favorites", "apple-best")
}

func Example_usage() {
	var favorites Favorites
	must.Nil(config.Instance("favorites", &favorites))
	fmt.Printf("Old favorites: %v\n", favorites)

	modified := config.New()
	must.Nil(modified.Parse(strings.NewReader(`
// Create a new favorites list with strawberry first.
instance dont-like-apples apple-best (
	data = fruits/strawberry
	next = apple-best/1 // Skip apple, go straight to orange.
)
instance favorites dont-like-apples
`)))
	favorites = nil
	must.Nil(modified.Instance("favorites", &favorites))
	fmt.Printf("New favorites: %v\n", favorites)

	// Output:
	// Old favorites: Apple, Orange
	// New favorites: Strawberry, Orange
}
