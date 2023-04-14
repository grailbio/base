package configmap_test

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/grailbio/base/config"
	"github.com/grailbio/base/config/configmap"
	"github.com/grailbio/base/must"
	"golang.org/x/exp/maps"
)

type (
	Fruit      interface{ IsFruit() }
	Apple      struct{}
	Orange     struct{}
	Strawberry struct{}

	Person string

	Favorites map[Person]Fruit
)

func (Apple) IsFruit()      {}
func (Orange) IsFruit()     {}
func (Strawberry) IsFruit() {}

func (fs Favorites) String() string {
	var parts []string
	for _, person := range maps.Keys(fs) {
		fruit := reflect.TypeOf(fs[person]).Name()
		parts = append(parts, fmt.Sprintf("%s: %s", person, fruit))
	}
	return strings.Join(parts, ", ")
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
	configmap.Register(
		"nominative-determinism",
		map[Person]string{"Aaron": "fruits/apple", "Othello": "fruits/orange"},
		"My friends' favorite fruits.",
		Favorites{},
	)
	config.Default("favorites", "nominative-determinism")
}

func Example_usage() {
	var favorites Favorites
	must.Nil(config.Instance("favorites", &favorites))
	fmt.Printf("Old favorites: %v\n", favorites)

	modified := config.New()
	modified.Parse(strings.NewReader(`
	// Create a new favorites list with strawberry first.
	instance fruit-independence nominative-determinism (
		key = "Aaron"
		data = fruits/strawberry
		next = nominative-determinism/1 // Skip apple, go straight to orange.
	)
	instance favorites fruit-independence
	`))
	favorites = nil
	must.Nil(modified.Instance("favorites", &favorites))
	fmt.Printf("New favorites: %v\n", favorites)

	// Output:
	// Old favorites: Aaron: Apple, Othello: Orange
	// New favorites: Aaron: Strawberry, Othello: Orange
}
