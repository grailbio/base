package main

import (
	"flag"
	"fmt"

	"github.com/grailbio/base/config"
	"github.com/grailbio/base/must"
)

type (
	Fruit  interface{ IsFruit() }
	Apple  struct{ color string }
	Orange struct{}
)

func (Apple) IsFruit()  {}
func (Orange) IsFruit() {}

func init() {
	config.Register("fruits/apple-red", func(c *config.Constructor[Apple]) {
		c.Doc = "Some people like apples."
		c.New = func() (Apple, error) { return Apple{"red"}, nil }
	})
	config.Register("fruits/apple-green", func(c *config.Constructor[Apple]) {
		c.Doc = "Another apple."
		c.New = func() (Apple, error) { return Apple{"green"}, nil }
	})
	config.Register("fruits/orange", func(c *config.Constructor[Orange]) {
		c.Doc = "Some people like oranges."
		c.New = func() (Orange, error) { return Orange{}, nil }
	})
	config.Register("favorite", func(c *config.Constructor[Fruit]) {
		c.Doc = "My favorite fruit."
		var favorite Fruit
		c.InstanceVar(&favorite, "is", "favorite-apple", "Favorite fruit?")
		c.New = func() (Fruit, error) { return favorite, nil }
	})
	config.Register("favorite-apple", func(c *config.Constructor[Apple]) {
		c.Doc = "My favorite apple."
		var favorite Apple
		c.InstanceVar(&favorite, "is", "fruits/apple-green", "Favorite apple?")
		c.New = func() (Apple, error) { return favorite, nil }
	})
}

func main() {
	config.RegisterFlags("", "")
	flag.Parse()
	must.Nil(config.ProcessFlags())

	var fruit Fruit
	must.Nil(config.Instance("favorite", &fruit))
	fmt.Printf("My favorite fruit is %#v.\n", fruit)

	var apple Apple
	must.Nil(config.Instance("favorite-apple", &apple))
	fmt.Printf("My favorite apple is %#v.\n", apple)
}
