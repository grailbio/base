package config

// selfConfig configures Profile using itself.
type selfConfig struct {
	strictParams bool
}

const selfInstance = "config"

func init() {
	RegisterGen(selfInstance, func(constr *ConstructorGen[selfConfig]) {
		var cfg selfConfig
		constr.BoolVar(&cfg.strictParams, "strict-params", false,
			"If true, forbid extra (unrecognized) parameters for configuring an instance, "+
				"instead failing at instance construction time.")
		constr.New = func() (selfConfig, error) { return cfg, nil }
	})
}
