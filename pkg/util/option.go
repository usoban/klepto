package option

// Option type.
type Option struct {
	isSome bool
	value interface{}
}

func Some(value interface{}) *Option {
	return &Option{true, value}
}

func None() *Option {
	return &Option{false, nil}
}

func IsSome(option *Option) bool {
	return option.isSome
}

func Value(option *Option) interface{} {
	if option.isSome == false {
		panic("Option does not have a value.")
	}

	return option.value
}

