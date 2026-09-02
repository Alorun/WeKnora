package provider

func init() {
	for _, modelProvider := range BuiltinProviders() {
		if err := Publish(modelProvider); err != nil {
			panic(err)
		}
	}
}
