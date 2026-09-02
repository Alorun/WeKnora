package docparser

func init() {
	for _, engine := range BuiltinEngineRegistrations() {
		if err := PublishEngine(engine); err != nil {
			panic(err)
		}
	}
}
