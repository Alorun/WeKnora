package embedding

import "github.com/Tencent/WeKnora/internal/models/provider"

func init() {
	for _, modelProvider := range provider.BuiltinProviders() {
		if err := provider.Publish(modelProvider); err != nil {
			panic(err)
		}
	}
}
