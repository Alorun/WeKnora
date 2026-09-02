package catalog

import (
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/plugin/control"
)

func definition(id control.PluginID, source control.SourceType) control.PluginDefinition {
	return control.PluginDefinition{
		ID: id, Name: string(id), Version: "1.0.0", Source: source,
		ExtensionID: "sample", ExtensionType: control.ExtensionDataSource,
		Capabilities: []string{"one"},
	}
}

func TestCatalogRejectsDuplicatesAndBuiltinOverride(t *testing.T) {
	catalog := New()
	if err := catalog.Register(definition("builtin.sample", control.SourceBuiltin)); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Register(definition("builtin.sample", control.SourceBuiltin)); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate error = %v", err)
	}
	if err := catalog.Register(definition("builtin.sample", control.SourceExternal)); err == nil {
		t.Fatalf("override error = %v", err)
	}
}

func TestCatalogReturnsDefensiveCopies(t *testing.T) {
	catalog := New()
	original := definition("community.sample", control.SourceExternal)
	if err := catalog.Register(original); err != nil {
		t.Fatal(err)
	}
	got, err := catalog.Get(original.ID)
	if err != nil {
		t.Fatal(err)
	}
	got.Capabilities[0] = "mutated"
	again, _ := catalog.Get(original.ID)
	if again.Capabilities[0] != "one" {
		t.Fatal("caller mutated catalog-owned definition")
	}
}
