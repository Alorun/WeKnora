package docparser

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/infrastructure/docparser/anydoc"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// stubRemote stands in for the docreader client. NewReader only ever hands it
// back, so the embedded interface is never called — and would panic if routing
// ever sent a parse here by mistake.
type stubRemote struct{ interfaces.DocReader }

func TestNewReaderRoutesByEngine(t *testing.T) {
	ctx := context.Background()
	remote := &stubRemote{}
	deps := ReaderDeps{Remote: remote}

	cases := []struct {
		name     string
		engine   string
		fileType string
		isURL    bool
		want     any
	}{
		{name: "simple engine", engine: SimpleEngineName, fileType: "md", want: &SimpleFormatReader{}},
		{name: "builtin engine goes remote", engine: BuiltinEngineName, fileType: "md", want: remote},
		{name: "unset engine handles simple formats in Go", fileType: "csv", want: &SimpleFormatReader{}},
		{name: "unset engine sends complex formats to docreader", fileType: "docx", want: remote},
		{name: "default pptx engine is remote markitdown", engine: "markitdown", fileType: "pptx", want: remote},
		{name: "URLs always go to docreader", fileType: "md", isURL: true, want: remote},
		{name: "docreader-only engines fall through", engine: "markitdown", fileType: "docx", want: remote},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := NewReader(ctx, tc.engine, tc.fileType, tc.isURL, deps)
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			for { // Inspect the selected implementation behind lifecycle routing.
				managed, ok := reader.(*activeEngineReader)
				if !ok {
					break
				}
				reader = managed.inner
			}
			if _, isSimple := tc.want.(*SimpleFormatReader); isSimple {
				if _, ok := reader.(*SimpleFormatReader); !ok {
					t.Fatalf("reader = %T, want *SimpleFormatReader", reader)
				}
				return
			}
			if reader != tc.want {
				t.Fatalf("reader = %T, want the docreader client", reader)
			}
		})
	}
}

func TestManagedFallbackRejectsStoppedBridge(t *testing.T) {
	remote := &recordingDocReader{}
	reader := managedFallbackReader(BuiltinEngineName, remote)
	if reader == nil {
		t.Fatal("managed fallback was not built for a ready bridge")
	}
	if err := UnregisterEngine(BuiltinEngineName); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, engine := range BuiltinEngineRegistrations() {
			if engine.Name() == BuiltinEngineName {
				_ = PublishEngine(engine)
				return
			}
		}
	})
	if _, err := reader.Read(context.Background(), &types.ReadRequest{}); err == nil {
		t.Fatal("fallback invoked a stopped DocReader bridge")
	}
	if remote.calls != 0 {
		t.Fatalf("stopped bridge received %d calls", remote.calls)
	}
}

type recordingDocReader struct{ calls int }

func (r *recordingDocReader) Read(context.Context, *types.ReadRequest) (*types.ReadResult, error) {
	r.calls++
	return &types.ReadResult{}, nil
}

// The anydoc engine is only linked into builds tagged `anydoc`; everywhere
// else it must be listed as unavailable and refuse to build a reader, so a
// knowledge base configured for it fails loudly instead of parsing with
// something else.
func TestAnydocEngineFollowsBuildAvailability(t *testing.T) {
	reader, err := NewReader(context.Background(), AnydocEngineName, "docx", false, ReaderDeps{})

	if anydoc.Available() {
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		if _, ok := reader.(*activeEngineReader).inner.(*AnydocReader); !ok {
			t.Fatalf("reader = %T, want *AnydocReader", reader)
		}
		return
	}
	if err == nil {
		t.Fatal("NewReader succeeded without the converter linked in, want an error")
	}
}

func TestListAllEnginesIncludesAnydoc(t *testing.T) {
	for _, engine := range ListAllEngines(true, nil, nil) {
		if engine.Name != AnydocEngineName {
			continue
		}
		if engine.Available != anydoc.Available() {
			t.Errorf("anydoc availability = %v, want %v", engine.Available, anydoc.Available())
		}
		if !engine.Available && engine.UnavailableReason == "" {
			t.Error("anydoc is unavailable without a reason to show")
		}
		if len(engine.FileTypes) == 0 {
			t.Error("anydoc lists no file types")
		}
		return
	}
	t.Fatal("anydoc engine not found in the engine list")
}
