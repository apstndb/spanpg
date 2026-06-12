package spanpg_test

import (
	"encoding/json"
	"math/big"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spancodec"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/apstndb/spanpg"
)

// pgNumericWire is the canonical wire string PGNumericValue produces for 3.14
// (spanner.NumericString pads to 9 fractional digits).
const pgNumericWire = "3.140000000"

func TestEncodeOptionsValueOf(t *testing.T) {
	t.Parallel()

	rat := big.NewRat(314, 100)
	tests := []struct {
		name  string
		input any
		want  spanner.GenericColumnValue
	}{
		{"big.Rat", *rat, gcvctor.StringBasedValueOf(typector.PGNumeric(), pgNumericWire)},
		{"*big.Rat", rat, gcvctor.StringBasedValueOf(typector.PGNumeric(), pgNumericWire)},
		{"*big.Rat nil", (*big.Rat)(nil), gcvctor.NullOf(typector.PGNumeric())},
		{"NullNumeric valid", spanner.NullNumeric{Numeric: *rat, Valid: true},
			gcvctor.StringBasedValueOf(typector.PGNumeric(), pgNumericWire)},
		{"NullNumeric invalid", spanner.NullNumeric{}, gcvctor.NullOf(typector.PGNumeric())},
		{"NullJSON valid object", spanner.NullJSON{Value: map[string]any{"k": 1.0}, Valid: true},
			gcvctor.StringBasedValueOf(typector.PGJSONB(), `{"k":1}`)},
		// Marshal-always semantics mirroring the client: a Go string Value
		// becomes a quoted JSON string on the wire...
		{"NullJSON string value", spanner.NullJSON{Value: "foo", Valid: true},
			gcvctor.StringBasedValueOf(typector.PGJSONB(), `"foo"`)},
		// ...and wire-format JSON text must be passed as json.RawMessage.
		// json.Marshal compacts the RawMessage (whitespace dropped) but keeps the
		// digits exact: no float64 round-trip corrupting large integers.
		{"NullJSON RawMessage", spanner.NullJSON{Value: json.RawMessage(`{"n": 12345678901234567890}`), Valid: true},
			gcvctor.StringBasedValueOf(typector.PGJSONB(), `{"n":12345678901234567890}`)},
		{"NullJSON invalid", spanner.NullJSON{}, gcvctor.NullOf(typector.PGJSONB())},

		// Slices: per spancodec's contract a WithValueEncoder registration for T
		// applies per element of []T, and WithGoType supplies the ARRAY element
		// type for nil and empty slices.
		{"[]big.Rat", []big.Rat{*rat},
			gcvctor.MustArrayValueOf(typector.PGNumeric(),
				gcvctor.StringBasedValueOf(typector.PGNumeric(), pgNumericWire))},
		{"[]*big.Rat with nil element", []*big.Rat{rat, nil},
			gcvctor.MustArrayValueOf(typector.PGNumeric(),
				gcvctor.StringBasedValueOf(typector.PGNumeric(), pgNumericWire),
				gcvctor.NullOf(typector.PGNumeric()))},
		{"nil []big.Rat", []big.Rat(nil), gcvctor.NullArrayOf(typector.PGNumeric())},
		{"empty []big.Rat", []big.Rat{}, gcvctor.EmptyArrayOf(typector.PGNumeric())},
		{"[]spanner.NullJSON", []spanner.NullJSON{{Value: map[string]any{"k": 1.0}, Valid: true}, {}},
			gcvctor.MustArrayValueOf(typector.PGJSONB(),
				gcvctor.StringBasedValueOf(typector.PGJSONB(), `{"k":1}`),
				gcvctor.NullOf(typector.PGJSONB()))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := spancodec.ValueOf(tt.input, spanpg.EncodeOptions()...)
			if err != nil {
				t.Fatalf("ValueOf(%v): %v", tt.input, err)
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("ValueOf(%v) (-want +got):\n%s", tt.input, diff)
			}
		})
	}
}

// TestEncodeOptionsPGWrapperPassThrough pins that spanner.PGNumeric and
// spanner.PGJsonB are NOT registered by EncodeOptions: they already encode
// with the PG annotations via the spancodec client mirror, so the result is
// identical with and without the options.
func TestEncodeOptionsPGWrapperPassThrough(t *testing.T) {
	t.Parallel()

	inputs := []any{
		spanner.PGNumeric{Numeric: "3.14", Valid: true},
		spanner.PGNumeric{},
		spanner.PGNumeric{Numeric: "NaN", Valid: true},
		spanner.PGJsonB{Value: map[string]any{"k": 1.0}, Valid: true},
		spanner.PGJsonB{},
	}
	for _, input := range inputs {
		plain, err := spancodec.ValueOf(input)
		if err != nil {
			t.Fatalf("ValueOf(%v): %v", input, err)
		}
		adapted, err := spancodec.ValueOf(input, spanpg.EncodeOptions()...)
		if err != nil {
			t.Fatalf("ValueOf(%v, EncodeOptions()...): %v", input, err)
		}
		if diff := cmp.Diff(plain, adapted, protocmp.Transform()); diff != "" {
			t.Errorf("ValueOf(%v) with EncodeOptions diverged from client mirror (-mirror +adapted):\n%s", input, diff)
		}
	}
}

func TestEncodeOptionsTypeFor(t *testing.T) {
	t.Parallel()

	for name, fn := range map[string]func() (any, error){
		"big.Rat":     func() (any, error) { return spancodec.TypeFor[big.Rat](spanpg.EncodeOptions()...) },
		"*big.Rat":    func() (any, error) { return spancodec.TypeFor[*big.Rat](spanpg.EncodeOptions()...) },
		"NullNumeric": func() (any, error) { return spancodec.TypeFor[spanner.NullNumeric](spanpg.EncodeOptions()...) },
		"[]big.Rat":   func() (any, error) { return spancodec.TypeFor[[]big.Rat](spanpg.EncodeOptions()...) },
		"NullJSON":    func() (any, error) { return spancodec.TypeFor[spanner.NullJSON](spanpg.EncodeOptions()...) },
		"[]NullJSON":  func() (any, error) { return spancodec.TypeFor[[]spanner.NullJSON](spanpg.EncodeOptions()...) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := fn()
			if err != nil {
				t.Fatalf("TypeFor: %v", err)
			}
			var want any
			switch name {
			case "big.Rat", "*big.Rat", "NullNumeric":
				want = typector.PGNumeric()
			case "[]big.Rat":
				want = typector.ElemTypeToArrayType(typector.PGNumeric())
			case "NullJSON":
				want = typector.PGJSONB()
			case "[]NullJSON":
				want = typector.ElemTypeToArrayType(typector.PGJSONB())
			}
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Errorf("TypeFor[%s] (-want +got):\n%s", name, diff)
			}
		})
	}
}
