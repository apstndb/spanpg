package spanpg

import (
	"math/big"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spancodec"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
)

// EncodeOptions returns [github.com/apstndb/spancodec] options that adapt
// encoding to the PostgreSQL dialect: NUMERIC-family and JSON-family Go values
// produce PG_NUMERIC / PG_JSONB annotated GCVs instead of the GoogleSQL forms.
// Pass them to [github.com/apstndb/spancodec.ValueOf],
// [github.com/apstndb/spancodec.RowTypeFor],
// [github.com/apstndb/spancodec.NewRowEncoder], and friends.
//
// The adaptation is required, not cosmetic: the POSTGRESQL dialect rejects
// query parameters whose type lacks the annotation. Probe
// (integration/pgtypeannotation, emulator 1.5.54): binding a
// GenericColumnValue param with plain code:NUMERIC fails with
// codes.Unimplemented "Unsupported GoogleSQL Type: NUMERIC", and plain
// code:JSON fails with "Unsupported GoogleSQL Type: JSON"; the same values
// with the PG_NUMERIC / PG_JSONB annotation are accepted and the result
// column metadata echoes the annotation.
//
// Registered mappings (each [github.com/apstndb/spancodec.WithValueEncoder]
// is paired with [github.com/apstndb/spancodec.WithGoType] so static
// inference — TypeFor, RowTypeFor, RowEncoder.RowType / ResultSetMetadata —
// carries the annotations too):
//
//   - [math/big.Rat] and *[math/big.Rat] (nil pointer → typed NULL
//     PG_NUMERIC) → [github.com/apstndb/spanvalue/gcvctor.PGNumericValue]
//   - [cloud.google.com/go/spanner.NullNumeric] → PGNumericValue when Valid,
//     typed NULL PG_NUMERIC otherwise
//
// PGNumericValue formats the wire string with the client's canonical
// [cloud.google.com/go/spanner.NumericString] (9 fractional digits, silently
// rounding) even though PG numeric accepts a wider value space. Because these
// registrations override the client mirror,
// [github.com/apstndb/spancodec.WithLossOfPrecisionHandling] does not apply
// to them; pass values needing more fractional digits (or NaN) as
// [cloud.google.com/go/spanner.PGNumeric] wire strings instead.
//   - [cloud.google.com/go/spanner.NullJSON] →
//     [github.com/apstndb/spanvalue/gcvctor.PGJSONBValue] of Value when
//     Valid, typed NULL PG_JSONB otherwise. Like the client, Value is always
//     marshaled: a Go string Value becomes a quoted JSON string on the wire;
//     pass wire-format JSON text as [encoding/json.RawMessage].
//
// [cloud.google.com/go/spanner.PGNumeric] and
// [cloud.google.com/go/spanner.PGJsonB] already encode with the annotations
// via the client mirror and are deliberately NOT registered; they pass
// through unchanged with or without these options.
//
// Hazard model (same as spancodec's): a registration overrides ALL built-in
// handling for its exact dynamic type only. Named types (e.g. type MyRat
// big.Rat) and other NUMERIC-capable inputs such as string-based wire values
// are untouched. Per spancodec's contract a registration for T also applies
// per element of []T, and the paired WithGoType supplies the ARRAY element
// type for nil and empty slices, so []big.Rat, []*big.Rat,
// []spanner.NullNumeric, and []spanner.NullJSON encode as arrays of
// PG-annotated elements. The slice types themselves are additionally
// registered with WithGoType: static inference resolves an exact Go type
// (the client supports []big.Rat etc. natively) before falling back to a
// registered element type, so without the slice registrations TypeFor[[]T]
// would report un-annotated ARRAY element types.
//
// Decoding needs no counterpart: the client (and therefore
// [github.com/apstndb/spancodec.Decode] /
// [github.com/apstndb/spancodec.ToStruct]) checks only the TypeCode for
// big.Rat, NullNumeric, and NullJSON destinations, so PG_NUMERIC / PG_JSONB
// columns decode into them natively (pinned in integration/pgtypeannotation).
// The one PG-specific decode hazard is the value space, not the type:
// PG_NUMERIC columns can hold "NaN", which no big.Rat-based destination can
// represent (the client fails with "unexpected string value"); use
// [cloud.google.com/go/spanner.PGNumeric] when NaN is possible.
func EncodeOptions() []spancodec.EncodeOption {
	pgNumericArray := typector.ElemTypeToArrayType(typector.PGNumeric())
	pgJSONBArray := typector.ElemTypeToArrayType(typector.PGJSONB())
	return []spancodec.EncodeOption{
		spancodec.WithValueEncoder(func(v big.Rat) (spanner.GenericColumnValue, error) {
			return gcvctor.PGNumericValue(&v), nil
		}),
		spancodec.WithGoType[big.Rat](typector.PGNumeric()),
		spancodec.WithGoType[[]big.Rat](pgNumericArray),

		spancodec.WithValueEncoder(func(v *big.Rat) (spanner.GenericColumnValue, error) {
			if v == nil {
				return gcvctor.NullOf(typector.PGNumeric()), nil
			}
			return gcvctor.PGNumericValue(v), nil
		}),
		spancodec.WithGoType[*big.Rat](typector.PGNumeric()),
		spancodec.WithGoType[[]*big.Rat](pgNumericArray),

		spancodec.WithValueEncoder(func(v spanner.NullNumeric) (spanner.GenericColumnValue, error) {
			if !v.Valid {
				return gcvctor.NullOf(typector.PGNumeric()), nil
			}
			return gcvctor.PGNumericValue(&v.Numeric), nil
		}),
		spancodec.WithGoType[spanner.NullNumeric](typector.PGNumeric()),
		spancodec.WithGoType[[]spanner.NullNumeric](pgNumericArray),

		spancodec.WithValueEncoder(func(v spanner.NullJSON) (spanner.GenericColumnValue, error) {
			if !v.Valid {
				return gcvctor.NullOf(typector.PGJSONB()), nil
			}
			return gcvctor.PGJSONBValue(v.Value)
		}),
		spancodec.WithGoType[spanner.NullJSON](typector.PGJSONB()),
		spancodec.WithGoType[[]spanner.NullJSON](pgJSONBArray),
	}
}
