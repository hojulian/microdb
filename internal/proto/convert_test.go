package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestUnMarshalValues(t *testing.T) {
	testCases := []struct {
		desc string
		vs   []*Value
		exp  []interface{}
	}{
		{
			desc: "simple row",
			vs: []*Value{
				{
					TypedValue: &Value_Varchar{
						Varchar: "value1",
					},
				},
				{
					TypedValue: &Value_Varchar{
						Varchar: "value2",
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 123,
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 456,
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 789,
					},
				},
			},
			exp: []interface{}{
				"value1",
				"value2",
				int64(123),
				int64(456),
				int64(789),
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			act := UnmarshalValues(tC.vs)
			assert.Equal(t, tC.exp, act, "unequal values")
		})
	}
}

func TestMarshalValues(t *testing.T) {
	testCases := []struct {
		desc string
		is   []interface{}
		exp  []*Value
	}{
		{
			desc: "simple row",
			is: []interface{}{
				"value1",
				"value2",
				int64(123),
				int64(456),
				int64(789),
				nil,
			},
			exp: []*Value{
				{
					TypedValue: &Value_Varchar{
						Varchar: "value1",
					},
				},
				{
					TypedValue: &Value_Varchar{
						Varchar: "value2",
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 123,
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 456,
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 789,
					},
				},
				{
					TypedValue: &Value_Null{},
				},
			},
		},
		{
			desc: "row with all types",
			is: []interface{}{
				"value1",
				123,
				int8(123),
				int16(123),
				int32(123),
				int64(123),
				uint(123),
				uint16(123),
				uint32(123),
				uint64(123),
				123.456,
				float64(123.456),
				true,
				nil,
				time.Unix(0, 1),
			},
			exp: []*Value{
				{
					TypedValue: &Value_Varchar{
						Varchar: "value1",
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 123,
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 123,
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 123,
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 123,
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 123,
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 123,
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 123,
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 123,
					},
				},
				{
					TypedValue: &Value_Integer{
						Integer: 123,
					},
				},
				{
					TypedValue: &Value_Decimal{
						Decimal: 123.456,
					},
				},
				{
					TypedValue: &Value_Decimal{
						Decimal: 123.456,
					},
				},
				{
					TypedValue: &Value_Boolean{
						Boolean: true,
					},
				},
				{
					TypedValue: &Value_Null{},
				},
				{
					TypedValue: &Value_Timestamp{
						Timestamp: timestamppb.New(time.Unix(0, 1)),
					},
				},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			act := MarshalValues(tC.is)
			assert.Equal(t, tC.exp, act, "unequal values")
		})
	}
}
