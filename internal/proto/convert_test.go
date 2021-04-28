package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			act := MarshalValues(tC.is)
			assert.Equal(t, tC.exp, act, "unequal values")
		})
	}
}
