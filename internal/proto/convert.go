// Package proto contains protobuf files and functions for MicroDB protocol
package proto

import (
	"database/sql/driver"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// Protobuf conversions

var _ driver.Result = &DriverResult{}

// MarshalValues marshals an array of any Go types into MicroDB value types.
func MarshalValues(is []interface{}) []*Value {
	vs := make([]*Value, 0, len(is))
	for _, i := range is {
		v := MarshalValue(i)
		vs = append(vs, v)
	}
	return vs
}

// MarshalDriverValues marshals an array of database driver values into MicroDB value types.
func MarshalDriverValues(args []driver.NamedValue) []*Value {
	vs := make([]*Value, 0, len(args))
	for _, a := range args {
		v := MarshalValue(a.Value)
		if v != nil {
			vs = append(vs, v)
		}
	}
	return vs
}

// MarshalValue marshals any Go type into a MicroDB value type.
//nolint // Allow longer method accounts for all data types.
func MarshalValue(i interface{}) *Value {
	switch v := i.(type) {
	case string:
		return &Value{
			TypedValue: &Value_Varchar{
				Varchar: v,
			},
		}

	case int:
		return &Value{
			TypedValue: &Value_Integer{
				Integer: int64(v),
			},
		}

	case int8:
		return &Value{
			TypedValue: &Value_Integer{
				Integer: int64(v),
			},
		}

	case int16:
		return &Value{
			TypedValue: &Value_Integer{
				Integer: int64(v),
			},
		}

	case int32:
		return &Value{
			TypedValue: &Value_Integer{
				Integer: int64(v),
			},
		}

	case int64:
		return &Value{
			TypedValue: &Value_Integer{
				Integer: v,
			},
		}

	case uint:
		return &Value{
			TypedValue: &Value_Integer{
				Integer: int64(v),
			},
		}

	case uint16:
		return &Value{
			TypedValue: &Value_Integer{
				Integer: int64(v),
			},
		}

	case uint32:
		return &Value{
			TypedValue: &Value_Integer{
				Integer: int64(v),
			},
		}

	case uint64:
		return &Value{
			TypedValue: &Value_Integer{
				Integer: int64(v),
			},
		}

	case float32:
		return &Value{
			TypedValue: &Value_Decimal{
				Decimal: v,
			},
		}

	case float64:
		return &Value{
			TypedValue: &Value_Decimal{
				Decimal: float32(v),
			},
		}

	case bool:
		return &Value{
			TypedValue: &Value_Boolean{
				Boolean: v,
			},
		}

	case nil:
		return &Value{
			TypedValue: &Value_Null{},
		}

	case time.Time:
		return &Value{
			TypedValue: &Value_Timestamp{
				Timestamp: timestamppb.New(v),
			},
		}
	}

	return &Value{TypedValue: &Value_Null{}}
}

// UnmarshalValues unmarshals an array of MicroDB value types into Go types.
func UnmarshalValues(vs []*Value) []interface{} {
	is := make([]interface{}, 0, len(vs))

	for _, v := range vs {
		i := v.GetInterface()
		is = append(is, i)
	}
	return is
}

// GetInterface unmarshals a MicroDB value type into a Go type.
func (x *Value) GetInterface() interface{} {
	switch x.GetTypedValue().(type) {
	case *Value_Varchar:
		return x.GetVarchar()

	case *Value_Integer:
		return x.GetInteger()

	case *Value_Decimal:
		return x.GetDecimal()

	case *Value_Boolean:
		return x.GetBoolean()

	case *Value_Null:
		return nil

	case *Value_Timestamp:
		return x.GetTimestamp().AsTime()
	}

	return nil
}

// LastInsertId returns the database's auto-generated ID after, for example, an INSERT into a table
// with primary key.
func (x *DriverResult) LastInsertId() (int64, error) {
	return x.GetResultLastInsertId(), nil
}

// RowsAffected returns the number of rows affected by the query.
func (x *DriverResult) RowsAffected() (int64, error) {
	return x.GetResultRowsAffected(), nil
}
