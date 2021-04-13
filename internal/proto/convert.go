package proto

func MarshalValues(is []interface{}) []*Value {
	vs := make([]*Value, 0, len(is))
	for _, i := range is {
		v := MarshalValue(interface{}(i))
		if v != nil {
			vs = append(vs, v)
		}
	}
	return vs
}

func MarshalValue(i interface{}) *Value {
	switch v := i.(type) {
	case string:
		return &Value{
			TypedValue: &Value_Varchar{
				Varchar: v,
			},
		}

	case int, int8, int16, int32, int64:
		return &Value{
			TypedValue: &Value_Integer{
				Integer: v.(int64),
			},
		}

	case float32, float64:
		return &Value{
			TypedValue: &Value_Decimal{
				Decimal: v.(float32),
			},
		}
	}

	return nil
}

func (x *Value) GetInterface() interface{} {
	switch x.GetTypedValue().(type) {
	case *Value_Varchar:
		return x.GetVarchar()

	case *Value_Integer:
		return x.GetInteger()

	case *Value_Decimal:
		return x.GetDecimal()
	}

	return nil
}
