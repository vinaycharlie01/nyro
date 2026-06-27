package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// testStringer is a test type that implements fmt.Stringer.
type testStringer struct {
	value string
}

func (ts testStringer) String() string {
	return ts.value
}

func TestKeyToString(t *testing.T) {
	tests := []struct {
		name string
		key  any
		want string
	}{
		{
			name: "string",
			key:  "test-key",
			want: "test-key",
		},
		{
			name: "int",
			key:  123,
			want: "123",
		},
		{
			name: "int8",
			key:  int8(127),
			want: "127",
		},
		{
			name: "int8_negative",
			key:  int8(-128),
			want: "-128",
		},
		{
			name: "int16",
			key:  int16(32767),
			want: "32767",
		},
		{
			name: "int16_negative",
			key:  int16(-32768),
			want: "-32768",
		},
		{
			name: "int32",
			key:  int32(2147483647),
			want: "2147483647",
		},
		{
			name: "int32_negative",
			key:  int32(-2147483648),
			want: "-2147483648",
		},
		{
			name: "int64",
			key:  int64(456),
			want: "456",
		},
		{
			name: "int64_negative",
			key:  int64(-9223372036854775808),
			want: "-9223372036854775808",
		},
		{
			name: "uint",
			key:  uint(789),
			want: "789",
		},
		{
			name: "uint8",
			key:  uint8(255),
			want: "255",
		},
		{
			name: "uint16",
			key:  uint16(65535),
			want: "65535",
		},
		{
			name: "uint32",
			key:  uint32(4294967295),
			want: "4294967295",
		},
		{
			name: "uint64",
			key:  uint64(18446744073709551615),
			want: "18446744073709551615",
		},
		{
			name: "fmt.Stringer",
			key:  testStringer{value: "custom-key"},
			want: "custom-key",
		},
		{
			name: "bool",
			key:  true,
			want: "true",
		},
		{
			name: "float64",
			key:  3.14,
			want: "3.14",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := KeyToString(tt.key)
			assert.Equal(t, tt.want, got)
		})
	}
}
