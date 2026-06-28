package cache_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	cache "github.com/vinaycharlie01/nyro/pkg"
)

func TestDecodeString(t *testing.T) {
	tests := []struct {
		name    string
		result  any
		want    string
		wantErr bool
	}{
		{
			name:   "nil_returns_zero_value",
			result: nil,
			want:   "",
		},
		{
			name:   "direct_string",
			result: "hello",
			want:   "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cache.Decode[string](tt.result)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDecodeUser(t *testing.T) {
	type User struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	tests := []struct {
		name    string
		result  any
		want    User
		wantErr bool
	}{
		{
			name: "map_to_struct",
			result: map[string]any{
				"id":   1,
				"name": "Vinay",
			},
			want: User{
				ID:   1,
				Name: "Vinay",
			},
		},
		{
			name: "invalid_type",
			result: map[string]any{
				"id": "not_a_number",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cache.Decode[User](tt.result)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDecodeError(t *testing.T) {
	type User struct {
		ID int `json:"id"`
	}

	invalidData := map[string]any{
		"id": "invalid_number",
	}

	_, err := cache.Decode[User](invalidData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cache: type mismatch in cached payload")
}
