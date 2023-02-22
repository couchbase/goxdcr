package base

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestServerVersion_String(t *testing.T) {
	tests := []struct {
		name string
		s    ServerVersion
		want string
	}{
		{
			name: "7.0",
			s:    VersionForCollectionSupport,
			want: "7.0",
		},
		{
			name: "Minor",
			s:    ServerVersion{7, 0, 0},
			want: "7.0.0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.s.String(), "String()")
		})
	}
}

func TestNewServerVersionFromString(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name    string
		args    args
		want    ServerVersion
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "7.0.0",
			args: args{str: "7.0.0"},
			want: ServerVersion{7, 0, 0},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewServerVersionFromString(tt.args.str)
			if !tt.wantErr(t, err, fmt.Sprintf("NewServerVersionFromString(%v)", tt.args.str)) {
				return
			}
			assert.Equalf(t, tt.want, got, "NewServerVersionFromString(%v)", tt.args.str)
		})
	}
}
