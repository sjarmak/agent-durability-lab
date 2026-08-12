package evidence

import (
	"strings"
	"testing"
)

func TestDecodeJSONStrictRejectsParserDifferentials(t *testing.T) {
	t.Parallel()

	type record struct {
		Name   string         `json:"name"`
		Nested map[string]int `json:"nested"`
	}
	tests := []struct {
		name    string
		data    string
		wantErr string
	}{
		{name: "valid", data: `{"name":"run","nested":{"value":1}}`},
		{name: "duplicate nested key", data: `{"name":"run","nested":{"value":1,"value":1}}`, wantErr: "duplicate"},
		{name: "trailing value", data: `{"name":"run","nested":{}} {}`, wantErr: "trailing"},
		{name: "unknown field", data: `{"name":"run","nested":{},"extra":true}`, wantErr: "unknown field"},
		{name: "malformed", data: `{"name":`, wantErr: "EOF"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var destination record
			err := DecodeJSONStrict([]byte(test.data), &destination)
			if test.wantErr == "" && err != nil {
				t.Fatalf("DecodeJSONStrict() error = %v", err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("DecodeJSONStrict() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}
