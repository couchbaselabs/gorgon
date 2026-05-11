package gorgon

import (
	"bytes"
	"errors"
	"testing"
)

func TestOperationMarshalling(t *testing.T) {
	op1 := &Operation{Output: 10}
	want1 := []byte(`{"ClientId":0,"Input":null,"Call":0,"Output":10,"Return":0}`)

	op2 := &Operation{Output: nil}
	want2 := []byte(`{"ClientId":0,"Input":null,"Call":0,"Output":null,"Return":0}`)

	var err error
	err = errors.New("Test Error")

	// Ambiguous error is a normal error
	op3 := &Operation{Output: err}
	want3 := []byte(`{"ClientId":0,"Input":null,"Call":0,"Output":"Test Error","Return":0}`)

	// Unambiguous error is wrapped
	err = WrapUnambiguousError(err)
	op4 := &Operation{Output: err}
	want4 := []byte(`{"ClientId":0,"Input":null,"Call":0,"Output":"Test Error","Return":0}`)

	tests := []struct {
		name  string
		input *Operation
		want  []byte
	}{
		{"output_int", op1, want1},
		{"output_nil", op2, want2},
		{"output_ambiguous", op3, want3},
		{"output_unambiguous", op4, want4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodedSlice, err := tt.input.MarshalJSON()
			if err != nil {
				t.Fatalf("Custom Encoder returned error: %s", err)
			}
			if !bytes.Equal(tt.want, encodedSlice) {
				t.Errorf("Expected encoded byte slice: %s, Actual byte slice: %s", string(tt.want), string(encodedSlice))
			}
		})
	}
}
