package filing

import "testing"

func TestDropDuplCols(t *testing.T) {
	tests := []struct {
		name  string
		input matrix
		want  matrix
	}{
		{
			"One duplicate",
			matrix{
				{"test", "test", "hihi", "hoho"},
				{"2", "2", "2", "2"},
				{"2", "2", "2", "2"},
			},
			matrix{
				{"test", "hihi", "hoho"},
				{"2", "2", "2"},
				{"2", "2", "2"},
			},
		},
		{
			"No duplicate",
			matrix{
				{"test", "hihi", "hoho"},
				{"2", "2", "2"},
				{"2", "2", "2"},
			},
			matrix{
				{"test", "hihi", "hoho"},
				{"2", "2", "2"},
				{"2", "2", "2"},
			},
		},
		{
			"Multiple same duplicates",
			matrix{
				{"test", "test", "test", "hoho"},
				{"2", "2", "2", "2"},
				{"2", "2", "2", "2"},
			},
			matrix{
				{"test", "hoho"},
				{"2", "2"},
				{"2", "2"},
			},
		},
		{
			"Multiple different duplicates",
			matrix{
				{"test", "test", "hihi", "hihi"},
				{"2", "2", "2", "2"},
				{"2", "2", "2", "2"},
			},
			matrix{
				{"test", "hihi"},
				{"2", "2"},
				{"2", "2"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.input.dropDuplCols()
			if err != nil {
				t.Errorf(err.Error())
				return
			}
			if len(got) != len(test.want) {
				t.Errorf("Got %d rows, want %d rows", len(got), len(test.want))
				return
			}
			for i, r := range test.want {
				if len(r) != len(got[i]) {
					t.Errorf("Got %d columns at row %d, want %d columns", len(got[i]), i, len(r))
					return
				}
				for j, c := range r {
					if c != got[i][j] {
						t.Errorf(
							"Got '%s' at row %d column %d, want '%s'",
							got[i][j], i, j, c,
						)
						return
					}
				}
			}
		})
	}
}
