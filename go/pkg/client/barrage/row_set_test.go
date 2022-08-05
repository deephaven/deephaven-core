package barrage

import "testing"

func TestRoundtrip(t *testing.T) {
	ser := NewRowSetSerializer()

	ser.AddRow(0)
	ser.AddRowRange(3, 5)
	ser.AddRow(10)
	ser.AddRowRange(8192, 8194)
	ser.AddRow(0x100_0000)
	ser.AddRow(0x420_0000_0000)

	bytes := ser.Finish()

	des, err := DeserializeRowSet(bytes)
	if err != nil {
		t.Error("deserialize error: ", err)
		return
	}

	expected := []int64{0, 3, 4, 5, 10, 8192, 8193, 8194, 0x100_0000, 0x420_0000_0000}
	if len(des) != len(expected) {
		t.Error("deserialized data was not correct (wrong length): ", expected)
		return
	}
	for i := 0; i < len(des); i++ {
		if des[i] != expected[i] {
			t.Error("deserialized data was not correct (mismatch): ", expected)
			return
		}
	}
}
