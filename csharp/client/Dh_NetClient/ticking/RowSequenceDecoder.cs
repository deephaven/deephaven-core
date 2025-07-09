//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
namespace Deephaven.Dh_NetClient;

public class RowSequenceDecoder {
  public static RowSequence ReadExternalCompressedDelta(DataInput input) {
    var builder = new RowSequenceBuilder();

    Int64 offset = 0;

    Int64 pending = -1;
    void Consume(Int64 v) {
      var s = pending;
      if (s == -1) {
        pending = v;
      } else if (v < 0) {
        var begin = (UInt64)s;
        var end = (UInt64)(-v) + 1;
        builder.AddInterval(Interval.Of(begin, end));
        pending = -1;
      } else {
        builder.Add((UInt64)s);
        pending = v;
      }
    };

    while (true) {
      Int64 actualValue;
      var command = input.ReadByte();

      switch (command & Constants.CmdMask) {
        case Constants.Offset: {
          Int64 value = input.ReadValue(command);
          actualValue = offset + (value < 0 ? -value : value);
          Consume(value < 0 ? -actualValue : actualValue);
          offset = actualValue;
          break;
        }

        case Constants.ShortArray: {
          var shortCount = (int)input.ReadValue(command);
          for (int ii = 0; ii < shortCount; ++ii) {
            var shortValue = input.ReadShort();
            actualValue = offset + (shortValue < 0 ? -shortValue : shortValue);
            Consume(shortValue < 0 ? -actualValue : actualValue);
            offset = actualValue;
          }

          break;
        }

        case Constants.ByteArray: {
          var byteCount = (int)input.ReadValue(command);
          for (int ii = 0; ii < byteCount; ++ii) {
            var byteValue = input.ReadByte();
            actualValue = offset + (byteValue < 0 ? -byteValue : byteValue);
            Consume(byteValue < 0 ? -actualValue : actualValue);
            offset = actualValue;
          }

          break;
        }

        case Constants.End: {
            if (pending >= 0) {
              builder.Add((UInt64)pending);
            }
            return builder.Build();
          }

        default: {
          throw new Exception($"Bad command: {command}");
        }
      }
    }
  }

}

static class Constants {
  public const sbyte ShortValue = 1;
  public const sbyte IntValue = 2;
  public const sbyte LongValue = 3;
  public const sbyte ByteValue = 4;

  public const sbyte ValueMask = 7;

  public const sbyte Offset = 8;
  public const sbyte ShortArray = 16;
  public const sbyte ByteArray = 24;
  public const sbyte End = 32;
  public const sbyte CmdMask = 0x78;
};

public class DataInput {
  private readonly byte[] _data;
  private int _offset = 0;

  public DataInput(IEnumerable<byte> data) {
    // Probably don't need to materialize this as an array, but it makes debugging easier.
    _data = data.ToArray();
  }

  public Int64 ReadValue(int command) {
    switch (command & Constants.ValueMask) {
      case Constants.LongValue: {
        return ReadLong();
      }
      case Constants.IntValue: {
        return ReadInt();
      }
      case Constants.ShortValue: {
        return ReadShort();
      }
      case Constants.ByteValue: {
        return ReadByte();
      }
      default: {
        throw new Exception("Bad command: {command:x}");
      }
    }
  }

  public sbyte ReadByte() {
    var result = (sbyte)_data[_offset];
    ++_offset;
    return result;
  }

  public Int16 ReadShort() {
    var result = BitConverter.ToInt16(_data, _offset);
    _offset += 2;
    return result;
  }

  public Int32 ReadInt() {
    var result = BitConverter.ToInt32(_data, _offset);
    _offset += 4;
    return result;
  }

  public Int64 ReadLong() {
    var result = BitConverter.ToInt64(_data, _offset);
    _offset += 8;
    return result;
  }
}
