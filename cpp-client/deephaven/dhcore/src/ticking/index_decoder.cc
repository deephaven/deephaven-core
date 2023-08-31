/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/ticking/index_decoder.h"

#include <cstdlib>
#include <memory>
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::container::RowSequenceBuilder;
using deephaven::dhcore::utility::Stringf;

namespace deephaven::dhcore::ticking {
namespace {
struct Constants {
  static constexpr const int8_t kShortValue = 1;
  static constexpr const int8_t kIntValue = 2;
  static constexpr const int8_t kLongValue = 3;
  static constexpr const int8_t kByteValue = 4;

  static constexpr const int8_t kValueMask = 7;

  static constexpr const int8_t kOffset = 8;
  static constexpr const int8_t kShortArray = 16;
  static constexpr const int8_t kByteArray = 24;
  static constexpr const int8_t kEnd = 32;
  static constexpr const int8_t kCmdMask = 0x78;
};
}  // namespace

std::shared_ptr<RowSequence> IndexDecoder::ReadExternalCompressedDelta(DataInput *in) {
  RowSequenceBuilder builder;

  int64_t offset = 0;

  int64_t pending = -1;
  auto consume = [&pending, &builder](int64_t v) {
    auto s = pending;
    if (s == -1) {
      pending = v;
    } else if (v < 0) {
      auto begin = static_cast<uint64_t>(s);
      auto end = static_cast<uint64_t>(-v) + 1;
      builder.AddInterval(begin, end);
      pending = -1;
    } else {
      builder.Add(s);
      pending = v;
    }
  };

  while (true) {
    int64_t actual_value;
    int command = in->ReadByte();

    switch (command & Constants::kCmdMask) {
      case Constants::kOffset: {
        int64_t value = in->ReadValue(command);
          actual_value = offset + (value < 0 ? -value : value);
        consume(value < 0 ? -actual_value : actual_value);
        offset = actual_value;
        break;
      }

      case Constants::kShortArray: {
        int short_count = static_cast<int>(in->ReadValue(command));
        for (int ii = 0; ii < short_count; ++ii) {
          int16_t short_value = in->ReadShort();
            actual_value = offset + (short_value < 0 ? -short_value : short_value);
          consume(short_value < 0 ? -actual_value : actual_value);
          offset = actual_value;
        }
        break;
      }

      case Constants::kByteArray: {
        int byte_count = static_cast<int>(in->ReadValue(command));
        for (int ii = 0; ii < byte_count; ++ii) {
          int8_t byte_value = in->ReadByte();
            actual_value = offset + (byte_value < 0 ? -byte_value : byte_value);
          consume(byte_value < 0 ? -actual_value : actual_value);
          offset = actual_value;
        }
        break;
      }

      case Constants::kEnd: {
        if (pending >= 0) {
          builder.Add(pending);
        }
        return builder.Build();
      }

      default: {
        auto message = Stringf("Bad command: %o", command);
        throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
      }
    }
  }
}

int64_t DataInput::ReadValue(int command) {
  switch (command & Constants::kValueMask) {
    case Constants::kLongValue: {
      return ReadLong();
    }
    case Constants::kIntValue: {
      return ReadInt();
    }
    case Constants::kShortValue: {
      return ReadShort();
    }
    case Constants::kByteValue: {
      return ReadByte();
    }
    default: {
      auto message = Stringf("Bad command: %o", command);
      throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
    }
  }
}

int8_t DataInput::ReadByte() {
  int8_t result;
  std::memcpy(&result, data_, sizeof(result));
  data_ += sizeof(result);
  return result;
}

int16_t DataInput::ReadShort() {
  int16_t result;
  std::memcpy(&result, data_, sizeof(result));
  data_ += sizeof(result);
  return result;
}

int32_t DataInput::ReadInt() {
  int32_t result;
  std::memcpy(&result, data_, sizeof(result));
  data_ += sizeof(result);
  return result;
}

int64_t DataInput::ReadLong() {
  int64_t result;
  std::memcpy(&result, data_, sizeof(result));
  data_ += sizeof(result);
  return result;
}
}  // namespace deephaven::dhcore::ticking
