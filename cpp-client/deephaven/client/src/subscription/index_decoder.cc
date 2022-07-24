/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/subscription/index_decoder.h"

#include <cstdlib>
#include <memory>
#include "deephaven/flatbuf/Barrage_generated.h"
#include "deephaven/client/container/row_sequence.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::container::RowSequence;
using deephaven::client::container::RowSequenceBuilder;
using deephaven::client::utility::stringf;

namespace deephaven::client::subscription {
namespace {
struct Constants {
  static constexpr const int8_t SHORT_VALUE = 1;
  static constexpr const int8_t INT_VALUE = 2;
  static constexpr const int8_t LONG_VALUE = 3;
  static constexpr const int8_t BYTE_VALUE = 4;

  static constexpr const int8_t VALUE_MASK = 7;

  static constexpr const int8_t OFFSET = 8;
  static constexpr const int8_t SHORT_ARRAY = 16;
  static constexpr const int8_t BYTE_ARRAY = 24;
  static constexpr const int8_t END = 32;
  static constexpr const int8_t CMD_MASK = 0x78;
};
}  // namespace

std::shared_ptr<RowSequence> IndexDecoder::readExternalCompressedDelta(DataInput *in) {
  RowSequenceBuilder builder;

  int64_t offset = 0;

  int64_t pending = -1;
  auto consume = [&pending, &builder](int64_t v) {
    auto s = pending;
    if (s == -1) {
      pending = v;
    } else if (v < 0) {
      auto begin = (uint64_t)s;
      auto end = ((uint64_t)-v) + 1;
      builder.addRange(begin, end);
      pending = -1;
    } else {
      builder.add(s);
      pending = v;
    }
  };

  while (true) {
    int64_t actualValue;
    int command = in->readByte();

    switch (command & Constants::CMD_MASK) {
      case Constants::OFFSET: {
        int64_t value = in->readValue(command);
        actualValue = offset + (value < 0 ? -value : value);
        consume(value < 0 ? -actualValue : actualValue);
        offset = actualValue;
        break;
      }

      case Constants::SHORT_ARRAY: {
        int shortCount = (int) in->readValue(command);
        for (int ii = 0; ii < shortCount; ++ii) {
          int16_t shortValue = in->readShort();
          actualValue = offset + (shortValue < 0 ? -shortValue : shortValue);
          consume(shortValue < 0 ? -actualValue : actualValue);
          offset = actualValue;
        }
        break;
      }

      case Constants::BYTE_ARRAY: {
        int byteCount = (int) in->readValue(command);
        for (int ii = 0; ii < byteCount; ++ii) {
          int8_t byteValue = in->readByte();
          actualValue = offset + (byteValue < 0 ? -byteValue : byteValue);
          consume(byteValue < 0 ? -actualValue : actualValue);
          offset = actualValue;
        }
        break;
      }

      case Constants::END: {
        if (pending >= 0) {
          builder.add(pending);
        }
        return builder.build();
      }

      default:
        throw std::runtime_error(stringf("Bad command: %o", command));
    }
  }
}

int64_t DataInput::readValue(int command) {
  switch (command & Constants::VALUE_MASK) {
    case Constants::LONG_VALUE: {
      return readLong();
    }
    case Constants::INT_VALUE: {
      return readInt();
    }
    case Constants::SHORT_VALUE: {
      return readShort();
    }
    case Constants::BYTE_VALUE: {
      return readByte();
    }
    default: {
      throw std::runtime_error(stringf("Bad command: %o", command));
    }
  }
}

int8_t DataInput::readByte() {
  int8_t result;
  std::memcpy(&result, data_, sizeof(result));
  data_ += sizeof(result);
  return result;
}

int16_t DataInput::readShort() {
  int16_t result;
  std::memcpy(&result, data_, sizeof(result));
  data_ += sizeof(result);
  return result;
}

int32_t DataInput::readInt() {
  int32_t result;
  std::memcpy(&result, data_, sizeof(result));
  data_ += sizeof(result);
  return result;
}

int64_t DataInput::readLong() {
  int64_t result;
  std::memcpy(&result, data_, sizeof(result));
  data_ += sizeof(result);
  return result;
}
}  // namespace deephaven::client::subscription
