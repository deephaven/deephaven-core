#pragma once

#include <vector>
#include <arrow/array.h>
#include "deephaven/client/highlevel/sad/sad_row_sequence.h"
#include "deephaven/client/highlevel/sad/sad_chunk.h"
#include "deephaven/client/highlevel/sad/sad_context.h"

namespace deephaven::client::highlevel::sad {
class SadColumnSourceContext;
class SadLongColumnSource;
class SadColumnSourceVisitor;

// the column source interfaces

class SadColumnSource {
public:
  virtual ~SadColumnSource();
  virtual std::shared_ptr<SadColumnSourceContext> createContext(size_t chunkSize) const = 0;
  virtual void fillChunk(SadContext *context, const SadRowSequence &rows, SadChunk *dest) const = 0;
  virtual void fillChunkUnordered(SadContext *context, const SadLongChunk &rowKeys, size_t size, SadChunk *dest) const = 0;

  virtual void acceptVisitor(SadColumnSourceVisitor *visitor) const = 0;
};

class SadMutableColumnSource : public SadColumnSource {
public:
  virtual ~SadMutableColumnSource() override;
  virtual void fillFromChunk(SadContext *context, const SadChunk &src, const SadRowSequence &rows) = 0;
  virtual void fillFromChunkUnordered(SadContext *context, const SadChunk &src, const SadLongChunk &rowKeys, size_t size) = 0;
};

// the per-type interfaces

class SadIntColumnSource : public SadMutableColumnSource {
public:
    ~SadIntColumnSource() override;
};

class SadLongColumnSource : public SadMutableColumnSource {
public:
  ~SadLongColumnSource() override;
};

class SadDoubleColumnSource : public SadMutableColumnSource {
public:
  ~SadDoubleColumnSource() override;
};

class SadIntArrayColumnSource final : public SadIntColumnSource, std::enable_shared_from_this<SadIntArrayColumnSource> {
    struct Private {};
public:
    static std::shared_ptr<SadIntArrayColumnSource> create();
    explicit SadIntArrayColumnSource(Private);
    ~SadIntArrayColumnSource() final;

    std::shared_ptr<SadColumnSourceContext> createContext(size_t chunkSize) const final;
    void fillChunk(SadContext *context, const SadRowSequence &rows, SadChunk *dest) const final;
    void fillChunkUnordered(SadContext *context, const SadLongChunk &rowKeys, size_t size, SadChunk *dest) const final;
    void fillFromChunk(SadContext *context, const SadChunk &src, const SadRowSequence &rows) final;
    void fillFromChunkUnordered(SadContext *context, const SadChunk &src, const SadLongChunk &rowKeys, size_t size) final;

    void acceptVisitor(SadColumnSourceVisitor *visitor) const final;

private:
    void ensureSize(size_t size);
    std::vector<int32_t> data_;
};

class SadLongArrayColumnSource final : public SadLongColumnSource, std::enable_shared_from_this<SadLongArrayColumnSource> {
  struct Private {};
public:
  static std::shared_ptr<SadLongArrayColumnSource> create();
  explicit SadLongArrayColumnSource(Private);
  ~SadLongArrayColumnSource() final;

  std::shared_ptr<SadColumnSourceContext> createContext(size_t chunkSize) const final;
  void fillChunk(SadContext *context, const SadRowSequence &rows, SadChunk *dest) const final;
  void fillChunkUnordered(SadContext *context, const SadLongChunk &rowKeys, size_t size, SadChunk *dest) const final;
  void fillFromChunk(SadContext *context, const SadChunk &src, const SadRowSequence &rows) final;
  void fillFromChunkUnordered(SadContext *context, const SadChunk &src, const SadLongChunk &rowKeys, size_t size) final;

  void acceptVisitor(SadColumnSourceVisitor *visitor) const final;

private:
  void ensureSize(size_t size);
  std::vector<int64_t> data_;
};

class SadDoubleArrayColumnSource final : public SadDoubleColumnSource, std::enable_shared_from_this<SadDoubleArrayColumnSource> {
  struct Private {};
public:
  static std::shared_ptr<SadDoubleArrayColumnSource> create();
  explicit SadDoubleArrayColumnSource(Private);
  ~SadDoubleArrayColumnSource() final;

  std::shared_ptr<SadColumnSourceContext> createContext(size_t chunkSize) const final;
  void fillChunk(SadContext *context, const SadRowSequence &rows, SadChunk *dest) const final;
  void fillChunkUnordered(SadContext *context, const SadLongChunk &rowKeys, size_t size, SadChunk *dest) const final;
  void fillFromChunk(SadContext *context, const SadChunk &src, const SadRowSequence &rows) final;
  void fillFromChunkUnordered(SadContext *context, const SadChunk &src, const SadLongChunk &rowKeys, size_t size) final;

  void acceptVisitor(SadColumnSourceVisitor *visitor) const final;

private:
  void ensureSize(size_t size);
  std::vector<double> data_;
};

class SadColumnSourceContext : public SadContext {
public:
  virtual ~SadColumnSourceContext();
};

class SadColumnSourceVisitor {
public:
  virtual void visit(const SadIntColumnSource *) = 0;
  virtual void visit(const SadLongColumnSource *) = 0;
  virtual void visit(const SadDoubleColumnSource *) = 0;
};
}  // namespace deephaven::client::highlevel::sad
