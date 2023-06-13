// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: deephaven/proto/object.proto

#ifndef GOOGLE_PROTOBUF_INCLUDED_deephaven_2fproto_2fobject_2eproto_2epb_2eh
#define GOOGLE_PROTOBUF_INCLUDED_deephaven_2fproto_2fobject_2eproto_2epb_2eh

#include <limits>
#include <string>
#include <type_traits>

#include "google/protobuf/port_def.inc"
#if PROTOBUF_VERSION < 4023000
#error "This file was generated by a newer version of protoc which is"
#error "incompatible with your Protocol Buffer headers. Please update"
#error "your headers."
#endif  // PROTOBUF_VERSION

#if 4023002 < PROTOBUF_MIN_PROTOC_VERSION
#error "This file was generated by an older version of protoc which is"
#error "incompatible with your Protocol Buffer headers. Please"
#error "regenerate this file with a newer version of protoc."
#endif  // PROTOBUF_MIN_PROTOC_VERSION
#include "google/protobuf/port_undef.inc"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/arena.h"
#include "google/protobuf/arenastring.h"
#include "google/protobuf/generated_message_util.h"
#include "google/protobuf/metadata_lite.h"
#include "google/protobuf/generated_message_reflection.h"
#include "google/protobuf/message.h"
#include "google/protobuf/repeated_field.h"  // IWYU pragma: export
#include "google/protobuf/extension_set.h"  // IWYU pragma: export
#include "google/protobuf/unknown_field_set.h"
#include "deephaven/proto/ticket.pb.h"
// @@protoc_insertion_point(includes)

// Must be included last.
#include "google/protobuf/port_def.inc"

#define PROTOBUF_INTERNAL_EXPORT_deephaven_2fproto_2fobject_2eproto

PROTOBUF_NAMESPACE_OPEN
namespace internal {
class AnyMetadata;
}  // namespace internal
PROTOBUF_NAMESPACE_CLOSE

// Internal implementation detail -- do not use these members.
struct TableStruct_deephaven_2fproto_2fobject_2eproto {
  static const ::uint32_t offsets[];
};
extern const ::PROTOBUF_NAMESPACE_ID::internal::DescriptorTable
    descriptor_table_deephaven_2fproto_2fobject_2eproto;
namespace io {
namespace deephaven {
namespace proto {
namespace backplane {
namespace grpc {
class FetchObjectRequest;
struct FetchObjectRequestDefaultTypeInternal;
extern FetchObjectRequestDefaultTypeInternal _FetchObjectRequest_default_instance_;
class FetchObjectResponse;
struct FetchObjectResponseDefaultTypeInternal;
extern FetchObjectResponseDefaultTypeInternal _FetchObjectResponse_default_instance_;
}  // namespace grpc
}  // namespace backplane
}  // namespace proto
}  // namespace deephaven
}  // namespace io
PROTOBUF_NAMESPACE_OPEN
template <>
::io::deephaven::proto::backplane::grpc::FetchObjectRequest* Arena::CreateMaybeMessage<::io::deephaven::proto::backplane::grpc::FetchObjectRequest>(Arena*);
template <>
::io::deephaven::proto::backplane::grpc::FetchObjectResponse* Arena::CreateMaybeMessage<::io::deephaven::proto::backplane::grpc::FetchObjectResponse>(Arena*);
PROTOBUF_NAMESPACE_CLOSE

namespace io {
namespace deephaven {
namespace proto {
namespace backplane {
namespace grpc {

// ===================================================================


// -------------------------------------------------------------------

class FetchObjectRequest final :
    public ::PROTOBUF_NAMESPACE_ID::Message /* @@protoc_insertion_point(class_definition:io.deephaven.proto.backplane.grpc.FetchObjectRequest) */ {
 public:
  inline FetchObjectRequest() : FetchObjectRequest(nullptr) {}
  ~FetchObjectRequest() override;
  template<typename = void>
  explicit PROTOBUF_CONSTEXPR FetchObjectRequest(::PROTOBUF_NAMESPACE_ID::internal::ConstantInitialized);

  FetchObjectRequest(const FetchObjectRequest& from);
  FetchObjectRequest(FetchObjectRequest&& from) noexcept
    : FetchObjectRequest() {
    *this = ::std::move(from);
  }

  inline FetchObjectRequest& operator=(const FetchObjectRequest& from) {
    CopyFrom(from);
    return *this;
  }
  inline FetchObjectRequest& operator=(FetchObjectRequest&& from) noexcept {
    if (this == &from) return *this;
    if (GetOwningArena() == from.GetOwningArena()
  #ifdef PROTOBUF_FORCE_COPY_IN_MOVE
        && GetOwningArena() != nullptr
  #endif  // !PROTOBUF_FORCE_COPY_IN_MOVE
    ) {
      InternalSwap(&from);
    } else {
      CopyFrom(from);
    }
    return *this;
  }

  inline const ::PROTOBUF_NAMESPACE_ID::UnknownFieldSet& unknown_fields() const {
    return _internal_metadata_.unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(::PROTOBUF_NAMESPACE_ID::UnknownFieldSet::default_instance);
  }
  inline ::PROTOBUF_NAMESPACE_ID::UnknownFieldSet* mutable_unknown_fields() {
    return _internal_metadata_.mutable_unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>();
  }

  static const ::PROTOBUF_NAMESPACE_ID::Descriptor* descriptor() {
    return GetDescriptor();
  }
  static const ::PROTOBUF_NAMESPACE_ID::Descriptor* GetDescriptor() {
    return default_instance().GetMetadata().descriptor;
  }
  static const ::PROTOBUF_NAMESPACE_ID::Reflection* GetReflection() {
    return default_instance().GetMetadata().reflection;
  }
  static const FetchObjectRequest& default_instance() {
    return *internal_default_instance();
  }
  static inline const FetchObjectRequest* internal_default_instance() {
    return reinterpret_cast<const FetchObjectRequest*>(
               &_FetchObjectRequest_default_instance_);
  }
  static constexpr int kIndexInFileMessages =
    0;

  friend void swap(FetchObjectRequest& a, FetchObjectRequest& b) {
    a.Swap(&b);
  }
  inline void Swap(FetchObjectRequest* other) {
    if (other == this) return;
  #ifdef PROTOBUF_FORCE_COPY_IN_SWAP
    if (GetOwningArena() != nullptr &&
        GetOwningArena() == other->GetOwningArena()) {
   #else  // PROTOBUF_FORCE_COPY_IN_SWAP
    if (GetOwningArena() == other->GetOwningArena()) {
  #endif  // !PROTOBUF_FORCE_COPY_IN_SWAP
      InternalSwap(other);
    } else {
      ::PROTOBUF_NAMESPACE_ID::internal::GenericSwap(this, other);
    }
  }
  void UnsafeArenaSwap(FetchObjectRequest* other) {
    if (other == this) return;
    ABSL_DCHECK(GetOwningArena() == other->GetOwningArena());
    InternalSwap(other);
  }

  // implements Message ----------------------------------------------

  FetchObjectRequest* New(::PROTOBUF_NAMESPACE_ID::Arena* arena = nullptr) const final {
    return CreateMaybeMessage<FetchObjectRequest>(arena);
  }
  using ::PROTOBUF_NAMESPACE_ID::Message::CopyFrom;
  void CopyFrom(const FetchObjectRequest& from);
  using ::PROTOBUF_NAMESPACE_ID::Message::MergeFrom;
  void MergeFrom( const FetchObjectRequest& from) {
    FetchObjectRequest::MergeImpl(*this, from);
  }
  private:
  static void MergeImpl(::PROTOBUF_NAMESPACE_ID::Message& to_msg, const ::PROTOBUF_NAMESPACE_ID::Message& from_msg);
  public:
  PROTOBUF_ATTRIBUTE_REINITIALIZES void Clear() final;
  bool IsInitialized() const final;

  ::size_t ByteSizeLong() const final;
  const char* _InternalParse(const char* ptr, ::PROTOBUF_NAMESPACE_ID::internal::ParseContext* ctx) final;
  ::uint8_t* _InternalSerialize(
      ::uint8_t* target, ::PROTOBUF_NAMESPACE_ID::io::EpsCopyOutputStream* stream) const final;
  int GetCachedSize() const final { return _impl_._cached_size_.Get(); }

  private:
  void SharedCtor(::PROTOBUF_NAMESPACE_ID::Arena* arena);
  void SharedDtor();
  void SetCachedSize(int size) const final;
  void InternalSwap(FetchObjectRequest* other);

  private:
  friend class ::PROTOBUF_NAMESPACE_ID::internal::AnyMetadata;
  static ::absl::string_view FullMessageName() {
    return "io.deephaven.proto.backplane.grpc.FetchObjectRequest";
  }
  protected:
  explicit FetchObjectRequest(::PROTOBUF_NAMESPACE_ID::Arena* arena);
  public:

  static const ClassData _class_data_;
  const ::PROTOBUF_NAMESPACE_ID::Message::ClassData*GetClassData() const final;

  ::PROTOBUF_NAMESPACE_ID::Metadata GetMetadata() const final;

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  enum : int {
    kSourceIdFieldNumber = 1,
  };
  // .io.deephaven.proto.backplane.grpc.TypedTicket source_id = 1;
  bool has_source_id() const;
  void clear_source_id() ;
  const ::io::deephaven::proto::backplane::grpc::TypedTicket& source_id() const;
  PROTOBUF_NODISCARD ::io::deephaven::proto::backplane::grpc::TypedTicket* release_source_id();
  ::io::deephaven::proto::backplane::grpc::TypedTicket* mutable_source_id();
  void set_allocated_source_id(::io::deephaven::proto::backplane::grpc::TypedTicket* source_id);
  private:
  const ::io::deephaven::proto::backplane::grpc::TypedTicket& _internal_source_id() const;
  ::io::deephaven::proto::backplane::grpc::TypedTicket* _internal_mutable_source_id();
  public:
  void unsafe_arena_set_allocated_source_id(
      ::io::deephaven::proto::backplane::grpc::TypedTicket* source_id);
  ::io::deephaven::proto::backplane::grpc::TypedTicket* unsafe_arena_release_source_id();
  // @@protoc_insertion_point(class_scope:io.deephaven.proto.backplane.grpc.FetchObjectRequest)
 private:
  class _Internal;

  template <typename T> friend class ::PROTOBUF_NAMESPACE_ID::Arena::InternalHelper;
  typedef void InternalArenaConstructable_;
  typedef void DestructorSkippable_;
  struct Impl_ {
    ::PROTOBUF_NAMESPACE_ID::internal::HasBits<1> _has_bits_;
    mutable ::PROTOBUF_NAMESPACE_ID::internal::CachedSize _cached_size_;
    ::io::deephaven::proto::backplane::grpc::TypedTicket* source_id_;
  };
  union { Impl_ _impl_; };
  friend struct ::TableStruct_deephaven_2fproto_2fobject_2eproto;
};// -------------------------------------------------------------------

class FetchObjectResponse final :
    public ::PROTOBUF_NAMESPACE_ID::Message /* @@protoc_insertion_point(class_definition:io.deephaven.proto.backplane.grpc.FetchObjectResponse) */ {
 public:
  inline FetchObjectResponse() : FetchObjectResponse(nullptr) {}
  ~FetchObjectResponse() override;
  template<typename = void>
  explicit PROTOBUF_CONSTEXPR FetchObjectResponse(::PROTOBUF_NAMESPACE_ID::internal::ConstantInitialized);

  FetchObjectResponse(const FetchObjectResponse& from);
  FetchObjectResponse(FetchObjectResponse&& from) noexcept
    : FetchObjectResponse() {
    *this = ::std::move(from);
  }

  inline FetchObjectResponse& operator=(const FetchObjectResponse& from) {
    CopyFrom(from);
    return *this;
  }
  inline FetchObjectResponse& operator=(FetchObjectResponse&& from) noexcept {
    if (this == &from) return *this;
    if (GetOwningArena() == from.GetOwningArena()
  #ifdef PROTOBUF_FORCE_COPY_IN_MOVE
        && GetOwningArena() != nullptr
  #endif  // !PROTOBUF_FORCE_COPY_IN_MOVE
    ) {
      InternalSwap(&from);
    } else {
      CopyFrom(from);
    }
    return *this;
  }

  inline const ::PROTOBUF_NAMESPACE_ID::UnknownFieldSet& unknown_fields() const {
    return _internal_metadata_.unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(::PROTOBUF_NAMESPACE_ID::UnknownFieldSet::default_instance);
  }
  inline ::PROTOBUF_NAMESPACE_ID::UnknownFieldSet* mutable_unknown_fields() {
    return _internal_metadata_.mutable_unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>();
  }

  static const ::PROTOBUF_NAMESPACE_ID::Descriptor* descriptor() {
    return GetDescriptor();
  }
  static const ::PROTOBUF_NAMESPACE_ID::Descriptor* GetDescriptor() {
    return default_instance().GetMetadata().descriptor;
  }
  static const ::PROTOBUF_NAMESPACE_ID::Reflection* GetReflection() {
    return default_instance().GetMetadata().reflection;
  }
  static const FetchObjectResponse& default_instance() {
    return *internal_default_instance();
  }
  static inline const FetchObjectResponse* internal_default_instance() {
    return reinterpret_cast<const FetchObjectResponse*>(
               &_FetchObjectResponse_default_instance_);
  }
  static constexpr int kIndexInFileMessages =
    1;

  friend void swap(FetchObjectResponse& a, FetchObjectResponse& b) {
    a.Swap(&b);
  }
  inline void Swap(FetchObjectResponse* other) {
    if (other == this) return;
  #ifdef PROTOBUF_FORCE_COPY_IN_SWAP
    if (GetOwningArena() != nullptr &&
        GetOwningArena() == other->GetOwningArena()) {
   #else  // PROTOBUF_FORCE_COPY_IN_SWAP
    if (GetOwningArena() == other->GetOwningArena()) {
  #endif  // !PROTOBUF_FORCE_COPY_IN_SWAP
      InternalSwap(other);
    } else {
      ::PROTOBUF_NAMESPACE_ID::internal::GenericSwap(this, other);
    }
  }
  void UnsafeArenaSwap(FetchObjectResponse* other) {
    if (other == this) return;
    ABSL_DCHECK(GetOwningArena() == other->GetOwningArena());
    InternalSwap(other);
  }

  // implements Message ----------------------------------------------

  FetchObjectResponse* New(::PROTOBUF_NAMESPACE_ID::Arena* arena = nullptr) const final {
    return CreateMaybeMessage<FetchObjectResponse>(arena);
  }
  using ::PROTOBUF_NAMESPACE_ID::Message::CopyFrom;
  void CopyFrom(const FetchObjectResponse& from);
  using ::PROTOBUF_NAMESPACE_ID::Message::MergeFrom;
  void MergeFrom( const FetchObjectResponse& from) {
    FetchObjectResponse::MergeImpl(*this, from);
  }
  private:
  static void MergeImpl(::PROTOBUF_NAMESPACE_ID::Message& to_msg, const ::PROTOBUF_NAMESPACE_ID::Message& from_msg);
  public:
  PROTOBUF_ATTRIBUTE_REINITIALIZES void Clear() final;
  bool IsInitialized() const final;

  ::size_t ByteSizeLong() const final;
  const char* _InternalParse(const char* ptr, ::PROTOBUF_NAMESPACE_ID::internal::ParseContext* ctx) final;
  ::uint8_t* _InternalSerialize(
      ::uint8_t* target, ::PROTOBUF_NAMESPACE_ID::io::EpsCopyOutputStream* stream) const final;
  int GetCachedSize() const final { return _impl_._cached_size_.Get(); }

  private:
  void SharedCtor(::PROTOBUF_NAMESPACE_ID::Arena* arena);
  void SharedDtor();
  void SetCachedSize(int size) const final;
  void InternalSwap(FetchObjectResponse* other);

  private:
  friend class ::PROTOBUF_NAMESPACE_ID::internal::AnyMetadata;
  static ::absl::string_view FullMessageName() {
    return "io.deephaven.proto.backplane.grpc.FetchObjectResponse";
  }
  protected:
  explicit FetchObjectResponse(::PROTOBUF_NAMESPACE_ID::Arena* arena);
  public:

  static const ClassData _class_data_;
  const ::PROTOBUF_NAMESPACE_ID::Message::ClassData*GetClassData() const final;

  ::PROTOBUF_NAMESPACE_ID::Metadata GetMetadata() const final;

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  enum : int {
    kTypedExportIdFieldNumber = 3,
    kTypeFieldNumber = 1,
    kDataFieldNumber = 2,
  };
  // repeated .io.deephaven.proto.backplane.grpc.TypedTicket typed_export_id = 3;
  int typed_export_id_size() const;
  private:
  int _internal_typed_export_id_size() const;

  public:
  void clear_typed_export_id() ;
  ::io::deephaven::proto::backplane::grpc::TypedTicket* mutable_typed_export_id(int index);
  ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField< ::io::deephaven::proto::backplane::grpc::TypedTicket >*
      mutable_typed_export_id();
  private:
  const ::io::deephaven::proto::backplane::grpc::TypedTicket& _internal_typed_export_id(int index) const;
  ::io::deephaven::proto::backplane::grpc::TypedTicket* _internal_add_typed_export_id();
  const ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField<::io::deephaven::proto::backplane::grpc::TypedTicket>& _internal_typed_export_id() const;
  ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField<::io::deephaven::proto::backplane::grpc::TypedTicket>* _internal_mutable_typed_export_id();
  public:
  const ::io::deephaven::proto::backplane::grpc::TypedTicket& typed_export_id(int index) const;
  ::io::deephaven::proto::backplane::grpc::TypedTicket* add_typed_export_id();
  const ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField< ::io::deephaven::proto::backplane::grpc::TypedTicket >&
      typed_export_id() const;
  // string type = 1;
  void clear_type() ;
  const std::string& type() const;




  template <typename Arg_ = const std::string&, typename... Args_>
  void set_type(Arg_&& arg, Args_... args);
  std::string* mutable_type();
  PROTOBUF_NODISCARD std::string* release_type();
  void set_allocated_type(std::string* ptr);

  private:
  const std::string& _internal_type() const;
  inline PROTOBUF_ALWAYS_INLINE void _internal_set_type(
      const std::string& value);
  std::string* _internal_mutable_type();

  public:
  // bytes data = 2;
  void clear_data() ;
  const std::string& data() const;




  template <typename Arg_ = const std::string&, typename... Args_>
  void set_data(Arg_&& arg, Args_... args);
  std::string* mutable_data();
  PROTOBUF_NODISCARD std::string* release_data();
  void set_allocated_data(std::string* ptr);

  private:
  const std::string& _internal_data() const;
  inline PROTOBUF_ALWAYS_INLINE void _internal_set_data(
      const std::string& value);
  std::string* _internal_mutable_data();

  public:
  // @@protoc_insertion_point(class_scope:io.deephaven.proto.backplane.grpc.FetchObjectResponse)
 private:
  class _Internal;

  template <typename T> friend class ::PROTOBUF_NAMESPACE_ID::Arena::InternalHelper;
  typedef void InternalArenaConstructable_;
  typedef void DestructorSkippable_;
  struct Impl_ {
    ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField< ::io::deephaven::proto::backplane::grpc::TypedTicket > typed_export_id_;
    ::PROTOBUF_NAMESPACE_ID::internal::ArenaStringPtr type_;
    ::PROTOBUF_NAMESPACE_ID::internal::ArenaStringPtr data_;
    mutable ::PROTOBUF_NAMESPACE_ID::internal::CachedSize _cached_size_;
  };
  union { Impl_ _impl_; };
  friend struct ::TableStruct_deephaven_2fproto_2fobject_2eproto;
};

// ===================================================================




// ===================================================================


#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif  // __GNUC__
// -------------------------------------------------------------------

// FetchObjectRequest

// .io.deephaven.proto.backplane.grpc.TypedTicket source_id = 1;
inline bool FetchObjectRequest::has_source_id() const {
  bool value = (_impl_._has_bits_[0] & 0x00000001u) != 0;
  PROTOBUF_ASSUME(!value || _impl_.source_id_ != nullptr);
  return value;
}
inline const ::io::deephaven::proto::backplane::grpc::TypedTicket& FetchObjectRequest::_internal_source_id() const {
  const ::io::deephaven::proto::backplane::grpc::TypedTicket* p = _impl_.source_id_;
  return p != nullptr ? *p : reinterpret_cast<const ::io::deephaven::proto::backplane::grpc::TypedTicket&>(
      ::io::deephaven::proto::backplane::grpc::_TypedTicket_default_instance_);
}
inline const ::io::deephaven::proto::backplane::grpc::TypedTicket& FetchObjectRequest::source_id() const {
  // @@protoc_insertion_point(field_get:io.deephaven.proto.backplane.grpc.FetchObjectRequest.source_id)
  return _internal_source_id();
}
inline void FetchObjectRequest::unsafe_arena_set_allocated_source_id(
    ::io::deephaven::proto::backplane::grpc::TypedTicket* source_id) {
  if (GetArenaForAllocation() == nullptr) {
    delete reinterpret_cast<::PROTOBUF_NAMESPACE_ID::MessageLite*>(_impl_.source_id_);
  }
  _impl_.source_id_ = source_id;
  if (source_id) {
    _impl_._has_bits_[0] |= 0x00000001u;
  } else {
    _impl_._has_bits_[0] &= ~0x00000001u;
  }
  // @@protoc_insertion_point(field_unsafe_arena_set_allocated:io.deephaven.proto.backplane.grpc.FetchObjectRequest.source_id)
}
inline ::io::deephaven::proto::backplane::grpc::TypedTicket* FetchObjectRequest::release_source_id() {
  _impl_._has_bits_[0] &= ~0x00000001u;
  ::io::deephaven::proto::backplane::grpc::TypedTicket* temp = _impl_.source_id_;
  _impl_.source_id_ = nullptr;
#ifdef PROTOBUF_FORCE_COPY_IN_RELEASE
  auto* old =  reinterpret_cast<::PROTOBUF_NAMESPACE_ID::MessageLite*>(temp);
  temp = ::PROTOBUF_NAMESPACE_ID::internal::DuplicateIfNonNull(temp);
  if (GetArenaForAllocation() == nullptr) { delete old; }
#else  // PROTOBUF_FORCE_COPY_IN_RELEASE
  if (GetArenaForAllocation() != nullptr) {
    temp = ::PROTOBUF_NAMESPACE_ID::internal::DuplicateIfNonNull(temp);
  }
#endif  // !PROTOBUF_FORCE_COPY_IN_RELEASE
  return temp;
}
inline ::io::deephaven::proto::backplane::grpc::TypedTicket* FetchObjectRequest::unsafe_arena_release_source_id() {
  // @@protoc_insertion_point(field_release:io.deephaven.proto.backplane.grpc.FetchObjectRequest.source_id)
  _impl_._has_bits_[0] &= ~0x00000001u;
  ::io::deephaven::proto::backplane::grpc::TypedTicket* temp = _impl_.source_id_;
  _impl_.source_id_ = nullptr;
  return temp;
}
inline ::io::deephaven::proto::backplane::grpc::TypedTicket* FetchObjectRequest::_internal_mutable_source_id() {
  _impl_._has_bits_[0] |= 0x00000001u;
  if (_impl_.source_id_ == nullptr) {
    auto* p = CreateMaybeMessage<::io::deephaven::proto::backplane::grpc::TypedTicket>(GetArenaForAllocation());
    _impl_.source_id_ = p;
  }
  return _impl_.source_id_;
}
inline ::io::deephaven::proto::backplane::grpc::TypedTicket* FetchObjectRequest::mutable_source_id() {
  ::io::deephaven::proto::backplane::grpc::TypedTicket* _msg = _internal_mutable_source_id();
  // @@protoc_insertion_point(field_mutable:io.deephaven.proto.backplane.grpc.FetchObjectRequest.source_id)
  return _msg;
}
inline void FetchObjectRequest::set_allocated_source_id(::io::deephaven::proto::backplane::grpc::TypedTicket* source_id) {
  ::PROTOBUF_NAMESPACE_ID::Arena* message_arena = GetArenaForAllocation();
  if (message_arena == nullptr) {
    delete reinterpret_cast< ::PROTOBUF_NAMESPACE_ID::MessageLite*>(_impl_.source_id_);
  }
  if (source_id) {
    ::PROTOBUF_NAMESPACE_ID::Arena* submessage_arena =
        ::PROTOBUF_NAMESPACE_ID::Arena::InternalGetOwningArena(
                reinterpret_cast<::PROTOBUF_NAMESPACE_ID::MessageLite*>(source_id));
    if (message_arena != submessage_arena) {
      source_id = ::PROTOBUF_NAMESPACE_ID::internal::GetOwnedMessage(
          message_arena, source_id, submessage_arena);
    }
    _impl_._has_bits_[0] |= 0x00000001u;
  } else {
    _impl_._has_bits_[0] &= ~0x00000001u;
  }
  _impl_.source_id_ = source_id;
  // @@protoc_insertion_point(field_set_allocated:io.deephaven.proto.backplane.grpc.FetchObjectRequest.source_id)
}

// -------------------------------------------------------------------

// FetchObjectResponse

// string type = 1;
inline void FetchObjectResponse::clear_type() {
  _impl_.type_.ClearToEmpty();
}
inline const std::string& FetchObjectResponse::type() const {
  // @@protoc_insertion_point(field_get:io.deephaven.proto.backplane.grpc.FetchObjectResponse.type)
  return _internal_type();
}
template <typename Arg_, typename... Args_>
inline PROTOBUF_ALWAYS_INLINE void FetchObjectResponse::set_type(Arg_&& arg,
                                                     Args_... args) {
  ;
  _impl_.type_.Set(static_cast<Arg_&&>(arg), args..., GetArenaForAllocation());
  // @@protoc_insertion_point(field_set:io.deephaven.proto.backplane.grpc.FetchObjectResponse.type)
}
inline std::string* FetchObjectResponse::mutable_type() {
  std::string* _s = _internal_mutable_type();
  // @@protoc_insertion_point(field_mutable:io.deephaven.proto.backplane.grpc.FetchObjectResponse.type)
  return _s;
}
inline const std::string& FetchObjectResponse::_internal_type() const {
  return _impl_.type_.Get();
}
inline void FetchObjectResponse::_internal_set_type(const std::string& value) {
  ;


  _impl_.type_.Set(value, GetArenaForAllocation());
}
inline std::string* FetchObjectResponse::_internal_mutable_type() {
  ;
  return _impl_.type_.Mutable( GetArenaForAllocation());
}
inline std::string* FetchObjectResponse::release_type() {
  // @@protoc_insertion_point(field_release:io.deephaven.proto.backplane.grpc.FetchObjectResponse.type)
  return _impl_.type_.Release();
}
inline void FetchObjectResponse::set_allocated_type(std::string* value) {
  _impl_.type_.SetAllocated(value, GetArenaForAllocation());
  #ifdef PROTOBUF_FORCE_COPY_DEFAULT_STRING
        if (_impl_.type_.IsDefault()) {
          _impl_.type_.Set("", GetArenaForAllocation());
        }
  #endif  // PROTOBUF_FORCE_COPY_DEFAULT_STRING
  // @@protoc_insertion_point(field_set_allocated:io.deephaven.proto.backplane.grpc.FetchObjectResponse.type)
}

// bytes data = 2;
inline void FetchObjectResponse::clear_data() {
  _impl_.data_.ClearToEmpty();
}
inline const std::string& FetchObjectResponse::data() const {
  // @@protoc_insertion_point(field_get:io.deephaven.proto.backplane.grpc.FetchObjectResponse.data)
  return _internal_data();
}
template <typename Arg_, typename... Args_>
inline PROTOBUF_ALWAYS_INLINE void FetchObjectResponse::set_data(Arg_&& arg,
                                                     Args_... args) {
  ;
  _impl_.data_.SetBytes(static_cast<Arg_&&>(arg), args..., GetArenaForAllocation());
  // @@protoc_insertion_point(field_set:io.deephaven.proto.backplane.grpc.FetchObjectResponse.data)
}
inline std::string* FetchObjectResponse::mutable_data() {
  std::string* _s = _internal_mutable_data();
  // @@protoc_insertion_point(field_mutable:io.deephaven.proto.backplane.grpc.FetchObjectResponse.data)
  return _s;
}
inline const std::string& FetchObjectResponse::_internal_data() const {
  return _impl_.data_.Get();
}
inline void FetchObjectResponse::_internal_set_data(const std::string& value) {
  ;


  _impl_.data_.Set(value, GetArenaForAllocation());
}
inline std::string* FetchObjectResponse::_internal_mutable_data() {
  ;
  return _impl_.data_.Mutable( GetArenaForAllocation());
}
inline std::string* FetchObjectResponse::release_data() {
  // @@protoc_insertion_point(field_release:io.deephaven.proto.backplane.grpc.FetchObjectResponse.data)
  return _impl_.data_.Release();
}
inline void FetchObjectResponse::set_allocated_data(std::string* value) {
  _impl_.data_.SetAllocated(value, GetArenaForAllocation());
  #ifdef PROTOBUF_FORCE_COPY_DEFAULT_STRING
        if (_impl_.data_.IsDefault()) {
          _impl_.data_.Set("", GetArenaForAllocation());
        }
  #endif  // PROTOBUF_FORCE_COPY_DEFAULT_STRING
  // @@protoc_insertion_point(field_set_allocated:io.deephaven.proto.backplane.grpc.FetchObjectResponse.data)
}

// repeated .io.deephaven.proto.backplane.grpc.TypedTicket typed_export_id = 3;
inline int FetchObjectResponse::_internal_typed_export_id_size() const {
  return _impl_.typed_export_id_.size();
}
inline int FetchObjectResponse::typed_export_id_size() const {
  return _internal_typed_export_id_size();
}
inline ::io::deephaven::proto::backplane::grpc::TypedTicket* FetchObjectResponse::mutable_typed_export_id(int index) {
  // @@protoc_insertion_point(field_mutable:io.deephaven.proto.backplane.grpc.FetchObjectResponse.typed_export_id)
  return _internal_mutable_typed_export_id()->Mutable(index);
}
inline ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField< ::io::deephaven::proto::backplane::grpc::TypedTicket >*
FetchObjectResponse::mutable_typed_export_id() {
  // @@protoc_insertion_point(field_mutable_list:io.deephaven.proto.backplane.grpc.FetchObjectResponse.typed_export_id)
  return _internal_mutable_typed_export_id();
}
inline const ::io::deephaven::proto::backplane::grpc::TypedTicket& FetchObjectResponse::_internal_typed_export_id(int index) const {
  return _internal_typed_export_id().Get(index);
}
inline const ::io::deephaven::proto::backplane::grpc::TypedTicket& FetchObjectResponse::typed_export_id(int index) const {
  // @@protoc_insertion_point(field_get:io.deephaven.proto.backplane.grpc.FetchObjectResponse.typed_export_id)
  return _internal_typed_export_id(index);
}
inline ::io::deephaven::proto::backplane::grpc::TypedTicket* FetchObjectResponse::_internal_add_typed_export_id() {
  return _internal_mutable_typed_export_id()->Add();
}
inline ::io::deephaven::proto::backplane::grpc::TypedTicket* FetchObjectResponse::add_typed_export_id() {
  ::io::deephaven::proto::backplane::grpc::TypedTicket* _add = _internal_add_typed_export_id();
  // @@protoc_insertion_point(field_add:io.deephaven.proto.backplane.grpc.FetchObjectResponse.typed_export_id)
  return _add;
}
inline const ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField< ::io::deephaven::proto::backplane::grpc::TypedTicket >&
FetchObjectResponse::typed_export_id() const {
  // @@protoc_insertion_point(field_list:io.deephaven.proto.backplane.grpc.FetchObjectResponse.typed_export_id)
  return _internal_typed_export_id();
}
inline const ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField<::io::deephaven::proto::backplane::grpc::TypedTicket>&
FetchObjectResponse::_internal_typed_export_id() const {
  return _impl_.typed_export_id_;
}
inline ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField<::io::deephaven::proto::backplane::grpc::TypedTicket>*
FetchObjectResponse::_internal_mutable_typed_export_id() {
  return &_impl_.typed_export_id_;
}

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif  // __GNUC__

// @@protoc_insertion_point(namespace_scope)
}  // namespace grpc
}  // namespace backplane
}  // namespace proto
}  // namespace deephaven
}  // namespace io


// @@protoc_insertion_point(global_scope)

#include "google/protobuf/port_undef.inc"

#endif  // GOOGLE_PROTOBUF_INCLUDED_deephaven_2fproto_2fobject_2eproto_2epb_2eh
