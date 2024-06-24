// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: deephaven/proto/ticket.proto

#include "deephaven/proto/ticket.pb.h"

#include <algorithm>
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/extension_set.h"
#include "google/protobuf/wire_format_lite.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/generated_message_reflection.h"
#include "google/protobuf/reflection_ops.h"
#include "google/protobuf/wire_format.h"
#include "google/protobuf/generated_message_tctable_impl.h"
// @@protoc_insertion_point(includes)

// Must be included last.
#include "google/protobuf/port_def.inc"
PROTOBUF_PRAGMA_INIT_SEG
namespace _pb = ::google::protobuf;
namespace _pbi = ::google::protobuf::internal;
namespace _fl = ::google::protobuf::internal::field_layout;
namespace io {
namespace deephaven {
namespace proto {
namespace backplane {
namespace grpc {

inline constexpr Ticket::Impl_::Impl_(
    ::_pbi::ConstantInitialized) noexcept
      : ticket_(
            &::google::protobuf::internal::fixed_address_empty_string,
            ::_pbi::ConstantInitialized()),
        _cached_size_{0} {}

template <typename>
PROTOBUF_CONSTEXPR Ticket::Ticket(::_pbi::ConstantInitialized)
    : _impl_(::_pbi::ConstantInitialized()) {}
struct TicketDefaultTypeInternal {
  PROTOBUF_CONSTEXPR TicketDefaultTypeInternal() : _instance(::_pbi::ConstantInitialized{}) {}
  ~TicketDefaultTypeInternal() {}
  union {
    Ticket _instance;
  };
};

PROTOBUF_ATTRIBUTE_NO_DESTROY PROTOBUF_CONSTINIT
    PROTOBUF_ATTRIBUTE_INIT_PRIORITY1 TicketDefaultTypeInternal _Ticket_default_instance_;

inline constexpr TypedTicket::Impl_::Impl_(
    ::_pbi::ConstantInitialized) noexcept
      : _cached_size_{0},
        type_(
            &::google::protobuf::internal::fixed_address_empty_string,
            ::_pbi::ConstantInitialized()),
        ticket_{nullptr} {}

template <typename>
PROTOBUF_CONSTEXPR TypedTicket::TypedTicket(::_pbi::ConstantInitialized)
    : _impl_(::_pbi::ConstantInitialized()) {}
struct TypedTicketDefaultTypeInternal {
  PROTOBUF_CONSTEXPR TypedTicketDefaultTypeInternal() : _instance(::_pbi::ConstantInitialized{}) {}
  ~TypedTicketDefaultTypeInternal() {}
  union {
    TypedTicket _instance;
  };
};

PROTOBUF_ATTRIBUTE_NO_DESTROY PROTOBUF_CONSTINIT
    PROTOBUF_ATTRIBUTE_INIT_PRIORITY1 TypedTicketDefaultTypeInternal _TypedTicket_default_instance_;
}  // namespace grpc
}  // namespace backplane
}  // namespace proto
}  // namespace deephaven
}  // namespace io
static ::_pb::Metadata file_level_metadata_deephaven_2fproto_2fticket_2eproto[2];
static constexpr const ::_pb::EnumDescriptor**
    file_level_enum_descriptors_deephaven_2fproto_2fticket_2eproto = nullptr;
static constexpr const ::_pb::ServiceDescriptor**
    file_level_service_descriptors_deephaven_2fproto_2fticket_2eproto = nullptr;
const ::uint32_t TableStruct_deephaven_2fproto_2fticket_2eproto::offsets[] PROTOBUF_SECTION_VARIABLE(
    protodesc_cold) = {
    ~0u,  // no _has_bits_
    PROTOBUF_FIELD_OFFSET(::io::deephaven::proto::backplane::grpc::Ticket, _internal_metadata_),
    ~0u,  // no _extensions_
    ~0u,  // no _oneof_case_
    ~0u,  // no _weak_field_map_
    ~0u,  // no _inlined_string_donated_
    ~0u,  // no _split_
    ~0u,  // no sizeof(Split)
    PROTOBUF_FIELD_OFFSET(::io::deephaven::proto::backplane::grpc::Ticket, _impl_.ticket_),
    PROTOBUF_FIELD_OFFSET(::io::deephaven::proto::backplane::grpc::TypedTicket, _impl_._has_bits_),
    PROTOBUF_FIELD_OFFSET(::io::deephaven::proto::backplane::grpc::TypedTicket, _internal_metadata_),
    ~0u,  // no _extensions_
    ~0u,  // no _oneof_case_
    ~0u,  // no _weak_field_map_
    ~0u,  // no _inlined_string_donated_
    ~0u,  // no _split_
    ~0u,  // no sizeof(Split)
    PROTOBUF_FIELD_OFFSET(::io::deephaven::proto::backplane::grpc::TypedTicket, _impl_.ticket_),
    PROTOBUF_FIELD_OFFSET(::io::deephaven::proto::backplane::grpc::TypedTicket, _impl_.type_),
    0,
    ~0u,
};

static const ::_pbi::MigrationSchema
    schemas[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) = {
        {0, -1, -1, sizeof(::io::deephaven::proto::backplane::grpc::Ticket)},
        {9, 19, -1, sizeof(::io::deephaven::proto::backplane::grpc::TypedTicket)},
};

static const ::_pb::Message* const file_default_instances[] = {
    &::io::deephaven::proto::backplane::grpc::_Ticket_default_instance_._instance,
    &::io::deephaven::proto::backplane::grpc::_TypedTicket_default_instance_._instance,
};
const char descriptor_table_protodef_deephaven_2fproto_2fticket_2eproto[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) = {
    "\n\034deephaven/proto/ticket.proto\022!io.deeph"
    "aven.proto.backplane.grpc\"\030\n\006Ticket\022\016\n\006t"
    "icket\030\001 \001(\014\"V\n\013TypedTicket\0229\n\006ticket\030\001 \001"
    "(\0132).io.deephaven.proto.backplane.grpc.T"
    "icket\022\014\n\004type\030\002 \001(\tBBH\001P\001Z<github.com/de"
    "ephaven/deephaven-core/go/internal/proto"
    "/ticketb\006proto3"
};
static ::absl::once_flag descriptor_table_deephaven_2fproto_2fticket_2eproto_once;
const ::_pbi::DescriptorTable descriptor_table_deephaven_2fproto_2fticket_2eproto = {
    false,
    false,
    255,
    descriptor_table_protodef_deephaven_2fproto_2fticket_2eproto,
    "deephaven/proto/ticket.proto",
    &descriptor_table_deephaven_2fproto_2fticket_2eproto_once,
    nullptr,
    0,
    2,
    schemas,
    file_default_instances,
    TableStruct_deephaven_2fproto_2fticket_2eproto::offsets,
    file_level_metadata_deephaven_2fproto_2fticket_2eproto,
    file_level_enum_descriptors_deephaven_2fproto_2fticket_2eproto,
    file_level_service_descriptors_deephaven_2fproto_2fticket_2eproto,
};

// This function exists to be marked as weak.
// It can significantly speed up compilation by breaking up LLVM's SCC
// in the .pb.cc translation units. Large translation units see a
// reduction of more than 35% of walltime for optimized builds. Without
// the weak attribute all the messages in the file, including all the
// vtables and everything they use become part of the same SCC through
// a cycle like:
// GetMetadata -> descriptor table -> default instances ->
//   vtables -> GetMetadata
// By adding a weak function here we break the connection from the
// individual vtables back into the descriptor table.
PROTOBUF_ATTRIBUTE_WEAK const ::_pbi::DescriptorTable* descriptor_table_deephaven_2fproto_2fticket_2eproto_getter() {
  return &descriptor_table_deephaven_2fproto_2fticket_2eproto;
}
// Force running AddDescriptors() at dynamic initialization time.
PROTOBUF_ATTRIBUTE_INIT_PRIORITY2
static ::_pbi::AddDescriptorsRunner dynamic_init_dummy_deephaven_2fproto_2fticket_2eproto(&descriptor_table_deephaven_2fproto_2fticket_2eproto);
namespace io {
namespace deephaven {
namespace proto {
namespace backplane {
namespace grpc {
// ===================================================================

class Ticket::_Internal {
 public:
};

Ticket::Ticket(::google::protobuf::Arena* arena)
    : ::google::protobuf::Message(arena) {
  SharedCtor(arena);
  // @@protoc_insertion_point(arena_constructor:io.deephaven.proto.backplane.grpc.Ticket)
}
inline PROTOBUF_NDEBUG_INLINE Ticket::Impl_::Impl_(
    ::google::protobuf::internal::InternalVisibility visibility, ::google::protobuf::Arena* arena,
    const Impl_& from)
      : ticket_(arena, from.ticket_),
        _cached_size_{0} {}

Ticket::Ticket(
    ::google::protobuf::Arena* arena,
    const Ticket& from)
    : ::google::protobuf::Message(arena) {
  Ticket* const _this = this;
  (void)_this;
  _internal_metadata_.MergeFrom<::google::protobuf::UnknownFieldSet>(
      from._internal_metadata_);
  new (&_impl_) Impl_(internal_visibility(), arena, from._impl_);

  // @@protoc_insertion_point(copy_constructor:io.deephaven.proto.backplane.grpc.Ticket)
}
inline PROTOBUF_NDEBUG_INLINE Ticket::Impl_::Impl_(
    ::google::protobuf::internal::InternalVisibility visibility,
    ::google::protobuf::Arena* arena)
      : ticket_(arena),
        _cached_size_{0} {}

inline void Ticket::SharedCtor(::_pb::Arena* arena) {
  new (&_impl_) Impl_(internal_visibility(), arena);
}
Ticket::~Ticket() {
  // @@protoc_insertion_point(destructor:io.deephaven.proto.backplane.grpc.Ticket)
  _internal_metadata_.Delete<::google::protobuf::UnknownFieldSet>();
  SharedDtor();
}
inline void Ticket::SharedDtor() {
  ABSL_DCHECK(GetArena() == nullptr);
  _impl_.ticket_.Destroy();
  _impl_.~Impl_();
}

PROTOBUF_NOINLINE void Ticket::Clear() {
// @@protoc_insertion_point(message_clear_start:io.deephaven.proto.backplane.grpc.Ticket)
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  ::uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  _impl_.ticket_.ClearToEmpty();
  _internal_metadata_.Clear<::google::protobuf::UnknownFieldSet>();
}

const char* Ticket::_InternalParse(
    const char* ptr, ::_pbi::ParseContext* ctx) {
  ptr = ::_pbi::TcParser::ParseLoop(this, ptr, ctx, &_table_.header);
  return ptr;
}


PROTOBUF_CONSTINIT PROTOBUF_ATTRIBUTE_INIT_PRIORITY1
const ::_pbi::TcParseTable<0, 1, 0, 0, 2> Ticket::_table_ = {
  {
    0,  // no _has_bits_
    0, // no _extensions_
    1, 0,  // max_field_number, fast_idx_mask
    offsetof(decltype(_table_), field_lookup_table),
    4294967294,  // skipmap
    offsetof(decltype(_table_), field_entries),
    1,  // num_field_entries
    0,  // num_aux_entries
    offsetof(decltype(_table_), field_names),  // no aux_entries
    &_Ticket_default_instance_._instance,
    ::_pbi::TcParser::GenericFallback,  // fallback
  }, {{
    // bytes ticket = 1;
    {::_pbi::TcParser::FastBS1,
     {10, 63, 0, PROTOBUF_FIELD_OFFSET(Ticket, _impl_.ticket_)}},
  }}, {{
    65535, 65535
  }}, {{
    // bytes ticket = 1;
    {PROTOBUF_FIELD_OFFSET(Ticket, _impl_.ticket_), 0, 0,
    (0 | ::_fl::kFcSingular | ::_fl::kBytes | ::_fl::kRepAString)},
  }},
  // no aux_entries
  {{
  }},
};

::uint8_t* Ticket::_InternalSerialize(
    ::uint8_t* target,
    ::google::protobuf::io::EpsCopyOutputStream* stream) const {
  // @@protoc_insertion_point(serialize_to_array_start:io.deephaven.proto.backplane.grpc.Ticket)
  ::uint32_t cached_has_bits = 0;
  (void)cached_has_bits;

  // bytes ticket = 1;
  if (!this->_internal_ticket().empty()) {
    const std::string& _s = this->_internal_ticket();
    target = stream->WriteBytesMaybeAliased(1, _s, target);
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    target =
        ::_pbi::WireFormat::InternalSerializeUnknownFieldsToArray(
            _internal_metadata_.unknown_fields<::google::protobuf::UnknownFieldSet>(::google::protobuf::UnknownFieldSet::default_instance), target, stream);
  }
  // @@protoc_insertion_point(serialize_to_array_end:io.deephaven.proto.backplane.grpc.Ticket)
  return target;
}

::size_t Ticket::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:io.deephaven.proto.backplane.grpc.Ticket)
  ::size_t total_size = 0;

  ::uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  // bytes ticket = 1;
  if (!this->_internal_ticket().empty()) {
    total_size += 1 + ::google::protobuf::internal::WireFormatLite::BytesSize(
                                    this->_internal_ticket());
  }

  return MaybeComputeUnknownFieldsSize(total_size, &_impl_._cached_size_);
}

const ::google::protobuf::Message::ClassData Ticket::_class_data_ = {
    Ticket::MergeImpl,
    nullptr,  // OnDemandRegisterArenaDtor
};
const ::google::protobuf::Message::ClassData* Ticket::GetClassData() const {
  return &_class_data_;
}

void Ticket::MergeImpl(::google::protobuf::Message& to_msg, const ::google::protobuf::Message& from_msg) {
  auto* const _this = static_cast<Ticket*>(&to_msg);
  auto& from = static_cast<const Ticket&>(from_msg);
  // @@protoc_insertion_point(class_specific_merge_from_start:io.deephaven.proto.backplane.grpc.Ticket)
  ABSL_DCHECK_NE(&from, _this);
  ::uint32_t cached_has_bits = 0;
  (void) cached_has_bits;

  if (!from._internal_ticket().empty()) {
    _this->_internal_set_ticket(from._internal_ticket());
  }
  _this->_internal_metadata_.MergeFrom<::google::protobuf::UnknownFieldSet>(from._internal_metadata_);
}

void Ticket::CopyFrom(const Ticket& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:io.deephaven.proto.backplane.grpc.Ticket)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

PROTOBUF_NOINLINE bool Ticket::IsInitialized() const {
  return true;
}

::_pbi::CachedSize* Ticket::AccessCachedSize() const {
  return &_impl_._cached_size_;
}
void Ticket::InternalSwap(Ticket* PROTOBUF_RESTRICT other) {
  using std::swap;
  auto* arena = GetArena();
  ABSL_DCHECK_EQ(arena, other->GetArena());
  _internal_metadata_.InternalSwap(&other->_internal_metadata_);
  ::_pbi::ArenaStringPtr::InternalSwap(&_impl_.ticket_, &other->_impl_.ticket_, arena);
}

::google::protobuf::Metadata Ticket::GetMetadata() const {
  return ::_pbi::AssignDescriptors(
      &descriptor_table_deephaven_2fproto_2fticket_2eproto_getter, &descriptor_table_deephaven_2fproto_2fticket_2eproto_once,
      file_level_metadata_deephaven_2fproto_2fticket_2eproto[0]);
}
// ===================================================================

class TypedTicket::_Internal {
 public:
  using HasBits = decltype(std::declval<TypedTicket>()._impl_._has_bits_);
  static constexpr ::int32_t kHasBitsOffset =
    8 * PROTOBUF_FIELD_OFFSET(TypedTicket, _impl_._has_bits_);
  static const ::io::deephaven::proto::backplane::grpc::Ticket& ticket(const TypedTicket* msg);
  static void set_has_ticket(HasBits* has_bits) {
    (*has_bits)[0] |= 1u;
  }
};

const ::io::deephaven::proto::backplane::grpc::Ticket& TypedTicket::_Internal::ticket(const TypedTicket* msg) {
  return *msg->_impl_.ticket_;
}
TypedTicket::TypedTicket(::google::protobuf::Arena* arena)
    : ::google::protobuf::Message(arena) {
  SharedCtor(arena);
  // @@protoc_insertion_point(arena_constructor:io.deephaven.proto.backplane.grpc.TypedTicket)
}
inline PROTOBUF_NDEBUG_INLINE TypedTicket::Impl_::Impl_(
    ::google::protobuf::internal::InternalVisibility visibility, ::google::protobuf::Arena* arena,
    const Impl_& from)
      : _has_bits_{from._has_bits_},
        _cached_size_{0},
        type_(arena, from.type_) {}

TypedTicket::TypedTicket(
    ::google::protobuf::Arena* arena,
    const TypedTicket& from)
    : ::google::protobuf::Message(arena) {
  TypedTicket* const _this = this;
  (void)_this;
  _internal_metadata_.MergeFrom<::google::protobuf::UnknownFieldSet>(
      from._internal_metadata_);
  new (&_impl_) Impl_(internal_visibility(), arena, from._impl_);
  ::uint32_t cached_has_bits = _impl_._has_bits_[0];
  _impl_.ticket_ = (cached_has_bits & 0x00000001u)
                ? CreateMaybeMessage<::io::deephaven::proto::backplane::grpc::Ticket>(arena, *from._impl_.ticket_)
                : nullptr;

  // @@protoc_insertion_point(copy_constructor:io.deephaven.proto.backplane.grpc.TypedTicket)
}
inline PROTOBUF_NDEBUG_INLINE TypedTicket::Impl_::Impl_(
    ::google::protobuf::internal::InternalVisibility visibility,
    ::google::protobuf::Arena* arena)
      : _cached_size_{0},
        type_(arena) {}

inline void TypedTicket::SharedCtor(::_pb::Arena* arena) {
  new (&_impl_) Impl_(internal_visibility(), arena);
  _impl_.ticket_ = {};
}
TypedTicket::~TypedTicket() {
  // @@protoc_insertion_point(destructor:io.deephaven.proto.backplane.grpc.TypedTicket)
  _internal_metadata_.Delete<::google::protobuf::UnknownFieldSet>();
  SharedDtor();
}
inline void TypedTicket::SharedDtor() {
  ABSL_DCHECK(GetArena() == nullptr);
  _impl_.type_.Destroy();
  delete _impl_.ticket_;
  _impl_.~Impl_();
}

PROTOBUF_NOINLINE void TypedTicket::Clear() {
// @@protoc_insertion_point(message_clear_start:io.deephaven.proto.backplane.grpc.TypedTicket)
  PROTOBUF_TSAN_WRITE(&_impl_._tsan_detect_race);
  ::uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  _impl_.type_.ClearToEmpty();
  cached_has_bits = _impl_._has_bits_[0];
  if (cached_has_bits & 0x00000001u) {
    ABSL_DCHECK(_impl_.ticket_ != nullptr);
    _impl_.ticket_->Clear();
  }
  _impl_._has_bits_.Clear();
  _internal_metadata_.Clear<::google::protobuf::UnknownFieldSet>();
}

const char* TypedTicket::_InternalParse(
    const char* ptr, ::_pbi::ParseContext* ctx) {
  ptr = ::_pbi::TcParser::ParseLoop(this, ptr, ctx, &_table_.header);
  return ptr;
}


PROTOBUF_CONSTINIT PROTOBUF_ATTRIBUTE_INIT_PRIORITY1
const ::_pbi::TcParseTable<1, 2, 1, 58, 2> TypedTicket::_table_ = {
  {
    PROTOBUF_FIELD_OFFSET(TypedTicket, _impl_._has_bits_),
    0, // no _extensions_
    2, 8,  // max_field_number, fast_idx_mask
    offsetof(decltype(_table_), field_lookup_table),
    4294967292,  // skipmap
    offsetof(decltype(_table_), field_entries),
    2,  // num_field_entries
    1,  // num_aux_entries
    offsetof(decltype(_table_), aux_entries),
    &_TypedTicket_default_instance_._instance,
    ::_pbi::TcParser::GenericFallback,  // fallback
  }, {{
    // string type = 2;
    {::_pbi::TcParser::FastUS1,
     {18, 63, 0, PROTOBUF_FIELD_OFFSET(TypedTicket, _impl_.type_)}},
    // .io.deephaven.proto.backplane.grpc.Ticket ticket = 1;
    {::_pbi::TcParser::FastMtS1,
     {10, 0, 0, PROTOBUF_FIELD_OFFSET(TypedTicket, _impl_.ticket_)}},
  }}, {{
    65535, 65535
  }}, {{
    // .io.deephaven.proto.backplane.grpc.Ticket ticket = 1;
    {PROTOBUF_FIELD_OFFSET(TypedTicket, _impl_.ticket_), _Internal::kHasBitsOffset + 0, 0,
    (0 | ::_fl::kFcOptional | ::_fl::kMessage | ::_fl::kTvTable)},
    // string type = 2;
    {PROTOBUF_FIELD_OFFSET(TypedTicket, _impl_.type_), -1, 0,
    (0 | ::_fl::kFcSingular | ::_fl::kUtf8String | ::_fl::kRepAString)},
  }}, {{
    {::_pbi::TcParser::GetTable<::io::deephaven::proto::backplane::grpc::Ticket>()},
  }}, {{
    "\55\0\4\0\0\0\0\0"
    "io.deephaven.proto.backplane.grpc.TypedTicket"
    "type"
  }},
};

::uint8_t* TypedTicket::_InternalSerialize(
    ::uint8_t* target,
    ::google::protobuf::io::EpsCopyOutputStream* stream) const {
  // @@protoc_insertion_point(serialize_to_array_start:io.deephaven.proto.backplane.grpc.TypedTicket)
  ::uint32_t cached_has_bits = 0;
  (void)cached_has_bits;

  cached_has_bits = _impl_._has_bits_[0];
  // .io.deephaven.proto.backplane.grpc.Ticket ticket = 1;
  if (cached_has_bits & 0x00000001u) {
    target = ::google::protobuf::internal::WireFormatLite::InternalWriteMessage(
        1, _Internal::ticket(this),
        _Internal::ticket(this).GetCachedSize(), target, stream);
  }

  // string type = 2;
  if (!this->_internal_type().empty()) {
    const std::string& _s = this->_internal_type();
    ::google::protobuf::internal::WireFormatLite::VerifyUtf8String(
        _s.data(), static_cast<int>(_s.length()), ::google::protobuf::internal::WireFormatLite::SERIALIZE, "io.deephaven.proto.backplane.grpc.TypedTicket.type");
    target = stream->WriteStringMaybeAliased(2, _s, target);
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    target =
        ::_pbi::WireFormat::InternalSerializeUnknownFieldsToArray(
            _internal_metadata_.unknown_fields<::google::protobuf::UnknownFieldSet>(::google::protobuf::UnknownFieldSet::default_instance), target, stream);
  }
  // @@protoc_insertion_point(serialize_to_array_end:io.deephaven.proto.backplane.grpc.TypedTicket)
  return target;
}

::size_t TypedTicket::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:io.deephaven.proto.backplane.grpc.TypedTicket)
  ::size_t total_size = 0;

  ::uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  // string type = 2;
  if (!this->_internal_type().empty()) {
    total_size += 1 + ::google::protobuf::internal::WireFormatLite::StringSize(
                                    this->_internal_type());
  }

  // .io.deephaven.proto.backplane.grpc.Ticket ticket = 1;
  cached_has_bits = _impl_._has_bits_[0];
  if (cached_has_bits & 0x00000001u) {
    total_size +=
        1 + ::google::protobuf::internal::WireFormatLite::MessageSize(*_impl_.ticket_);
  }

  return MaybeComputeUnknownFieldsSize(total_size, &_impl_._cached_size_);
}

const ::google::protobuf::Message::ClassData TypedTicket::_class_data_ = {
    TypedTicket::MergeImpl,
    nullptr,  // OnDemandRegisterArenaDtor
};
const ::google::protobuf::Message::ClassData* TypedTicket::GetClassData() const {
  return &_class_data_;
}

void TypedTicket::MergeImpl(::google::protobuf::Message& to_msg, const ::google::protobuf::Message& from_msg) {
  auto* const _this = static_cast<TypedTicket*>(&to_msg);
  auto& from = static_cast<const TypedTicket&>(from_msg);
  // @@protoc_insertion_point(class_specific_merge_from_start:io.deephaven.proto.backplane.grpc.TypedTicket)
  ABSL_DCHECK_NE(&from, _this);
  ::uint32_t cached_has_bits = 0;
  (void) cached_has_bits;

  if (!from._internal_type().empty()) {
    _this->_internal_set_type(from._internal_type());
  }
  if ((from._impl_._has_bits_[0] & 0x00000001u) != 0) {
    _this->_internal_mutable_ticket()->::io::deephaven::proto::backplane::grpc::Ticket::MergeFrom(
        from._internal_ticket());
  }
  _this->_internal_metadata_.MergeFrom<::google::protobuf::UnknownFieldSet>(from._internal_metadata_);
}

void TypedTicket::CopyFrom(const TypedTicket& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:io.deephaven.proto.backplane.grpc.TypedTicket)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

PROTOBUF_NOINLINE bool TypedTicket::IsInitialized() const {
  return true;
}

::_pbi::CachedSize* TypedTicket::AccessCachedSize() const {
  return &_impl_._cached_size_;
}
void TypedTicket::InternalSwap(TypedTicket* PROTOBUF_RESTRICT other) {
  using std::swap;
  auto* arena = GetArena();
  ABSL_DCHECK_EQ(arena, other->GetArena());
  _internal_metadata_.InternalSwap(&other->_internal_metadata_);
  swap(_impl_._has_bits_[0], other->_impl_._has_bits_[0]);
  ::_pbi::ArenaStringPtr::InternalSwap(&_impl_.type_, &other->_impl_.type_, arena);
  swap(_impl_.ticket_, other->_impl_.ticket_);
}

::google::protobuf::Metadata TypedTicket::GetMetadata() const {
  return ::_pbi::AssignDescriptors(
      &descriptor_table_deephaven_2fproto_2fticket_2eproto_getter, &descriptor_table_deephaven_2fproto_2fticket_2eproto_once,
      file_level_metadata_deephaven_2fproto_2fticket_2eproto[1]);
}
// @@protoc_insertion_point(namespace_scope)
}  // namespace grpc
}  // namespace backplane
}  // namespace proto
}  // namespace deephaven
}  // namespace io
namespace google {
namespace protobuf {
}  // namespace protobuf
}  // namespace google
// @@protoc_insertion_point(global_scope)
#include "google/protobuf/port_undef.inc"
