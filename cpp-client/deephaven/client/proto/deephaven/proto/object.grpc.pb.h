// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: deephaven/proto/object.proto
// Original file comments:
//
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#ifndef GRPC_deephaven_2fproto_2fobject_2eproto__INCLUDED
#define GRPC_deephaven_2fproto_2fobject_2eproto__INCLUDED

#include "deephaven/proto/object.pb.h"

#include <functional>
#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>
#include <grpcpp/support/client_callback.h>
#include <grpcpp/client_context.h>
#include <grpcpp/completion_queue.h>
#include <grpcpp/support/message_allocator.h>
#include <grpcpp/support/method_handler.h>
#include <grpcpp/impl/proto_utils.h>
#include <grpcpp/impl/rpc_method.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/impl/server_callback_handlers.h>
#include <grpcpp/server_context.h>
#include <grpcpp/impl/service_type.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/stub_options.h>
#include <grpcpp/support/sync_stream.h>

namespace io {
namespace deephaven {
namespace proto {
namespace backplane {
namespace grpc {

class ObjectService final {
 public:
  static constexpr char const* service_full_name() {
    return "io.deephaven.proto.backplane.grpc.ObjectService";
  }
  class StubInterface {
   public:
    virtual ~StubInterface() {}
    virtual ::grpc::Status FetchObject(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest& request, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* response) = 0;
    std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>> AsyncFetchObject(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest& request, ::grpc::CompletionQueue* cq) {
      return std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>>(AsyncFetchObjectRaw(context, request, cq));
    }
    std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>> PrepareAsyncFetchObject(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest& request, ::grpc::CompletionQueue* cq) {
      return std::unique_ptr< ::grpc::ClientAsyncResponseReaderInterface< ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>>(PrepareAsyncFetchObjectRaw(context, request, cq));
    }
    class async_interface {
     public:
      virtual ~async_interface() {}
      virtual void FetchObject(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* request, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* response, std::function<void(::grpc::Status)>) = 0;
      virtual void FetchObject(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* request, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* response, ::grpc::ClientUnaryReactor* reactor) = 0;
    };
    typedef class async_interface experimental_async_interface;
    virtual class async_interface* async() { return nullptr; }
    class async_interface* experimental_async() { return async(); }
   private:
    virtual ::grpc::ClientAsyncResponseReaderInterface< ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>* AsyncFetchObjectRaw(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest& request, ::grpc::CompletionQueue* cq) = 0;
    virtual ::grpc::ClientAsyncResponseReaderInterface< ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>* PrepareAsyncFetchObjectRaw(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest& request, ::grpc::CompletionQueue* cq) = 0;
  };
  class Stub final : public StubInterface {
   public:
    Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options = ::grpc::StubOptions());
    ::grpc::Status FetchObject(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest& request, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* response) override;
    std::unique_ptr< ::grpc::ClientAsyncResponseReader< ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>> AsyncFetchObject(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest& request, ::grpc::CompletionQueue* cq) {
      return std::unique_ptr< ::grpc::ClientAsyncResponseReader< ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>>(AsyncFetchObjectRaw(context, request, cq));
    }
    std::unique_ptr< ::grpc::ClientAsyncResponseReader< ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>> PrepareAsyncFetchObject(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest& request, ::grpc::CompletionQueue* cq) {
      return std::unique_ptr< ::grpc::ClientAsyncResponseReader< ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>>(PrepareAsyncFetchObjectRaw(context, request, cq));
    }
    class async final :
      public StubInterface::async_interface {
     public:
      void FetchObject(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* request, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* response, std::function<void(::grpc::Status)>) override;
      void FetchObject(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* request, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* response, ::grpc::ClientUnaryReactor* reactor) override;
     private:
      friend class Stub;
      explicit async(Stub* stub): stub_(stub) { }
      Stub* stub() { return stub_; }
      Stub* stub_;
    };
    class async* async() override { return &async_stub_; }

   private:
    std::shared_ptr< ::grpc::ChannelInterface> channel_;
    class async async_stub_{this};
    ::grpc::ClientAsyncResponseReader< ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>* AsyncFetchObjectRaw(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest& request, ::grpc::CompletionQueue* cq) override;
    ::grpc::ClientAsyncResponseReader< ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>* PrepareAsyncFetchObjectRaw(::grpc::ClientContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest& request, ::grpc::CompletionQueue* cq) override;
    const ::grpc::internal::RpcMethod rpcmethod_FetchObject_;
  };
  static std::unique_ptr<Stub> NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options = ::grpc::StubOptions());

  class Service : public ::grpc::Service {
   public:
    Service();
    virtual ~Service();
    virtual ::grpc::Status FetchObject(::grpc::ServerContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* request, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* response);
  };
  template <class BaseClass>
  class WithAsyncMethod_FetchObject : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service* /*service*/) {}
   public:
    WithAsyncMethod_FetchObject() {
      ::grpc::Service::MarkMethodAsync(0);
    }
    ~WithAsyncMethod_FetchObject() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable synchronous version of this method
    ::grpc::Status FetchObject(::grpc::ServerContext* /*context*/, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* /*request*/, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* /*response*/) override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
    void RequestFetchObject(::grpc::ServerContext* context, ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* request, ::grpc::ServerAsyncResponseWriter< ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>* response, ::grpc::CompletionQueue* new_call_cq, ::grpc::ServerCompletionQueue* notification_cq, void *tag) {
      ::grpc::Service::RequestAsyncUnary(0, context, request, response, new_call_cq, notification_cq, tag);
    }
  };
  typedef WithAsyncMethod_FetchObject<Service > AsyncService;
  template <class BaseClass>
  class WithCallbackMethod_FetchObject : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service* /*service*/) {}
   public:
    WithCallbackMethod_FetchObject() {
      ::grpc::Service::MarkMethodCallback(0,
          new ::grpc::internal::CallbackUnaryHandler< ::io::deephaven::proto::backplane::grpc::FetchObjectRequest, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>(
            [this](
                   ::grpc::CallbackServerContext* context, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* request, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* response) { return this->FetchObject(context, request, response); }));}
    void SetMessageAllocatorFor_FetchObject(
        ::grpc::MessageAllocator< ::io::deephaven::proto::backplane::grpc::FetchObjectRequest, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>* allocator) {
      ::grpc::internal::MethodHandler* const handler = ::grpc::Service::GetHandler(0);
      static_cast<::grpc::internal::CallbackUnaryHandler< ::io::deephaven::proto::backplane::grpc::FetchObjectRequest, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>*>(handler)
              ->SetMessageAllocator(allocator);
    }
    ~WithCallbackMethod_FetchObject() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable synchronous version of this method
    ::grpc::Status FetchObject(::grpc::ServerContext* /*context*/, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* /*request*/, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* /*response*/) override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
    virtual ::grpc::ServerUnaryReactor* FetchObject(
      ::grpc::CallbackServerContext* /*context*/, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* /*request*/, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* /*response*/)  { return nullptr; }
  };
  typedef WithCallbackMethod_FetchObject<Service > CallbackService;
  typedef CallbackService ExperimentalCallbackService;
  template <class BaseClass>
  class WithGenericMethod_FetchObject : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service* /*service*/) {}
   public:
    WithGenericMethod_FetchObject() {
      ::grpc::Service::MarkMethodGeneric(0);
    }
    ~WithGenericMethod_FetchObject() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable synchronous version of this method
    ::grpc::Status FetchObject(::grpc::ServerContext* /*context*/, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* /*request*/, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* /*response*/) override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
  };
  template <class BaseClass>
  class WithRawMethod_FetchObject : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service* /*service*/) {}
   public:
    WithRawMethod_FetchObject() {
      ::grpc::Service::MarkMethodRaw(0);
    }
    ~WithRawMethod_FetchObject() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable synchronous version of this method
    ::grpc::Status FetchObject(::grpc::ServerContext* /*context*/, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* /*request*/, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* /*response*/) override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
    void RequestFetchObject(::grpc::ServerContext* context, ::grpc::ByteBuffer* request, ::grpc::ServerAsyncResponseWriter< ::grpc::ByteBuffer>* response, ::grpc::CompletionQueue* new_call_cq, ::grpc::ServerCompletionQueue* notification_cq, void *tag) {
      ::grpc::Service::RequestAsyncUnary(0, context, request, response, new_call_cq, notification_cq, tag);
    }
  };
  template <class BaseClass>
  class WithRawCallbackMethod_FetchObject : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service* /*service*/) {}
   public:
    WithRawCallbackMethod_FetchObject() {
      ::grpc::Service::MarkMethodRawCallback(0,
          new ::grpc::internal::CallbackUnaryHandler< ::grpc::ByteBuffer, ::grpc::ByteBuffer>(
            [this](
                   ::grpc::CallbackServerContext* context, const ::grpc::ByteBuffer* request, ::grpc::ByteBuffer* response) { return this->FetchObject(context, request, response); }));
    }
    ~WithRawCallbackMethod_FetchObject() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable synchronous version of this method
    ::grpc::Status FetchObject(::grpc::ServerContext* /*context*/, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* /*request*/, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* /*response*/) override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
    virtual ::grpc::ServerUnaryReactor* FetchObject(
      ::grpc::CallbackServerContext* /*context*/, const ::grpc::ByteBuffer* /*request*/, ::grpc::ByteBuffer* /*response*/)  { return nullptr; }
  };
  template <class BaseClass>
  class WithStreamedUnaryMethod_FetchObject : public BaseClass {
   private:
    void BaseClassMustBeDerivedFromService(const Service* /*service*/) {}
   public:
    WithStreamedUnaryMethod_FetchObject() {
      ::grpc::Service::MarkMethodStreamed(0,
        new ::grpc::internal::StreamedUnaryHandler<
          ::io::deephaven::proto::backplane::grpc::FetchObjectRequest, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>(
            [this](::grpc::ServerContext* context,
                   ::grpc::ServerUnaryStreamer<
                     ::io::deephaven::proto::backplane::grpc::FetchObjectRequest, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse>* streamer) {
                       return this->StreamedFetchObject(context,
                         streamer);
                  }));
    }
    ~WithStreamedUnaryMethod_FetchObject() override {
      BaseClassMustBeDerivedFromService(this);
    }
    // disable regular version of this method
    ::grpc::Status FetchObject(::grpc::ServerContext* /*context*/, const ::io::deephaven::proto::backplane::grpc::FetchObjectRequest* /*request*/, ::io::deephaven::proto::backplane::grpc::FetchObjectResponse* /*response*/) override {
      abort();
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
    }
    // replace default version of method with streamed unary
    virtual ::grpc::Status StreamedFetchObject(::grpc::ServerContext* context, ::grpc::ServerUnaryStreamer< ::io::deephaven::proto::backplane::grpc::FetchObjectRequest,::io::deephaven::proto::backplane::grpc::FetchObjectResponse>* server_unary_streamer) = 0;
  };
  typedef WithStreamedUnaryMethod_FetchObject<Service > StreamedUnaryService;
  typedef Service SplitStreamedService;
  typedef WithStreamedUnaryMethod_FetchObject<Service > StreamedService;
};

}  // namespace grpc
}  // namespace backplane
}  // namespace proto
}  // namespace deephaven
}  // namespace io


#endif  // GRPC_deephaven_2fproto_2fobject_2eproto__INCLUDED
