/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/client/server/server.h"
#include "deephaven/client/utility/executor.h"
#include "deephaven/dhcore/utility/callbacks.h"

namespace deephaven::client {
namespace impl {

class ClientImpl {
  struct Private {
  };
  typedef deephaven::client::server::Server Server;
  typedef deephaven::client::utility::Executor Executor;

  template<typename... Args>
  using SFCallback = deephaven::dhcore::utility::SFCallback<Args...>;

public:
  static std::shared_ptr<ClientImpl> create(std::shared_ptr<Server> server,
      std::shared_ptr<Executor> executor, std::shared_ptr<Executor> flightExecutor, const std::string &sessionType);

  ClientImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&managerImpl);
  ~ClientImpl();

  void shutdown() {
    managerImpl_->shutdown();
  }

  const std::shared_ptr<TableHandleManagerImpl> &managerImpl() const { return managerImpl_; }

private:
  std::shared_ptr<TableHandleManagerImpl> managerImpl_;
};
}  // namespace impl
}  // namespace deephaven::client
