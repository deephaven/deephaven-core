/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/client/server/server.h"
#include "deephaven/client/utility/executor.h"
#include "deephaven/dhcore/utility/callbacks.h"

namespace deephaven::client::impl {
class ClientImpl {
  struct Private {
  };
  using Server = deephaven::client::server::Server;
  using Executor = deephaven::client::utility::Executor;

  template<typename... Args>
  using SFCallback = deephaven::dhcore::utility::SFCallback<Args...>;

public:
  [[nodiscard]]
  static std::shared_ptr<ClientImpl> Create(std::shared_ptr<Server> server,
      std::shared_ptr<Executor> executor, std::shared_ptr<Executor> flight_executor, const std::string &session_type);

  ClientImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&manager_impl);
  ~ClientImpl();

  void Shutdown() {
    managerImpl_->Shutdown();
  }

  [[nodiscard]]
  const std::shared_ptr<TableHandleManagerImpl> &ManagerImpl() const { return managerImpl_; }

private:
  std::shared_ptr<TableHandleManagerImpl> managerImpl_;
};
}  // namespace deephaven::client::impl
