/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include "deephaven/client/highlevel/client.h"
#include "deephaven/client/lowlevel/server.h"
#include "deephaven/client/utility/callbacks.h"
#include "deephaven/client/utility/executor.h"
#include "deephaven/client/utility/utility.h"

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {

class ClientImpl {
  struct Private {};
  typedef deephaven::client::lowlevel::Server Server;
  typedef deephaven::client::utility::Executor Executor;

  template<typename... Args>
  using SFCallback = deephaven::client::utility::SFCallback<Args...>;

public:
  static std::shared_ptr<ClientImpl> create(std::shared_ptr<Server> server,
      std::shared_ptr<Executor> executor, std::shared_ptr<Executor> flightExecutor);

  ClientImpl(Private, std::shared_ptr<TableHandleManagerImpl> &&managerImpl);
  ~ClientImpl();

  const std::shared_ptr<TableHandleManagerImpl> &managerImpl() const { return managerImpl_; }

private:
  std::shared_ptr<TableHandleManagerImpl> managerImpl_;
};
}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
