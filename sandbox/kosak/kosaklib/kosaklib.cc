#include <functional>
#include <memory>
#include <thread>
#include "kosaklib.h"

namespace zambonitown {
typedef const char *(stringAllocFunc_t)(const char *);

stringAllocFunc_t *stringAllocCallback;

class ClientOptions {
public:
  int &Ttl() { return ttl_; }
  const int &Ttl() const { return ttl_; }

private:
  int ttl_ = 0;
};

class ClientImpl : std::enable_shared_from_this<ClientImpl> {
public:
  static std::shared_ptr<ClientImpl> create(int ttl) {
    if (ttl == 0) {
      throw std::runtime_error("I am sad about ttl = 0");
    }
    return std::make_shared<ClientImpl>(ttl);
  }

  explicit ClientImpl(int ttl) : ttl_(ttl) {}

private:
  int ttl_ = 0;
};

class Client {
public:
  static Client Connect(const std::string &target, const ClientOptions &options) {
    std::cout << "Connecting to " << target << '\n';
    std::cout << "options.ttl = " << options.Ttl() << '\n';
    auto impl = ClientImpl::create(options.Ttl());
    return Client(std::move(impl));
  }

  Client(Client &&other) noexcept = default;

  ~Client() {
    std::cout << "Destructing Client -- who the hell called you\n";
  }

private:
  explicit Client(std::shared_ptr<ClientImpl> impl) : impl_(std::move(impl)) {
    std::cout << "Creating Client\n";
  }

  std::shared_ptr<ClientImpl> impl_;
};

class WrappedException {
public:
  explicit WrappedException(std::exception_ptr eptr) {
    try {
      std::rethrow_exception(eptr);
    } catch (std::exception &e) {
      what_ = e.what();
    } catch (...) {
      what_ = "(unknown exception)";
    }
  }

  const std::string &What() const { return what_; }

private:
  std::string what_;
};

template<typename T>
struct ResultOrError {
  template<typename U>
  void SetResult(const U &callback) {
    try {
      result_ = callback();
      error_ = nullptr;
    } catch (...) {
      result_ = nullptr;
      error_ = new WrappedException(std::current_exception());
    }
  }

  T *result_ = nullptr;
  void *error_ = nullptr;
};

struct ZamboniString {
  static const ZamboniString *create(std::string_view what);
};
}  // namespace zambonitown

using zambonitown::Client;
using zambonitown::ClientOptions;
using zambonitown::ResultOrError;
using zambonitown::WrappedException;
using zambonitown::stringAllocFunc_t;
using zambonitown::stringAllocCallback;
using zambonitown::ZamboniString;

extern "C" {
void zambonitown_SetStringAllocCallback(stringAllocFunc_t *cb) {
  stringAllocCallback = cb;
}

void zambonitown_TestArrayOfInts(int *data, int size) {
  for (int i = 0; i != size; ++i) {
    data[i] = i * 111 + 5;
  }
}

void zambonitown_ClientOptions_ctor(ResultOrError<ClientOptions> *roe) {
  std::cout << "Inside ClientOptions_ctor\n";
  roe->SetResult([]() {
    return new ClientOptions();
  });
}

void zambonitown_ClientOptions_dtor(ClientOptions *self) {
  delete self;
}

void zambonitown_ClientOptions_Ttl(ClientOptions *self, int ttl) {
  self->Ttl() = ttl;
}

void zambonitown_Client_Connect(const char *s, ClientOptions *options,
    ResultOrError<Client> *roe) {
  roe->SetResult([s, options]() {
    auto client = Client::Connect(s, *options);
    return new Client(std::move(client));
  });
}

void zambonitown_Client_dtor(Client *self) {
  delete self;
}

const ZamboniString *zambonitown_WrappedException_What(WrappedException *self) {
  std::cout << "I'm in here. what is the problem\n";
  std::cout << "What I have is " << self << '\n';
  std::cerr << "BLAH DATA VOID " << (void*)self->What().data() << '\n';
  std::cerr << "BLAH DATA " << self->What().data() << '\n';
  std::cerr << "BLAH SIZE " << self->What().size() << '\n';
  std::cout << self->What() << '\n';
  return ZamboniString::create(self->What());
}

void zambonitown_WrappedException_dtor(void *self) {
  const auto *selfp = static_cast<WrappedException*>(self);
  delete selfp;
}

const ZamboniString *ZamboniString::create(std::string_view what) {
  const char *allocatedString = stringAllocCallback(what.data());
  return reinterpret_cast<const ZamboniString*>(allocatedString);
}
}  // extern "C"
