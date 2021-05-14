var url = new URL('/socket', window.location);
if (url.protocol === 'http:') {
  url.protocol = 'ws:';
} else {
  url.protocol = 'wss:';
}

window.c = new dh.Client(url.href);
c.addEventListener(dh.Client.EVENT_CONNECT, () => {
  window.connected && window.connected();
  c.login({username:'dh',token:'dh',type:'password'}).then(result => {
    console.log("login successful");
    window.started && window.started();
  });
});

function el(id) {
  return document.getElementById(id);
}
function newEl(tagName) {
  return document.createElement(tagName);
}

function ensureLog() {
  var log = el('log');
  if (log) return log;
  log = newEl('div');
  log.innerHTML =`
   <h3>Log</h3>
   <button onclick="clearLog()">Clear Log</button>
   <ul id="log"></ul>  
`;
  document.body.appendChild(log);
  return log.querySelector('#log');
// <div>
}
function log(message) {
  const logMessage = newEl('li');
  logMessage.innerText = message;
  if (arguments.length > 1) {
    logMessage.classList.add(
      Array.prototype.slice.call(arguments, 1)
    );
  }
  ensureLog().appendChild(logMessage)
}
function clearLog() {
  ensureLog().innerHTML = '';
}

function monitor(name, sock) {
  if (monitor.socks.has(sock)) {
    return;
  }
  if (!monitor.statusEl) {
    monitor.statusEl = newEl('div');
    monitor.statusEl.innerHTML = '<h3>Socket Status</h3>';
    document.body.appendChild(monitor.statusEl);
  }
  var el = newEl('div');
  el.innerHTML = `
    <span>${name}: </span>
    <span class='state' />
  `;
  monitor.statusEl.appendChild(el);

  monitor.socks.set(sock, {name: name, el: el.querySelector('.state'), state: 'Unknown'});
  monitor.setState(sock, 'Open');
  sock.addEventListener('open', function() {
    monitor.setState(sock, 'Open');
  });
  sock.addEventListener('close', function() {
    monitor.setState(sock, 'Close');
  });
  sock.addEventListener('error', function() {
    monitor.setState(sock, 'Error');
  });
  sock.addEventListener('message', function() {
    log(`${name}: ` + event.data, 'rpc');
  });
}
monitor.socks = new Map();
monitor.setState = (sock, state) => {
  const data = monitor.socks.get(sock);
  data.el.innerHTML = `${state} (${sock.readyState})`;
  data.el.state = state;
};
