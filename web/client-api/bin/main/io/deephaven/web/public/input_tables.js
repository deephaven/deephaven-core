// various tools for use when you want to support input tables.
// you should pull in init.js before this file

let button, cleanup;
function initInputTables() {
  let e = document.getElementById('inputs');
  if (!e) {
    e = document.createElement('ul');
    e.id = 'inputs';
    document.body.appendChild(e);
  }
  e.classList.add('inputEditor');
  e.innerHTML = '<li class="disabled">No input table selected</li>';
  button = e.querySelector('button');
  if (!button) {
    button = document.createElement('button');
    button.innerText = 'Add row';
    e.appendChild(button);
  }
}

function rebuildInputTables(it, table) {
  const e = document.getElementById('inputs');
  e.innerHTML = '';
  for (const key of it.keys) {
    // add required editor field
    const col = table.findColumn(key);
    addInputField(key, col, e, true);
  }
  for (const value of it.values) {
    // add optional editor field
    const col = table.findColumn(value);
    addInputField(value, col, e, false);
  }
  e.appendChild(button);
  cleanup && cleanup.call(null);
  // overwrite old callback... /lazy/
  button.onclick = function() {
    const vals = getInputValues(e);
    it.addRow(vals);
  }
}

function getInputValues(e) {
  const vals = Object.create(null);
  e.querySelectorAll('input').forEach(v=>{
    console.log(v);
    const k = v.id.replace(/col-/, '');
    vals[k] = v.value;
  });
  return vals;
}

function addInputField(key, col, e, required) {
  const li = document.createElement('li');
  li.classList.add('input');
  li.title = required ? 'Required' : 'Optional';
  li.innerHTML = `<label>${key}: ${required?'<sup>(*)</sup>':''} </label><input type='text' id='col-${key}' placeholder='(${col.type})' />`;
  e.appendChild(li);
}
