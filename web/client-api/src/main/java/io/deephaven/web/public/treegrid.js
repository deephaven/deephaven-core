import {dh} from './dh-core.js';
const {Table, FilterValue} = dh;
/**
 * Simple es6 class encapsulating some basic rendering for a tree table in a grid, allowing
 * expanding and collapsing nodes, and filtering and sorting columns.
 */
class TreeGrid {
  constructor(treeTable) {
    this.treeTable = treeTable;
    this.treeTable.addEventListener(Table.EVENT_UPDATED, e => this.repaint(e.detail));

    this.rootElement = document.createElement('table');
    this.rootElement.addEventListener('click', e => this.toggle(e));

    this.offset = 0;
    this.height = 100;

    this.treeTable.setViewport(this.offset, this.offset + this.height);

    // set up filter
    this.topMenu = document.createElement('div');
    this.topMenu.id = 'topMenu';
    document.body.appendChild(this.topMenu);
  }

  get element() {
    return this.rootElement;
  }

  toggle(clickEvent) {
    var cellElt = clickEvent.target;
    if (cellElt.classList.contains('togglable')) {
      var rowElt = cellElt.parentElement;
      var rowNumber = (rowElt.rowIndex - 1) + this.offset;

      if (cellElt.classList.contains('expanded')) {
        this.treeTable.collapse(rowNumber);
      } else {
        this.treeTable.expand(rowNumber);
      }
    }
  }

  repaint(viewportData) {
    console.log("updated event", viewportData);
    // for the sake of the example, redrawing all headers each time,
    // should only do this when a change occurs
    var oldHead = this.rootElement.querySelector('thead');
    oldHead && this.rootElement.removeChild(oldHead);
    var oldBody = this.rootElement.querySelector('tbody.body');
    oldBody && this.rootElement.removeChild(oldBody);

    var header = document.createElement('thead');
    var headerRow = document.createElement('tr');
    // build a very simple header
    // first add a "hierarchy" column - this could just as easily be part of another column to indent its content
    headerRow.appendChild(document.createElement('td'));
    viewportData.columns.forEach(c => {
      var td = document.createElement('td');
      td.innerText = c.name;
      //look for an existing sort and show it so it is still visible on re-render
      var sort = this.treeTable.sort.find(s => s.column === c);
      if (sort != null) {
        td.className = sort.direction.toLowerCase();
      }
      td.onclick = event => {
        // toggle sort direction if it was already set
        var existingSort = td.className;
        // clear all other sorts visually
        headerRow.querySelectorAll('.asc,.desc').forEach(elt => elt.className = '');
        if (existingSort.indexOf('asc') === -1) {
          if (existingSort.indexOf('desc') === -1) {
            td.className = 'asc';
            this.treeTable.applySort([c.sort().asc()]);
          } else {
            // remove sort
            this.treeTable.applySort([]);
          }
        } else {
          td.className = 'desc';
          this.treeTable.applySort([c.sort().desc()]);
        }

        // load content again
        this.treeTable.setViewport(this.offset, this.offset + this.height);
      };
      headerRow.appendChild(td);
    });
    header.appendChild(headerRow);
    this.rootElement.appendChild(header);

    var tbody = document.createElement('tbody');
    tbody.className = 'body';

    var rows = viewportData.rows;
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var tr = document.createElement('tr');

      // first, the hierarchy cell - indent based on current depth, and draw the expand/collapse tool
      var hierarchyCell = document.createElement('td');
      hierarchyCell.style.paddingLeft = row.depth * 12 + 'px';
      if (row.hasChildren) {
        hierarchyCell.classList.add('togglable');
        if (row.isExpanded) {
          hierarchyCell.textContent = '–';
          hierarchyCell.classList.add('expanded');
        } else {
          hierarchyCell.textContent = '+';
          hierarchyCell.classList.add('collapsed');
        }
      } else {
        hierarchyCell.classList.add('leaf');
        hierarchyCell.innerHTML = '&nbsp;';
      }
      tr.appendChild(hierarchyCell);

      for (var j = 0; j < viewportData.columns.length; j++) {
        var td = document.createElement('td');
        var col = viewportData.columns[j];

        td.textContent = row.get(col);
        td.internalValue = row.get(col);
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
      tbody.oncontextmenu = e => this._showFilterMenu(e);
    }
    this.rootElement.insertBefore(tbody, this.rootElement.firstChild);
    this.rootElement.insertBefore(header, this.rootElement.firstChild);
  }

  // Replace all current filters with this new set, update the UI list of filters, and set the viewport
  updateFilters(newFilters) {
    this.treeTable.applyFilter(newFilters);
    this.treeTable.setViewport(this.offset, this.offset + this.height);
    // while (activeFilters.hasChildNodes()) {
    //   activeFilters.removeChild(activeFilters.lastChild);
    // }
    // newFilters.forEach(filter => {
    //   var li = document.createElement('li');
    //   li.textContent = filter.toString();
    //   activeFilters.appendChild(li);
    // });
  }

  clear() {
    this.updateFilters([]);
  }

  // When a user right-clicks a cell, offer filters based on that cell's data
  _showFilterMenu(event) {
    var target = event.target;
    var row = target.parentElement;
    if (target.tagName !== 'TD') {
      return;
    }
    // track the Column we are in, and the value of the current cell
    var column = this.treeTable.columns[Array.prototype.indexOf.call(row.children, target) - 1];

    // show a new menu here, with an item for each filter
    var ul = document.createElement("ul");
    Object.entries(TreeGrid.filterSetup).forEach((entry) => {
      var li = document.createElement("li");
      li.textContent = entry[0];
      var buildFilter = entry[1].build;
      var valueOptions = entry[1].value;
      li.onclick = event => {
        // when this item is clicked, check the options for the value
        var current = this.treeTable.filter;
        if (valueOptions === 'NONE') {
          // if NONE, just create the filter
          current.push(buildFilter(column));
          this.updateFilters(current);
          this.hideMenu();
        } else if (valueOptions === 'MATCH_COLUMN') {
          // if MATCH_COLUMN, start from the cell value, let the user edit, assume the same type as column
          //TODO convert value more completely
          var value = target.internalValue;
          if (typeof value === 'string') {
            value = FilterValue.ofString(value);
          } else /*if (typeof value === 'number')*/ {
            //otherwise, we'll just assume it is a number, ofNumber will throw exceptions if not
            value = FilterValue.ofNumber(value);
          }
          //TODO make editable
          current.push(buildFilter(column, value));
          this.updateFilters(current);
          this.hideMenu();
        } else if (valueOptions === 'MATCH_COLUMN_LIST') {
          // if MATCH_COLUMN_LIST, same as MATCH_COLUMN except support a list
          //TODO convert value more completely
          var value = target.internalValue;
          if (typeof value === 'string') {
            value = FilterValue.ofString(value);
          } else /*if (typeof value === 'number')*/ {
            //otherwise, we'll just assume it is a number, ofNumber will throw exceptions if not
            value = FilterValue.ofNumber(value);
          }

          //TODO make editable
          current.push(buildFilter(column, [value]));
          this.updateFilters(current);
          this.hideMenu();
        }

      };
      ul.appendChild(li);
    });
    this.showMenu(event.pageX, event.pageY, ul);

    // cancel the current right-click to avoid browser context menu
    event.preventDefault();
  }

  /*
     Super-simple menu bookkeeping functions, allowing only one visible at a time,
     and hidden if a user clicks on another dom element.
   */
  hideOnClickOutOfMenu(event) {
    var target = event.target;
    while (target != null) {
      if (target === this.topMenu) {
        return;
      }
      target = target.parentElement;
    }
    //event.preventDefault();
    this.hideMenu();
  }
  hideMenu() {
    this.topMenu.parentElement.removeChild(this.topMenu);
    document.body.removeEventListener("mousedown", this.hideOnClickOutOfMenu.bind(this), true);
  }
  showMenu(x, y, children) {
    // remove before starting updates
    this.topMenu.parentElement && this.topMenu.parentElement.removeChild(this.topMenu);
    // update the menu with the new options
    while (this.topMenu.hasChildNodes()) {
      this.topMenu.removeChild(this.topMenu.lastChild);
    }
    this.topMenu.appendChild(children);

    // move the "menu" of options to here
    this.topMenu.style.display = 'block';
    this.topMenu.style.top = y + 'px';
    this.topMenu.style.left = x + 'px';

    //append to dom again
    document.body.appendChild(this.topMenu);

    // watch for any other clicks
    //TODO wrong!
    document.body.addEventListener("mousedown", this.hideOnClickOutOfMenu.bind(this), true);
  }

  static get filterSetup() {
    return {
      "Equals...": {
        build: (column, value) => column.filter().eq(value),
        value: 'MATCH_COLUMN'
      },
      "Equals Ignore Case...": {
        build: (column, value) => column.filter().eqIgnoreCase(value),
        value: 'MATCH_COLUMN'
      },
      "Not Equals...": {
        build: (column, value) => column.filter().notEq(value),
        value: 'MATCH_COLUMN'
      },
      "Not Equals Ignore Case...": {
        build: (column, value) => column.filter().notEqIgnoreCase(value),
        value: 'MATCH_COLUMN'
      },
      "Greater Than...": {
        build: (column, value) => column.filter().greaterThan(value),
        value: 'MATCH_COLUMN'
      },
      "Greater Than Or Equal To...": {
        build: (column, value) => column.filter().greaterThanOrEqualTo(value),
        value: 'MATCH_COLUMN'
      },
      "Less Than...": {
        build: (column, value) => column.filter().lessThan(value),
        value: 'MATCH_COLUMN'
      },
      "Less Than Or Equal To...": {
        build: (column, value) => column.filter().lessThanOrEqualTo(value),
        value: 'MATCH_COLUMN'
      },
      "In...": {
        build: (column, value) => column.filter().in(value),
        value: 'MATCH_COLUMN_LIST'
      },
      "Not In...": {
        build: (column, value) => column.filter().notIn(value),
        value: 'MATCH_COLUMN_LIST'
      },
      "In Ignore Case...": {
        build: (column, value) => column.filter().inIgnoreCase(value),
        value: 'MATCH_COLUMN_LIST'
      },
      "Not In Ignore Case...": {
        build: (column, value) => column.filter().notInIgnoreCase(value),
        value: 'MATCH_COLUMN_LIST'
      },
      "Is True": {
        build: (column, value) => column.filter().isTrue(),
        value: 'NONE'
      },
      "Is False": {
        build: (column, value) => column.filter().isFalse(),
        value: 'NONE'
      },
      "Is Null": {
        build: (column, value) => column.filter().isNull(),
        value: 'NONE'
      },
      "Starts With...": {
        build: (column, value) => column.filter().invoke("startsWith", value),
        value: 'MATCH_COLUMN'
      },
      "Ends With...": {
        build: (column, value) => column.filter().invoke("endsWith", value),
        value: 'MATCH_COLUMN'
      },
      "Matches...": {
        build: (column, value) => column.filter().invoke("matches", value),
        value: 'MATCH_COLUMN'
      },
      "Contains...": {
        build: (column, value) => column.filter().invoke("contains", value),
        value: 'MATCH_COLUMN'
      },
    }
  };
}
