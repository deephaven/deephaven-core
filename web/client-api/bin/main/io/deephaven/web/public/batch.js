
var filterSetup = {
  "Equals..." : {
    build : (column, value) => column.filter().eq(value),
    value: 'MATCH_COLUMN'
  },
  "Equals Ignore Case..." : {
    build : (column, value) => column.filter().eqIgnoreCase(value),
    value: 'MATCH_COLUMN'
  },
  "Not Equals..." : {
    build : (column, value) => column.filter().notEq(value),
    value: 'MATCH_COLUMN'
  },
  "Not Equals Ignore Case..." : {
    build : (column, value) => column.filter().notEqIgnoreCase(value),
    value: 'MATCH_COLUMN'
  },
  "Greater Than..." : {
    build : (column, value) => column.filter().greaterThan(value),
    value: 'MATCH_COLUMN'
  },
  "Greater Than Or Equal To..." : {
    build : (column, value) => column.filter().greaterThanOrEqualTo(value),
    value: 'MATCH_COLUMN'
  },
  "Less Than..." : {
    build : (column, value) => column.filter().lessThan(value),
    value: 'MATCH_COLUMN'
  },
  "Less Than Or Equal To..." : {
    build : (column, value) => column.filter().lessThanOrEqualTo(value),
    value: 'MATCH_COLUMN'
  },
  "In..." : {
    build : (column, value) => column.filter().in(value),
    value: 'MATCH_COLUMN_LIST'
  },
  "Not In..." : {
    build : (column, value) => column.filter().notIn(value),
    value: 'MATCH_COLUMN_LIST'
  },
  "In Ignore Case..." : {
    build : (column, value) => column.filter().inIgnoreCase(value),
    value: 'MATCH_COLUMN_LIST'
  },
  "Not In Ignore Case..." : {
    build : (column, value) => column.filter().notInIgnoreCase(value),
    value: 'MATCH_COLUMN_LIST'
  },
  "Is True" : {
    build : (column, value) => column.filter().isTrue(),
    value: 'NONE'
  },
  "Is False" : {
    build : (column, value) => column.filter().isFalse(),
    value: 'NONE'
  },
  "Is Null" : {
    build : (column, value) => column.filter().isNull(),
    value: 'NONE'
  },
  "Starts With..." : {
    build : (column, value) => column.filter().invoke("startsWith", value),
    value: 'MATCH_COLUMN'
  },
  "Ends With..." : {
    build : (column, value) => column.filter().invoke("endsWith", value),
    value: 'MATCH_COLUMN'
  },
  "Matches..." : {
    build : (column, value) => column.filter().invoke("matches", value),
    value: 'MATCH_COLUMN'
  },
  "Contains..." : {
    build : (column, value) => column.filter().invoke("contains", value),
    value: 'MATCH_COLUMN'
  },

};

function cleanSort(td) {
  td.className = '';
  delete td.existingSort;
  delete td.sortLog;
}

// TODO: put more reusable goodies in here
