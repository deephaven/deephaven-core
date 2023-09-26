/**
 * Helper class to display a pair of totals tables for a given table-like object.
 * Requires a totals config and the totals-like object and a HTMLTableElement to
 * attach the totals to, as a `<tbody>` tag. Assumes that no other tbody elements
 * are present with the class "foot". Optional param of "offset" can be a number
 * indicating an offset to add to the columns, thus leaving one or more columns
 * blank.
 *
 * The "table-like object" must have the following:
 *   o  a `columns` property, returning Column[]
 *   o  a `getTotalsTable` method, taking a totals config and returning a Promise<Table>
 *   o  a `getGrandTotalsTable` method, taking a totals config and returning a Promise<Table>
 *
 */
class TotalsTableHelper {
  constructor(totalsConfig, tableInstance, tableElement, offset) {
    this.tableInstance = tableInstance;
    this.tableElement = tableElement;
    this.offset = offset || 0;

    this.updateConfig(totalsConfig).catch(e => console.error("failed to create totals! ", e));
  }

  /**
   * Replaces the config, reusing the existing table instance and element. Returns a promise
   * that indicates success in replacing the totals config, or failure if something went wrong.
   */
  updateConfig(totalsConfig) {
    this.close();

    this.totalsConfig = totalsConfig;

    return Promise.all([
      this.tableInstance.getTotalsTable(totalsConfig),
      this.tableInstance.getGrandTotalsTable(totalsConfig)
    ]).then(([totals, grandTotals]) => {
      this.totalsTable = totals;
      this.grandTotalsTable = grandTotals;

      var totalsRow = document.createElement('tbody');
      totalsRow.className = 'foot totals';
      var grandTotalsRow = document.createElement('tbody');
      grandTotalsRow.className = 'foot grandTotals';
      this.tableElement.appendChild(totalsRow);
      this.tableElement.appendChild(grandTotalsRow);

      //automatically subscribe to the first 25 rows, and all columns
      //TODO if the table's size reaches 25, add a "more" button or some other way to continue
      totals.setViewport(0, 24);
      grandTotals.setViewport(0, 25);

      // helper function to bind the totals table to the tbody it will fill in
      var columns = this.tableInstance.columns;
      var offset = this.offset;
      function bindTotalsToRow(totalsTable, tbody) {
        // It's possible that not all columns will be aggregated. One option is to render the totals table by
        // itself, another is to line up the columns, and skip the irrelevant ones.
        // This example does the latter, so skips over cells where no total is present
        return function(e) {
          var viewportData = e.detail;
          while (tbody.hasChildNodes()) {
            tbody.removeChild(tbody.lastChild);
          }
          for (var rowIndex = 0; rowIndex < viewportData.rows.length; rowIndex++) {
            var row = viewportData.rows[rowIndex];
            var tr = document.createElement('tr');
            //skip "offset" columns first
            for (var i = 0; i < offset; i++) {
              tr.appendChild(document.createElement('td'));
            }
            for (var mainTableColIndex = 0; mainTableColIndex < columns.length; mainTableColIndex++) {
              var name = columns[mainTableColIndex].name;
              var col = totalsTable.columns.find(c => c.name === name);
              if (col) {
                var cell = document.createElement('td');
                cell.textContent = row.get(col);
                tr.appendChild(cell);
              } else {
                // empty node so we skip it
                tr.appendChild(document.createElement('td'));
              }
            }
            tbody.appendChild(tr);
          }
        };
      }
      totals.addEventListener('updated', bindTotalsToRow(totals, totalsRow));
      grandTotals.addEventListener('updated', bindTotalsToRow(grandTotals, grandTotalsRow));

    });
  }
  /**
   * Closes the active totals tables
   */
  close() {
    this.totalsTable && this.totalsTable.close();
    this.grandTotalsTable && this.grandTotalsTable.close();

    this.tableElement.querySelectorAll('tbody.foot').forEach(foot => this.tableElement.removeChild(foot));
  }

}