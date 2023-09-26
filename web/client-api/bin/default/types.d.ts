// Minimum TypeScript Version: 4.3

/**
* This is part of EcmaScript 2015, documented here for completeness. It supports a single method, <b>next()</b>, which
* returns an object with a <b>boolean</b> named <b>done</b> (true if there are no more items to return; false
* otherwise), and optionally some <b>T</b> instance, <b>value</b>, if there was at least one remaining item.
*
* @param <T>
*/
export interface Iterator<T> {
	hasNext():boolean;
	next():IIterableResult<T>;
}
export interface IIterableResult<T> {
	value:T;
	done:boolean;
}
export namespace dh.storage {

	/**
	* Represents a file's contents loaded from the server. If an etag was specified when loading, client should first test
	* if the etag of this instance matches - if so, the contents will be empty, and the client's existing contents should
	* be used.
	*/
	export class FileContents {
		protected constructor();

		static blob(blob:Blob):FileContents;
		static text(...text:string[]):FileContents;
		static arrayBuffers(...buffers:ArrayBuffer[]):FileContents;
		text():Promise<string>;
		arrayBuffer():Promise<ArrayBuffer>;
		get etag():string;
	}

	/**
	* Remote service to read and write files on the server. Paths use "/" as a separator, and should not start with "/".
	*/
	export class StorageService {
		protected constructor();

		/**
		* Lists items in a given directory, with an optional filter glob to only list files that match. The empty or "root"
		* path should be specified as the empty string.
		*
		* @param path the path of the directory to list
		* @param glob optional glob to filter the contents of the directory
		* @return a promise containing the any items that are present in the given directory that match the glob, or an
		*         error.
		*/
		listItems(path:string, glob?:string):Promise<Array<ItemDetails>>;
		/**
		* Downloads a file at the given path, unless an etag is provided that matches the file's current contents.
		*
		* @param path the path of the file to fetch
		* @param etag an optional etag from the last time the client saw this file
		* @return a promise containing details about the file's contents, or an error.
		*/
		loadFile(path:string, etag?:string):Promise<FileContents>;
		/**
		* Deletes the item at the given path. Directories must be empty to be deleted.
		*
		* @param path the path of the item to delete
		* @return a promise with no value on success, or an error.
		*/
		deleteItem(path:string):Promise<void>;
		/**
		* Saves the provided contents to the given path, creating a file or replacing an existing one. The optional newFile
		* parameter can be passed to indicate that an existing file must not be overwritten, only a new file created.
		*
		* Note that directories must be empty to be overwritten.
		*
		* @param path the path of the file to write
		* @param contents the contents to write to that path
		* @param allowOverwrite true to allow an existing file to be overwritten, false or skip to require a new file
		* @return a promise with a FileContents, holding only the new etag (if the server emitted one), or an error
		*/
		saveFile(path:string, contents:FileContents, allowOverwrite?:boolean):Promise<FileContents>;
		/**
		* Moves (and/or renames) an item from its old path to its new path. The optional newFile parameter can be passed to
		* enforce that an existing item must not be overwritten.
		*
		* Note that directories must be empty to be overwritten.
		*
		* @param oldPath the path of the existing item
		* @param newPath the new path to move the item to
		* @param allowOverwrite true to allow an existing file to be overwritten, false or skip to require a new file
		* @return a promise with no value on success, or an error.
		*/
		moveItem(oldPath:string, newPath:string, allowOverwrite?:boolean):Promise<void>;
		/**
		* Creates a new directory at the specified path.
		*
		* @param path the path of the directory to create
		* @return a promise with no value on success, or an error.
		*/
		createDirectory(path:string):Promise<void>;
	}

	/**
	* Storage service metadata about files and folders.
	*/
	export class ItemDetails {
		protected constructor();

		get filename():string;
		get basename():string;
		get size():number;
		get etag():string;
		get type():ItemTypeType;
		get dirname():string;
	}


	type ItemTypeType = string;
	export class ItemType {
		static readonly DIRECTORY:ItemTypeType;
		static readonly FILE:ItemTypeType;
	}

}

export namespace dh {

	/**
	* Event data, describing the indexes that were added/removed/updated, and providing access to Rows (and thus data
	* in columns) either by index, or scanning the complete present index.
	*
	* This class supports two ways of reading the table - checking the changes made since the last update, and reading
	* all data currently in the table. While it is more expensive to always iterate over every single row in the table,
	* it may in some cases actually be cheaper than maintaining state separately and updating only the changes, though
	* both options should be considered.
	*
	* The RangeSet objects allow iterating over the LongWrapper indexes in the table. Note that these "indexes" are not
	* necessarily contiguous and may be negative, and represent some internal state on the server, allowing it to keep
	* track of data efficiently. Those LongWrapper objects can be passed to the various methods on this instance to
	* read specific rows or cells out of the table.
	*/
	export interface SubscriptionTableData extends TableData {
		get fullIndex():RangeSet;
		/**
		* The ordered set of row indexes removed since the last update
		* 
		* @return dh.RangeSet
		*/
		get removed():RangeSet;
		/**
		* The ordered set of row indexes added since the last update
		* 
		* @return dh.RangeSet
		*/
		get added():RangeSet;
		get columns():Array<Column>;
		/**
		* The ordered set of row indexes updated since the last update
		* 
		* @return dh.RangeSet
		*/
		get modified():RangeSet;
		get rows():Array<unknown>;
	}
	export interface HasEventHandling {
		/**
		* Listen for events on this object.
		*
		* @param name the name of the event to listen for
		* @param callback a function to call when the event occurs
		* @return Returns a cleanup function.
		* @param <T> the type of the data that the event will provide
		*/
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		/**
		* Removes an event listener added to this table.
		*
		* @param name
		* @param callback
		* @return
		* @param <T>
		*/
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
	}
	export interface Row {
		get(column:Column):any;
		getFormat(column:Column):Format;
		get index():LongWrapper;
	}
	/**
	* Behaves like a Table, but doesn't expose all of its API for changing the internal state. Instead, state is driven by
	* the upstream table - when it changes handle, this listens and updates its own handle accordingly.
	*
	* Additionally, this is automatically subscribed to its one and only row, across all columns.
	*
	* A new config is returned any time it is accessed, to prevent accidental mutation, and to allow it to be used as a
	* template when fetching a new totals table, or changing the totals table in use.
	*
	* A simplistic Table, providing access to aggregation of the table it is sourced from. This table is always
	* automatically subscribed to its parent, and adopts changes automatically from it. This class has limited methods
	* found on Table. Instances of this type always have a size of one when no groupBy is set on the config, but may
	* potentially contain as few as zero rows, or as many as the parent table if each row gets its own group.
	*
	* When using the `groupBy` feature, it may be desireable to also provide a row to the user with all values across all
	* rows. To achieve this, request the same Totals Table again, but remove the `groupBy` setting.
	*/
	export interface TotalsTable extends JoinableTable {
		/**
		* Specifies the range of items to pass to the client and update as they change. If the columns parameter is not
		* provided, all columns will be used. Until this is called, no data will be available. Invoking this will result in
		* events to be fired once data becomes available, starting with an <b>updated</b> event and one <b>rowadded</b>
		* event per row in that range.
		*
		* @param firstRow
		* @param lastRow
		* @param columns
		* @param updateIntervalMs
		*/
		setViewport(firstRow:number, lastRow:number, columns?:Array<Column>, updateIntervalMs?:number):void;
		/**
		* the currently visible viewport. If the current set of operations has not yet resulted in data, it will not
		* resolve until that data is ready.
		* 
		* @return Promise of {@link TableData}
		*/
		getViewportData():Promise<TableData>;
		/**
		* a column by the given name. You should prefer to always retrieve a new Column instance instead of caching a
		* returned value.
		* 
		* @param key
		* @return {@link Column}
		*/
		findColumn(key:string):Column;
		/**
		* multiple columns specified by the given names.
		* 
		* @param keys
		* @return {@link Column} array
		*/
		findColumns(keys:string[]):Column[];
		/**
		* Indicates that the table will no longer be used, and resources used to provide it can be freed up on the server.
		*/
		close():void;
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
		nextEvent<T>(eventName:string, timeoutInMillis:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		/**
		* Replace the currently set sort on this table. Returns the previously set value. Note that the sort property will
		* immediately return the new value, but you may receive update events using the old sort before the new sort is
		* applied, and the <b>sortchanged</b> event fires. Reusing existing, applied sorts may enable this to perform
		* better on the server. The <b>updated</b> event will also fire, but <b>rowadded</b> and <b>rowremoved</b> will
		* not.
		*
		* @param sort
		* @return {@link Sort} array
		*/
		applySort(sort:Sort[]):Array<Sort>;
		/**
		* Replace the current custom columns with a new set. These columns can be used when adding new filter and sort
		* operations to the table, as long as they are present.
		*
		* @param customColumns
		* @return
		*/
		applyCustomColumns(customColumns:Array<string|CustomColumn>):Array<CustomColumn>;
		/**
		* Replace the currently set filters on the table. Returns the previously set value. Note that the filter property
		* will immediately return the new value, but you may receive update events using the old filter before the new one
		* is applied, and the <b>filterchanged</b> event fires. Reusing existing, applied filters may enable this to
		* perform better on the server. The <b>updated</b> event will also fire, but <b>rowadded</b> and <b>rowremoved</b>
		* will not.
		*
		* @param filter
		* @return {@link FilterCondition} array
		*/
		applyFilter(filter:FilterCondition[]):Array<FilterCondition>;
		/**
		* An ordered list of Filters to apply to the table. To update, call applyFilter(). Note that this getter will
		* return the new value immediately, even though it may take a little time to update on the server. You may listen
		* for the <b>filterchanged</b> event to know when to update the UI.
		* 
		* @return {@link FilterCondition} array
		*/
		get filter():Array<FilterCondition>;
		/**
		* The total number of rows in this table. This may change as the base table's configuration, filter, or contents
		* change.
		* 
		* @return double
		*/
		get size():number;
		/**
		* The columns present on this table. Note that this may not include all columns in the parent table, and in cases
		* where a given column has more than one aggregation applied, the column name will have a suffix indicating the
		* aggregation used. This suffixed name will be of the form <b>columnName + '__' + aggregationName</b>.
		* 
		* @return {@link Column} array
		*/
		get columns():Array<Column>;
		get totalsTableConfig():TotalsTableConfig;
		/**
		* An ordered list of Sorts to apply to the table. To update, call applySort(). Note that this getter will return
		* the new value immediately, even though it may take a little time to update on the server. You may listen for the
		* <b>sortchanged</b> event to know when to update the UI.
		* 
		* @return {@link Sort} array
		*/
		get sort():Array<Sort>;
		/**
		* Read-only. An ordered list of custom column formulas to add to the table, either adding new columns or replacing
		* existing ones. To update, call <b>applyCustomColumns()</b>.
		* 
		* @return {@link CustomColumn} array
		*/
		get customColumns():Array<CustomColumn>;
	}
	/**
	* Contains data in the current viewport. Also contains the offset to this data, so that the actual row number may be
	* determined. Do not assume that the first row in `rows` is the first visible row, because extra rows may be provided
	* for easier scrolling without going to the server.
	*/
	export interface ViewportData extends TableData {
		/**
		* The index of the first returned row
		* 
		* @return double
		*/
		get offset():number;
		/**
		* A list of columns describing the data types in each row
		* 
		* @return {@link Column} array.
		*/
		get columns():Array<Column>;
		/**
		* An array of rows of data
		* 
		* @return {@link ViewportRow} array.
		*/
		get rows():Array<ViewportRow>;
	}
	/**
	* Wrap LocalTime values for use in JS. Provides text formatting for display and access to the underlying value.
	*/
	export interface LocalTimeWrapper {
		valueOf():string;
		getHour():number;
		getMinute():number;
		getSecond():number;
		getNano():number;
		toString():string;
	}
	/**
	* This object may be pooled internally or discarded and not updated. Do not retain references to it. Instead, request
	* the viewport again.
	*/
	export interface ViewportRow extends Row {
		get index():LongWrapper;
	}
	export interface RefreshToken {
		get bytes():string;
		get expiry():number;
	}
	/**
	* Row implementation that also provides additional read-only properties. represents visible rows in the table,
	* but with additional properties to reflect the tree structure.
	*/
	export interface TreeRow extends ViewportRow {
		/**
		* True if this node is currently expanded to show its children; false otherwise. Those children will be the
		* rows below this one with a greater depth than this one
		* 
		* @return boolean
		*/
		get isExpanded():boolean;
		/**
		* The number of levels above this node; zero for top level nodes. Generally used by the UI to indent the
		* row and its expand/collapse icon
		* 
		* @return int
		*/
		get depth():number;
		/**
		* True if this node has children and can be expanded; false otherwise. Note that this value may change when
		* the table updates, depending on the table's configuration
		* 
		* @return boolean
		*/
		get hasChildren():boolean;
		get index():LongWrapper;
	}
	/**
	* Wrap LocalDate values for use in JS. Provides text formatting for display and access to the underlying value.
	*/
	export interface LocalDateWrapper {
		valueOf():string;
		getYear():number;
		getMonthValue():number;
		getDayOfMonth():number;
		toString():string;
	}
	export interface WorkerHeapInfo {
		/**
		* Total heap size available for this worker.
		*/
		get totalHeapSize():number;
		get freeMemory():number;
		get maximumHeapSize():number;
	}
	/**
	* Represents a server-side object that may not yet have been fetched by the client. Does not memoize its result, so
	* fetch() should only be called once, and calling close() on this object will also close the result of the fetch.
	*/
	export interface WidgetExportedObject {
		fetch():Promise<any>;
		/**
		* Releases the server-side resources associated with this object, regardless of whether or not other client-side
		* objects exist that also use that object.
		*/
		close():void;
		get type():string;
	}
	/**
	* Javascript wrapper for {@link ColumnStatistics} This class holds the results of a call to generate statistics on a
	* table column.
	*/
	export interface ColumnStatistics {
		/**
		* Gets the type of formatting that should be used for given statistic.
		*
		* the format type for a statistic. A null return value means that the column formatting should be used.
		*
		* @param name the display name of the statistic
		* @return String
		*/
		getType(name:string):string;
		/**
		* Gets a map with the name of each unique value as key and the count a the value. A map of each unique value's name
		* to the count of how many times it occurred in the column. This map will be empty for tables containing more than
		* 19 unique values.
		*
		* @return Map of String double
		*/
		get uniqueValues():Map<string, number>;
		/**
		* Gets a map with the display name of statistics as keys and the numeric stat as a value.
		*
		* A map of each statistic's name to its value.
		*
		* @return Map of String and Object
		*/
		get statisticsMap():Map<string, object>;
	}
	export interface JoinableTable {
		freeze():Promise<Table>;
		snapshot(baseTable:Table, doInitialSnapshot?:boolean, stampColumns?:string[]):Promise<Table>;
		/**
		* @deprecated
		*/
		join(joinType:object, rightTable:JoinableTable, columnsToMatch:Array<string>, columnsToAdd?:Array<string>, asOfMatchRule?:object):Promise<Table>;
		asOfJoin(rightTable:JoinableTable, columnsToMatch:Array<string>, columnsToAdd?:Array<string>, asOfMatchRule?:string):Promise<Table>;
		crossJoin(rightTable:JoinableTable, columnsToMatch:Array<string>, columnsToAdd?:Array<string>, reserve_bits?:number):Promise<Table>;
		exactJoin(rightTable:JoinableTable, columnsToMatch:Array<string>, columnsToAdd?:Array<string>):Promise<Table>;
		naturalJoin(rightTable:JoinableTable, columnsToMatch:Array<string>, columnsToAdd?:Array<string>):Promise<Table>;
	}
	export interface LayoutHints {
		readonly searchDisplayMode?:SearchDisplayModeType|null;

		get hiddenColumns():string[]|null;
		get frozenColumns():string[]|null;
		get columnGroups():ColumnGroup[]|null;
		get areSavedLayoutsAllowed():boolean;
		get frontColumns():string[];
		get backColumns():string[]|null;
	}
	/**
	* Common interface for various ways of accessing table data and formatting.
	*
	* Java note: this interface contains some extra overloads that aren't available in JS. Implementations are expected to
	* implement only abstract methods, and default methods present in this interface will dispatch accordingly.
	*/
	export interface TableData {
		get(index:LongWrapper|number):Row;
		getData(index:LongWrapper|number, column:Column):any;
		getFormat(index:LongWrapper|number, column:Column):Format;
		get columns():Array<Column>;
		get rows():Array<unknown>;
	}
	/**
	* Represents the contents of a single widget data message from the server, with a binary data paylod and exported
	* objects. Implemented both by Widget itself and by the {@code event.details} when data is received by the client.
	*
	* Terminology note: the name of this type should probably use "Data" instead of "Message", and the methods should use
	* "payload" rather than "data" to match other platforms and the protobuf itself. These names are instead used for
	* backwards compatibility and to better follow JS expectations.
	*/
	export interface WidgetMessageDetails {
		/**
		* Returns the data from this message as a base64-encoded string.
		*/
		getDataAsBase64():string;
		/**
		* Returns the data from this message as a Uint8Array.
		*/
		getDataAsU8():Uint8Array;
		/**
		* Returns the data from this message as a utf-8 string.
		*/
		getDataAsString():string;
		/**
		* Returns an array of exported objects sent from the server. The plugin implementation is now responsible for these
		* objects, and should close them when no longer needed.
		*/
		get exportedObjects():WidgetExportedObject[];
	}
	export interface TreeViewportData extends TableData {
		get offset():number;
		get columns():Array<Column>;
		get rows():Array<TreeRow>;
	}
	/**
	* Encapsulates event handling around table subscriptions by "cheating" and wrapping up a JsTable instance to do the
	* real dirty work. This allows a viewport to stay open on the old table if desired, while this one remains open.
	* <p>
	* As this just wraps a JsTable (and thus a CTS), it holds its own flattened, pUT'd handle to get deltas from the
	* server. The setViewport method can be used to adjust this table instead of creating a new one.
	* <p>
	* Existing methods on JsTable like setViewport and getViewportData are intended to proxy to this, which then will talk
	* to the underlying handle and accumulated data.
	* <p>
	* As long as we keep the existing methods/events on JsTable, close() is not required if no other method is called, with
	* the idea then that the caller did not actually use this type. This means that for every exported method (which then
	* will mark the instance of "actually being used, please don't automatically close me"), there must be an internal
	* version called by those existing JsTable method, which will allow this instance to be cleaned up once the JsTable
	* deems it no longer in use.
	* <p>
	* Note that if the caller does close an instance, this shuts down the JsTable's use of this (while the converse is not
	* true), providing a way to stop the server from streaming updates to the client.
	*
	* This object serves as a "handle" to a subscription, allowing it to be acted on directly or canceled outright. If you
	* retain an instance of this, you have two choices - either only use it to call `close()` on it to stop the table's
	* viewport without creating a new one, or listen directly to this object instead of the table for data events, and
	* always call `close()` when finished. Calling any method on this object other than close() will result in it
	* continuing to live on after `setViewport` is called on the original table, or after the table is modified.
	*/
	export interface TableViewportSubscription extends HasEventHandling {
		/**
		* Changes the rows and columns set on this viewport. This cannot be used to change the update interval.
		* 
		* @param firstRow
		* @param lastRow
		* @param columns
		* @param updateIntervalMs
		*/
		setViewport(firstRow:number, lastRow:number, columns?:Column[]|undefined|null, updateIntervalMs?:number|undefined|null):void;
		/**
		* Stops this viewport from running, stopping all events on itself and on the table that created it.
		*/
		close():void;
		/**
		* Gets the data currently visible in this viewport
		* 
		* @return Promise of {@link TableData}.
		*/
		getViewportData():Promise<TableData>;
		snapshot(rows:RangeSet, columns:Column[]):Promise<TableData>;
	}
	export interface ColumnGroup {
		get name():string|null;
		get children():string[]|null;
		get color():string|null;
	}
	/**
	* This object may be pooled internally or discarded and not updated. Do not retain references to it.
	*/
	export interface Format {
		/**
		* The format string to apply to the value of this cell.
		* 
		* @return String
		*/
		readonly formatString?:string|null;
		/**
		* Color to apply to the cell's background, in <b>#rrggbb</b> format.
		* 
		* @return String
		*/
		readonly backgroundColor?:string|null;
		/**
		* Color to apply to the text, in <b>#rrggbb</b> format.
		* 
		* @return String
		*/
		readonly color?:string|null;
		/**
		* @deprecated Prefer formatString. Number format string to apply to the value in this cell.
		*/
		readonly numberFormat?:string|null;
	}

	/**
	* Exists to keep the dh.TableMap namespace so that the web UI can remain compatible with the DHE API, which still calls
	* this type TableMap.
	* @deprecated
	*/
	export class TableMap {
		static readonly EVENT_KEYADDED:string;
		static readonly EVENT_DISCONNECT:string;
		static readonly EVENT_RECONNECT:string;
		static readonly EVENT_RECONNECTFAILED:string;
	}

	/**
	* Describes a Sort present on the table. No visible constructor, created through the use of Column.sort(), will be tied
	* to that particular column data. Sort instances are immutable, and use a builder pattern to make modifications. All
	* methods return a new Sort instance.
	*/
	export class Sort {
		static readonly ASCENDING:string;
		static readonly DESCENDING:string;
		static readonly REVERSE:string;

		protected constructor();

		/**
		* Builds a Sort instance to sort values in ascending order.
		* 
		* @return {@link Sort}
		*/
		asc():Sort;
		/**
		* Builds a Sort instance to sort values in descending order.
		* 
		* @return {@link Sort}
		*/
		desc():Sort;
		/**
		* Builds a Sort instance which takes the absolute value before applying order.
		* 
		* @return {@link Sort}
		*/
		abs():Sort;
		toString():string;
		/**
		* True if the absolute value of the column should be used when sorting; defaults to false.
		* 
		* @return boolean
		*/
		get isAbs():boolean;
		/**
		* The column which is sorted.
		* 
		* @return {@link Column}
		*/
		get column():Column;
		/**
		* The direction of this sort, either <b>ASC</b>, <b>DESC</b>, or <b>REVERSE</b>.
		* 
		* @return String
		*/
		get direction():string;
	}

	/**
	* Describes how a Totals Table will be generated from its parent table. Each table has a default (which may be null)
	* indicating how that table was configured when it was declared, and each Totals Table has a similar property
	* describing how it was created. Both the <b>Table.getTotalsTable</b> and <b>Table.getGrandTotalsTable</b> methods take
	* this config as an optional parameter - without it, the table's default will be used, or if null, a default instance
	* of <b>TotalsTableConfig</b> will be supplied.
	*
	* This class has a no-arg constructor, allowing an instance to be made with the default values provided. However, any
	* JS object can be passed in to the methods which accept instances of this type, provided their values adhere to the
	* expected formats.
	*/
	export class TotalsTableConfig {
		/**
		* @deprecated
		*/
		static readonly COUNT:string;
		/**
		* @deprecated
		*/
		static readonly MIN:string;
		/**
		* @deprecated
		*/
		static readonly MAX:string;
		/**
		* @deprecated
		*/
		static readonly SUM:string;
		/**
		* @deprecated
		*/
		static readonly ABS_SUM:string;
		/**
		* @deprecated
		*/
		static readonly VAR:string;
		/**
		* @deprecated
		*/
		static readonly AVG:string;
		/**
		* @deprecated
		*/
		static readonly STD:string;
		/**
		* @deprecated
		*/
		static readonly FIRST:string;
		/**
		* @deprecated
		*/
		static readonly LAST:string;
		/**
		* @deprecated
		*/
		static readonly SKIP:string;
		/**
		* Specifies if a Totals Table should be expanded by default in the UI. Defaults to false.
		*/
		showTotalsByDefault:boolean;
		/**
		* Specifies if a Grand Totals Table should be expanded by default in the UI. Defaults to false.
		*/
		showGrandTotalsByDefault:boolean;
		/**
		* Specifies the default operation for columns that do not have a specific operation applied; defaults to "Sum".
		*/
		defaultOperation:AggregationOperationType;
		/**
		* Mapping from each column name to the aggregation(s) that should be applied to that column in the resulting Totals
		* Table. If a column is omitted, the defaultOperation is used.
		*/
		operationMap:{ [key: string]: Array<AggregationOperationType>; };
		/**
		* Groupings to use when generating the Totals Table. One row will exist for each unique set of values observed in
		* these columns. See also `Table.selectDistinct`.
		*/
		groupBy:Array<string>;

		constructor();

		toString():string;
	}

	/**
	* Describes a filter which can be applied to a table. Replacing these instances may be more expensive than reusing
	* them. These instances are immutable - all operations that compose them to build bigger expressions return a new
	* instance.
	*/
	export class FilterCondition {
		protected constructor();

		/**
		* the opposite of this condition
		* 
		* @return FilterCondition
		*/
		not():FilterCondition;
		/**
		* a condition representing the current condition logically ANDed with the other parameters
		* 
		* @param filters
		* @return FilterCondition
		*/
		and(...filters:FilterCondition[]):FilterCondition;
		/**
		* a condition representing the current condition logically ORed with the other parameters
		* 
		* @param filters
		* @return FilterCondition.
		*/
		or(...filters:FilterCondition[]):FilterCondition;
		/**
		* a string suitable for debugging showing the details of this condition.
		* 
		* @return String.
		*/
		toString():string;
		get columns():Array<Column>;
		/**
		* a filter condition invoking a static function with the given parameters. Currently supported Deephaven static
		* functions:
		* <ul>
		* <li><b>inRange</b>: Given three comparable values, returns true if the first is less than the second but greater
		* than the third</li>
		* <li><b>isInf</b>:Returns true if the given number is <i>infinity</i></li>
		* <li><b>isNaN</b>:Returns true if the given number is <i>not a number</i></li>
		* <li><b>isNormal</b>:Returns true if the given number <i>is not null</i>, <i>is not infinity</i>, and <i>is not
		* "not a number"</i></li>
		* <li><b>startsWith</b>:Returns true if the first string starts with the second string</li>
		* <li><b>endsWith</b>Returns true if the first string ends with the second string</li>
		* <li><b>matches</b>:Returns true if the first string argument matches the second string used as a Java regular
		* expression</li>
		* <li><b>contains</b>:Returns true if the first string argument contains the second string as a substring</li>
		* <li><b>in</b>:Returns true if the first string argument can be found in the second array argument.
		* <p>
		* Note that the array can only be specified as a column reference at this time - typically the `FilterValue.in`
		* method should be used in other cases
		* </p>
		* </li>
		* </ul>
		*
		* @param function
		* @param args
		* @return dh.FilterCondition
		*/
		static invoke(func:string, ...args:FilterValue[]):FilterCondition;
		/**
		* a filter condition which will check if the given value can be found in any supported column on whatever table
		* this FilterCondition is passed to. This FilterCondition is somewhat unique in that it need not be given a column
		* instance, but will adapt to any table. On numeric columns, with a value passed in which can be parsed as a
		* number, the column will be filtered to numbers which equal, or can be "rounded" effectively to this number. On
		* String columns, the given value will match any column which contains this string in a case-insensitive search. An
		* optional second argument can be passed, an array of `FilterValue` from the columns to limit this search to (see
		* {@link Column#filter}).
		* 
		* @param value
		* @param columns
		* @return dh.FilterCondition
		*/
		static search(value:FilterValue, columns?:FilterValue[]):FilterCondition;
	}

	export class Ide {
		constructor();

		/**
		* @deprecated
		*/
		getExistingSession(websocketUrl:string, authToken:string, serviceId:string, language:string):Promise<IdeSession>;
		/**
		* @deprecated
		*/
		static getExistingSession(websocketUrl:string, authToken:string, serviceId:string, language:string):Promise<IdeSession>;
	}

	export class CustomColumn {
		static readonly TYPE_FORMAT_COLOR:string;
		static readonly TYPE_FORMAT_NUMBER:string;
		static readonly TYPE_FORMAT_DATE:string;
		static readonly TYPE_NEW:string;

		protected constructor();

		valueOf():string;
		toString():string;
		/**
		* The expression to evaluate this custom column.
		* 
		* @return String
		*/
		get expression():string;
		/**
		* The name of the column to use.
		* 
		* @return String
		*/
		get name():string;
		/**
		* Type of custom column. One of
		*
		* <ul>
		* <li>FORMAT_COLOR</li>
		* <li>FORMAT_NUMBER</li>
		* <li>FORMAT_DATE</li>
		* <li>NEW</li>
		* </ul>
		*
		* @return String
		*/
		get type():string;
	}

	/**
	* Event fired when a command is issued from the client.
	*/
	export class CommandInfo {
		constructor(code:string, result:Promise<dh.ide.CommandResult>);

		get result():Promise<dh.ide.CommandResult>;
		get code():string;
	}

	export class DateWrapper extends LongWrapper {
		protected constructor();

		static ofJsDate(date:Date):DateWrapper;
		asDate():Date;
	}

	/**
	* Wrap BigDecimal values for use in JS. Provides text formatting for display and access to the underlying value.
	*/
	export class BigDecimalWrapper {
		protected constructor();

		static ofString(value:string):BigDecimalWrapper;
		asNumber():number;
		valueOf():string;
		toString():string;
	}

	/**
	* A Widget represents a server side object that sends one or more responses to the client. The client can then
	* interpret these responses to see what to render, or how to respond.
	*
	* Most custom object types result in a single response being sent to the client, often with other exported objects, but
	* some will have streamed responses, and allow the client to send follow-up requests of its own. This class's API is
	* backwards compatible, but as such does not offer a way to tell the difference between a streaming or non-streaming
	* object type, the client code that handles the payloads is expected to know what to expect. See
	* dh.WidgetMessageDetails for more information.
	*
	* When the promise that returns this object resolves, it will have the first response assigned to its fields. Later
	* responses from the server will be emitted as "message" events. When the connection with the server ends
	*/
	export class Widget implements WidgetMessageDetails, HasEventHandling {
		static readonly EVENT_MESSAGE:string;
		static readonly EVENT_CLOSE:string;

		protected constructor();

		/**
		* Ends the client connection to the server.
		*/
		close():void;
		getDataAsBase64():string;
		getDataAsU8():Uint8Array;
		getDataAsString():string;
		/**
		* Sends a string/bytes payload to the server, along with references to objects that exist on the server.
		*
		* @param msg string/buffer/view instance that represents data to send
		* @param references an array of objects that can be safely sent to the server
		*/
		sendMessage(msg:string|ArrayBuffer|ArrayBufferView, references?:Array<Table|Widget|WidgetExportedObject|PartitionedTable|TotalsTable|TreeTable>):void;
		/**
		* Listen for events on this object.
		*
		* @param name the name of the event to listen for
		* @param callback a function to call when the event occurs
		* @return Returns a cleanup function.
		* @param <T> the type of the data that the event will provide
		*/
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		/**
		* Removes an event listener added to this table.
		*
		* @param name
		* @param callback
		* @return
		* @param <T>
		*/
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
		get exportedObjects():WidgetExportedObject[];
		/**
		* @return the type of this widget
		*/
		get type():string;
	}


	/**
	* Presently optional and not used by the server, this allows the client to specify some authentication details. String
	* authToken <i>- base 64 encoded auth token. String serviceId -</i> The service ID to use for the connection.
	*/
	export class ConnectOptions {
		headers:{ [key: string]: string; };

		constructor();
	}

	/**
	* Describes the structure of the column, and if desired can be used to get access to the data to be rendered in this
	* column.
	*/
	export class Column {
		/**
		* If this column is part of a roll-up tree table, represents the type of the row data that can be found in this
		* column for leaf nodes if includeConstituents is enabled. Otherwise, it is <b>null</b>.
		*
		* @return String
		*/
		readonly constituentType?:string|null;
		readonly description?:string|null;

		protected constructor();

		/**
		* the value for this column in the given row. Type will be consistent with the type of the Column.
		*
		* @param row
		* @return Any
		*/
		get(row:Row):any;
		getFormat(row:Row):Format;
		/**
		* Creates a sort builder object, to be used when sorting by this column.
		*
		* @return {@link Sort}
		*/
		sort():Sort;
		/**
		* Creates a new value for use in filters based on this column. Used either as a parameter to another filter
		* operation, or as a builder to create a filter operation.
		*
		* @return {@link FilterValue}
		*/
		filter():FilterValue;
		/**
		* a <b>CustomColumn</b> object to apply using `applyCustomColumns` with the expression specified.
		*
		* @param expression
		* @return {@link CustomColumn}
		*/
		formatColor(expression:string):CustomColumn;
		/**
		* a <b>CustomColumn</b> object to apply using <b>applyCustomColumns</b> with the expression specified.
		*
		* @param expression
		* @return {@link CustomColumn}
		*/
		formatNumber(expression:string):CustomColumn;
		/**
		* a <b>CustomColumn</b> object to apply using <b>applyCustomColumns</b> with the expression specified.
		*
		* @param expression
		* @return {@link CustomColumn}
		*/
		formatDate(expression:string):CustomColumn;
		toString():string;
		/**
		* Label for this column.
		*
		* @return String
		*/
		get name():string;
		/**
		* True if this column is a partition column. Partition columns are used for filtering uncoalesced tables (see
		* <b>isUncoalesced</b> property on <b>Table</b>)
		*
		* @return boolean
		*/
		get isPartitionColumn():boolean;
		/**
		* @deprecated do not use. Internal index of the column in the table, to be used as a key on the Row.
		* @return int
		*/
		get index():number;
		get isSortable():boolean;
		/**
		* Type of the row data that can be found in this column.
		*
		* @return String
		*/
		get type():string;
		/**
		* Format entire rows colors using the expression specified. Returns a <b>CustomColumn</b> object to apply to a
		* table using <b>applyCustomColumns</b> with the parameters specified.
		*
		* @param expression
		* @return {@link CustomColumn}
		*/
		static formatRowColor(expression:string):CustomColumn;
		/**
		* a <b>CustomColumn</b> object to apply using <b>applyCustomColumns</b> with the expression specified.
		*
		* @param name
		* @param expression
		* @return {@link CustomColumn}
		*/
		static createCustomColumn(name:string, expression:string):CustomColumn;
	}

	export class QueryInfo {
		static readonly EVENT_TABLE_OPENED:string;
		static readonly EVENT_DISCONNECT:string;
		static readonly EVENT_RECONNECT:string;
		static readonly EVENT_CONNECT:string;
	}

	/**
	* Behaves like a JsTable externally, but data, state, and viewports are managed by an entirely different mechanism, and
	* so reimplemented here.
	*
	* Any time a change is made, we build a new request and send it to the server, and wait for the updated state.
	*
	* Semantics around getting updates from the server are slightly different - we don't "unset" the viewport here after
	* operations are performed, but encourage the client code to re-set them to the desired position.
	*
	* The table size will be -1 until a viewport has been fetched.
	*
	* Similar to a table, a Tree Table provides access to subscribed viewport data on the current hierarchy. A different
	* Row type is used within that viewport, showing the depth of that node within the tree and indicating details about
	* whether or not it has children or is expanded. The Tree Table itself then provides the ability to change if a row is
	* expanded or not. Methods used to control or check if a row should be expanded or not can be invoked on a TreeRow
	* instance, or on the number of the row (thus allowing for expanding/collapsing rows which are not currently visible in
	* the viewport).
	*
	* Events and viewports are somewhat different than tables, due to the expense of computing the expanded/collapsed rows
	* and count of children at each level of the hierarchy, and differences in the data that is available.
	*
	* - There is no <b>totalSize</b> property. - The viewport is not un-set when changes are made to filter or sort, but
	* changes will continue to be streamed in. It is suggested that the viewport be changed to the desired position
	* (usually the first N rows) after any filter/sort change is made. Likewise, <b>getViewportData()</b> will always
	* return the most recent data, and will not wait if a new operation is pending. - Custom columns are not directly
	* supported. If the <b>TreeTable</b> was created client-side, the original Table can have custom columns applied, and
	* the <b>TreeTable</b> can be recreated. - The <b>totalsTableConfig</b> property is instead a method, and returns a
	* promise so the config can be fetched asynchronously. - Totals Tables for trees vary in behavior between hierarchical
	* tables and roll-up tables. This behavior is based on the original flat table used to produce the Tree Table - for a
	* hierarchical table (i.e. Table.treeTable in the query config), the totals will include non-leaf nodes (since they are
	* themselves actual rows in the table), but in a roll-up table, the totals only include leaf nodes (as non-leaf nodes
	* are generated through grouping the contents of the original table). Roll-ups also have the
	* <b>isIncludeConstituents</b> property, indicating that a <b>Column</b> in the tree may have a <b>constituentType</b>
	* property reflecting that the type of cells where <b>hasChildren</b> is false will be different from usual.
	*/
	export class TreeTable implements HasEventHandling {
		/**
		* event.detail is the currently visible viewport data based on the active viewport configuration.
		*/
		static readonly EVENT_UPDATED:string;
		static readonly EVENT_DISCONNECT:string;
		static readonly EVENT_RECONNECT:string;
		static readonly EVENT_RECONNECTFAILED:string;
		static readonly EVENT_REQUEST_FAILED:string;
		readonly description?:string|null;

		protected constructor();

		/**
		* Expands the given node, so that its children are visible when they are in the viewport. The parameter can be the
		* row index, or the row object itself. The second parameter is a boolean value, false by default, specifying if the
		* row and all descendants should be fully expanded. Equivalent to `setExpanded(row, true)` with an optional third
		* boolean parameter.
		*
		* @param row
		* @param expandDescendants
		*/
		expand(row:TreeRow|number, expandDescendants?:boolean):void;
		/**
		* Collapses the given node, so that its children and descendants are not visible in the size or the viewport. The
		* parameter can be the row index, or the row object itself. Equivalent to <b>setExpanded(row, false, false)</b>.
		*
		* @param row
		*/
		collapse(row:TreeRow|number):void;
		/**
		* Specifies if the given node should be expanded or collapsed. If this node has children, and the value is changed,
		* the size of the table will change. If node is to be expanded and the third parameter, <b>expandDescendants</b>,
		* is true, then its children will also be expanded.
		*
		* @param row
		* @param isExpanded
		* @param expandDescendants
		*/
		setExpanded(row:TreeRow|number, isExpanded:boolean, expandDescendants?:boolean):void;
		expandAll():void;
		collapseAll():void;
		/**
		* true if the given row is expanded, false otherwise. Equivalent to `TreeRow.isExpanded`, if an instance of the row
		* is available
		* 
		* @param row
		* @return boolean
		*/
		isExpanded(row:TreeRow|number):boolean;
		setViewport(firstRow:number, lastRow:number, columns?:Array<Column>|undefined|null, updateInterval?:number|undefined|null):void;
		getViewportData():Promise<TreeViewportData>;
		/**
		* Indicates that the table will no longer be used, and server resources can be freed.
		*/
		close():void;
		typedTicket():dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
		/**
		* Applies the given sort to all levels of the tree. Returns the previous sort in use.
		*
		* @param sort
		* @return {@link Sort} array
		*/
		applySort(sort:Sort[]):Array<Sort>;
		/**
		* Applies the given filter to the contents of the tree in such a way that if any node is visible, then any parent
		* node will be visible as well even if that parent node would not normally be visible due to the filter's
		* condition. Returns the previous sort in use.
		*
		* @param filter
		* @return {@link FilterCondition} array
		*/
		applyFilter(filter:FilterCondition[]):Array<FilterCondition>;
		/**
		* a column with the given name, or throws an exception if it cannot be found
		* 
		* @param key
		* @return {@link Column}
		*/
		findColumn(key:string):Column;
		/**
		* an array with all of the named columns in order, or throws an exception if one cannot be found.
		* 
		* @param keys
		* @return {@link Column} array
		*/
		findColumns(keys:string[]):Column[];
		/**
		* Provides Table-like selectDistinct functionality, but with a few quirks, since it is only fetching the distinct
		* values for the given columns in the source table:
		* <ul>
		* <li>Rollups may make no sense, since values are aggregated.</li>
		* <li>Values found on orphaned (and removed) nodes will show up in the resulting table, even though they are not in
		* the tree.</li>
		* <li>Values found on parent nodes which are only present in the tree since a child is visible will not be present
		* in the resulting table.</li>
		* </ul>
		*/
		selectDistinct(columns:Column[]):Promise<Table>;
		getTotalsTableConfig():Promise<TotalsTableConfig>;
		getTotalsTable(config?:object):Promise<TotalsTable>;
		getGrandTotalsTable(config?:object):Promise<TotalsTable>;
		/**
		* a new copy of this treetable, so it can be sorted and filtered separately, and maintain a different viewport.
		* Unlike Table, this will _not_ copy the filter or sort, since tree table viewport semantics differ, and without a
		* viewport set, the treetable doesn't evaluate these settings, and they aren't readable on the properties. Expanded
		* state is also not copied.
		*
		* @return Promise of dh.TreeTable
		*/
		copy():Promise<TreeTable>;
		/**
		* The current filter configuration of this Tree Table.
		* 
		* @return {@link FilterCondition} array
		*/
		get filter():Array<FilterCondition>;
		/**
		* True if this is a roll-up and will provide the original rows that make up each grouping.
		* 
		* @return boolean
		*/
		get includeConstituents():boolean;
		get groupedColumns():Array<Column>;
		get isClosed():boolean;
		/**
		* The current number of rows given the table's contents and the various expand/collapse states of each node. (No
		* totalSize is provided at this time; its definition becomes unclear between roll-up and tree tables, especially
		* when considering collapse/expand states).
		*
		* @return double
		*/
		get size():number;
		/**
		* The columns that can be shown in this Tree Table.
		* 
		* @return {@link Column} array
		*/
		get columns():Array<Column>;
		/**
		* The current sort configuration of this Tree Table
		* 
		* @return {@link Sort} array.
		*/
		get sort():Array<Sort>;
		/**
		* Listen for events on this object.
		*
		* @param name the name of the event to listen for
		* @param callback a function to call when the event occurs
		* @return Returns a cleanup function.
		* @param <T> the type of the data that the event will provide
		*/
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		/**
		* Removes an event listener added to this table.
		*
		* @param name
		* @param callback
		* @return
		* @param <T>
		*/
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
	}

	/**
	* Wrap BigInteger values for use in JS. Provides text formatting for display and access to the underlying value.
	*/
	export class BigIntegerWrapper {
		protected constructor();

		static ofString(str:string):BigIntegerWrapper;
		asNumber():number;
		valueOf():string;
		toString():string;
	}

	export class CoreClient implements HasEventHandling {
		static readonly EVENT_CONNECT:string;
		static readonly EVENT_DISCONNECT:string;
		static readonly EVENT_RECONNECT:string;
		static readonly EVENT_RECONNECT_AUTH_FAILED:string;
		static readonly EVENT_REFRESH_TOKEN_UPDATED:string;
		static readonly EVENT_REQUEST_FAILED:string;
		static readonly EVENT_REQUEST_STARTED:string;
		static readonly EVENT_REQUEST_SUCCEEDED:string;
		static readonly LOGIN_TYPE_PASSWORD:string;
		static readonly LOGIN_TYPE_ANONYMOUS:string;

		constructor(serverUrl:string, connectOptions?:ConnectOptions);

		running():Promise<CoreClient>;
		getServerUrl():string;
		getAuthConfigValues():Promise<string[][]>;
		login(credentials:LoginCredentials):Promise<void>;
		relogin(token:RefreshToken):Promise<void>;
		onConnected(timeoutInMillis?:number):Promise<void>;
		getServerConfigValues():Promise<string[][]>;
		getStorageService():dh.storage.StorageService;
		getAsIdeConnection():Promise<IdeConnection>;
		disconnect():void;
		/**
		* Listen for events on this object.
		*
		* @param name the name of the event to listen for
		* @param callback a function to call when the event occurs
		* @return Returns a cleanup function.
		* @param <T> the type of the data that the event will provide
		*/
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		/**
		* Removes an event listener added to this table.
		*
		* @param name
		* @param callback
		* @return
		* @param <T>
		*/
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
	}

	export class LongWrapper {
		protected constructor();

		static ofString(str:string):LongWrapper;
		asNumber():number;
		valueOf():string;
		toString():string;
	}

	/**
	* A js type for operating on input tables.
	*
	* Represents a User Input Table, which can have data added to it from other sources.
	*
	* You may add rows using dictionaries of key-value tuples (representing columns by name), add tables containing all the
	* key/value columns to add, or delete tables containing the keys to delete. Each operation is atomic, and will either
	* succeed completely or fail completely. To guarantee order of operations, apply an operation and wait for the response
	* before sending the next operation.
	*
	* Each table has one or more key columns, where each unique combination of keys will appear at most once in the table.
	*
	* To view the results of the Input Table, you should use standard table operations on the InputTable's source Table
	* object.
	*/
	export class InputTable {
		protected constructor();

		/**
		* Adds a single row to the table. For each key or value column name in the Input Table, we retrieve that javascript
		* property at that name and validate it can be put into the given column type.
		* 
		* @param row
		* @param userTimeZone
		* @return Promise of dh.InputTable
		*/
		addRow(row:{ [key: string]: any; }, userTimeZone?:string):Promise<InputTable>;
		/**
		* Add multiple rows to a table.
		* 
		* @param rows
		* @param userTimeZone
		* @return Promise of dh.InputTable
		*/
		addRows(rows:{ [key: string]: any; }[], userTimeZone?:string):Promise<InputTable>;
		/**
		* Add an entire table to this Input Table. Only column names that match the definition of the input table will be
		* copied, and all key columns must have values filled in. This only copies the current state of the source table;
		* future updates to the source table will not be reflected in the Input Table. The returned promise will be
		* resolved to the same InputTable instance this method was called upon once the server returns.
		*
		* @param tableToAdd
		* @return Promise of dh.InputTable
		*/
		addTable(tableToAdd:Table):Promise<InputTable>;
		/**
		* Add multiple tables to this Input Table.
		* 
		* @param tablesToAdd
		* @return Promise of dh.InputTable
		*/
		addTables(tablesToAdd:Table[]):Promise<InputTable>;
		/**
		* Deletes an entire table from this Input Table. Key columns must match the Input Table.
		* 
		* @param tableToDelete
		* @return Promise of dh.InputTable
		*/
		deleteTable(tableToDelete:Table):Promise<InputTable>;
		/**
		* Delete multiple tables from this Input Table.
		* 
		* @param tablesToDelete
		* @return
		*/
		deleteTables(tablesToDelete:Table[]):Promise<InputTable>;
		/**
		* A list of the key columns, by name
		* 
		* @return String array.
		*/
		get keys():string[];
		/**
		* A list of the value columns, by name
		* 
		* @return String array.
		*/
		get values():string[];
		/**
		* A list of the key Column objects
		* 
		* @return {@link Column} array.
		*/
		get keyColumns():Column[];
		/**
		* A list of the value Column objects
		* 
		* @return {@link Column} array.
		*/
		get valueColumns():Column[];
		/**
		* The source table for this Input Table
		* 
		* @return dh.table
		*/
		get table():Table;
	}

	/**
	* Describes a grouping and aggregations for a roll-up table. Pass to the <b>Table.rollup</b> function to create a
	* roll-up table.
	*/
	export class RollupConfig {
		/**
		* Ordered list of columns to group by to form the hierarchy of the resulting roll-up table.
		*/
		groupingColumns:Array<String>;
		/**
		* Mapping from each aggregation name to the ordered list of columns it should be applied to in the resulting
		* roll-up table.
		*/
		aggregations:{ [key: string]: Array<AggregationOperationType>; };
		/**
		* Optional parameter indicating if an extra leaf node should be added at the bottom of the hierarchy, showing the
		* rows in the underlying table which make up that grouping. Since these values might be a different type from the
		* rest of the column, any client code must check if TreeRow.hasChildren = false, and if so, interpret those values
		* as if they were Column.constituentType instead of Column.type. Defaults to false.
		*/
		includeConstituents:boolean;
		includeOriginalColumns?:boolean|null;
		/**
		* Optional parameter indicating if original column descriptions should be included. Defaults to true.
		*/
		includeDescriptions:boolean;

		constructor();
	}

	/**
	* Deprecated for use in Deephaven Core.
	* @deprecated
	*/
	export class Client {
		static readonly EVENT_REQUEST_FAILED:string;
		static readonly EVENT_REQUEST_STARTED:string;
		static readonly EVENT_REQUEST_SUCCEEDED:string;

		constructor();
	}

	/**
	* Describes data that can be filtered, either a column reference or a literal value. Used this way, the type of a value
	* can be specified so that values which are ambiguous or not well supported in JS will not be confused with Strings or
	* imprecise numbers (e.g., nanosecond-precision date values). Additionally, once wrapped in this way, methods can be
	* called on these value literal instances. These instances are immutable - any method called on them returns a new
	* instance.
	*/
	export class FilterValue {
		/**
		* Constructs a number for the filter API from the given parameter. Can also be used on the values returned from
		* <b><Row.get/b> for DateTime values. To create a filter with a date, use <b>dh.DateWrapper.ofJsDate</b> or
		* <b>dh.i18n.DateTimeFormat.parse</b>. To create a filter with a 64-bit long integer, use
		* <b>dh.LongWrapper.ofString</b>.
		*
		* @param input
		* @return
		*/
		static ofNumber(input:LongWrapper|number):FilterValue;
		/**
		* a filter condition checking if the current value is equal to the given parameter
		* 
		* @param term
		* @return {@link FilterCondition}
		*/
		eq(term:FilterValue):FilterCondition;
		/**
		* a filter condition checking if the current value is equal to the given parameter, ignoring differences of upper
		* vs lower case
		* 
		* @param term
		* @return {@link FilterCondition}
		*/
		eqIgnoreCase(term:FilterValue):FilterCondition;
		/**
		* a filter condition checking if the current value is not equal to the given parameter
		* 
		* @param term
		* @return {@link FilterCondition}
		*/
		notEq(term:FilterValue):FilterCondition;
		/**
		* a filter condition checking if the current value is not equal to the given parameter, ignoring differences of
		* upper vs lower case
		* 
		* @param term
		* @return {@link FilterCondition}
		*/
		notEqIgnoreCase(term:FilterValue):FilterCondition;
		/**
		* a filter condition checking if the current value is greater than the given parameter
		* 
		* @param term
		* @return {@link FilterCondition}
		*/
		greaterThan(term:FilterValue):FilterCondition;
		/**
		* a filter condition checking if the current value is less than the given parameter
		* 
		* @param term
		* @return {@link FilterCondition}
		*/
		lessThan(term:FilterValue):FilterCondition;
		/**
		* a filter condition checking if the current value is greater than or equal to the given parameter
		* 
		* @param term
		* @return {@link FilterCondition}
		*/
		greaterThanOrEqualTo(term:FilterValue):FilterCondition;
		/**
		* a filter condition checking if the current value is less than or equal to the given parameter
		* 
		* @param term
		* @return {@link FilterCondition}
		*/
		lessThanOrEqualTo(term:FilterValue):FilterCondition;
		/**
		* a filter condition checking if the current value is in the given set of values
		* 
		* @param terms
		* @return {@link FilterCondition}
		*/
		in(terms:FilterValue[]):FilterCondition;
		/**
		* a filter condition checking if the current value is in the given set of values, ignoring differences of upper vs
		* lower case
		* 
		* @param terms
		* @return {@link FilterCondition}
		*/
		inIgnoreCase(terms:FilterValue[]):FilterCondition;
		/**
		* a filter condition checking that the current value is not in the given set of values
		* 
		* @param terms
		* @return {@link FilterCondition}
		*/
		notIn(terms:FilterValue[]):FilterCondition;
		/**
		* a filter condition checking that the current value is not in the given set of values, ignoring differences of
		* upper vs lower case
		* 
		* @param terms
		* @return {@link FilterCondition}
		*/
		notInIgnoreCase(terms:FilterValue[]):FilterCondition;
		/**
		* a filter condition checking if the given value contains the given string value
		* 
		* @param term
		* @return {@link FilterCondition}
		*/
		contains(term:FilterValue):FilterCondition;
		/**
		* a filter condition checking if the given value contains the given string value, ignoring differences of upper vs
		* lower case
		* 
		* @param term
		* @return {@link FilterCondition}
		*/
		containsIgnoreCase(term:FilterValue):FilterCondition;
		/**
		* a filter condition checking if the given value matches the provided regular expressions string. Regex patterns
		* use Java regex syntax
		* 
		* @param pattern
		* @return {@link FilterCondition}
		*/
		matches(pattern:FilterValue):FilterCondition;
		/**
		* a filter condition checking if the given value matches the provided regular expressions string, ignoring
		* differences of upper vs lower case. Regex patterns use Java regex syntax
		* 
		* @param pattern
		* @return {@link FilterCondition}
		*/
		matchesIgnoreCase(pattern:FilterValue):FilterCondition;
		/**
		* a filter condition checking if the current value is a true boolean
		* 
		* @return {@link FilterCondition}
		*/
		isTrue():FilterCondition;
		/**
		* a filter condition checking if the current value is a false boolean
		* 
		* @return {@link FilterCondition}
		*/
		isFalse():FilterCondition;
		/**
		* a filter condition checking if the current value is a null value
		* 
		* @return {@link FilterCondition}
		*/
		isNull():FilterCondition;
		/**
		* a filter condition invoking the given method on the current value, with the given parameters. Currently supported
		* functions that can be invoked on a String:
		* <ul>
		* <li><b>startsWith</b>: Returns true if the current string value starts with the supplied string argument</li>
		* <li><b>endsWith</b>: Returns true if the current string value ends with the supplied string argument</li>
		* <li><b>matches</b>: Returns true if the current string value matches the supplied string argument used as a Java
		* regular expression</li>
		* <li><b>contains</b>: Returns true if the current string value contains the supplied string argument
		* <p>
		* When invoking against a constant, this should be avoided in favor of FilterValue.contains
		* </p>
		* </li>
		* </ul>
		*
		* @param method
		* @param args
		* @return
		*/
		invoke(method:string, ...args:FilterValue[]):FilterCondition;
		toString():string;
		/**
		* Constructs a string for the filter API from the given parameter.
		*
		* @param input
		* @return
		*/
		static ofString(input:any):FilterValue;
		/**
		* Constructs a boolean for the filter API from the given parameter.
		*
		* @param b
		* @return
		*/
		static ofBoolean(b:boolean):FilterValue;
	}

	export class IdeSession implements HasEventHandling {
		static readonly EVENT_COMMANDSTARTED:string;
		static readonly EVENT_REQUEST_FAILED:string;

		protected constructor();

		/**
		* Load the named table, with columns and size information already fully populated.
		* 
		* @param name
		* @param applyPreviewColumns optional boolean
		* @return {@link Promise} of {@link JsTable}
		*/
		getTable(name:string, applyPreviewColumns?:boolean):Promise<Table>;
		/**
		* Load the named Figure, including its tables and tablemaps as needed.
		* 
		* @param name
		* @return promise of dh.plot.Figure
		*/
		getFigure(name:string):Promise<dh.plot.Figure>;
		/**
		* Loads the named tree table or roll-up table, with column data populated. All nodes are collapsed by default, and
		* size is presently not available until the viewport is first set.
		* 
		* @param name
		* @return {@link Promise} of {@link JsTreeTable}
		*/
		getTreeTable(name:string):Promise<TreeTable>;
		getHierarchicalTable(name:string):Promise<TreeTable>;
		getObject(definitionObject:dh.ide.VariableDescriptor):Promise<any>;
		newTable(columnNames:string[], types:string[], data:string[][], userTimeZone:string):Promise<Table>;
		/**
		* Merges the given tables into a single table. Assumes all tables have the same structure.
		* 
		* @param tables
		* @return {@link Promise} of {@link JsTable}
		*/
		mergeTables(tables:Table[]):Promise<Table>;
		bindTableToVariable(table:Table, name:string):Promise<void>;
		subscribeToFieldUpdates(callback:(arg0:dh.ide.VariableChanges)=>void):()=>void;
		close():void;
		runCode(code:string):Promise<dh.ide.CommandResult>;
		onLogMessage(callback:(arg0:dh.ide.LogItem)=>void):()=>void;
		openDocument(params:object):void;
		changeDocument(params:object):void;
		getCompletionItems(params:object):Promise<Array<dh.lsp.CompletionItem>>;
		getSignatureHelp(params:object):Promise<Array<dh.lsp.SignatureInformation>>;
		getHover(params:object):Promise<dh.lsp.Hover>;
		closeDocument(params:object):void;
		/**
		* Creates an empty table with the specified number of rows. Optionally columns and types may be specified, but all
		* values will be null.
		* 
		* @param size
		* @return {@link Promise} of {@link JsTable}
		*/
		emptyTable(size:number):Promise<Table>;
		/**
		* Creates a new table that ticks automatically every "periodNanos" nanoseconds. A start time may be provided; if so
		* the table will be populated with the interval from the specified date until now.
		* 
		* @param periodNanos
		* @param startTime
		* @return {@link Promise} of {@link JsTable}
		*/
		timeTable(periodNanos:number, startTime?:DateWrapper):Promise<Table>;
		/**
		* Listen for events on this object.
		*
		* @param name the name of the event to listen for
		* @param callback a function to call when the event occurs
		* @return Returns a cleanup function.
		* @param <T> the type of the data that the event will provide
		*/
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		/**
		* Removes an event listener added to this table.
		*
		* @param name
		* @param callback
		* @return
		* @param <T>
		*/
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
	}

	/**
	* Provides access to data in a table. Note that several methods present their response through Promises. This allows
	* the client to both avoid actually connecting to the server until necessary, and also will permit some changes not to
	* inform the UI right away that they have taken place.
	*/
	export class Table implements JoinableTable, HasEventHandling {
		readonly description?:string|null;
		readonly pluginName?:string|null;
		readonly layoutHints?:null|LayoutHints;
		/**
		* The table size has updated, so live scrollbars and the like can be updated accordingly.
		*/
		static readonly EVENT_SIZECHANGED:string;
		static readonly EVENT_UPDATED:string;
		static readonly EVENT_ROWADDED:string;
		static readonly EVENT_ROWREMOVED:string;
		static readonly EVENT_ROWUPDATED:string;
		static readonly EVENT_SORTCHANGED:string;
		static readonly EVENT_FILTERCHANGED:string;
		static readonly EVENT_CUSTOMCOLUMNSCHANGED:string;
		static readonly EVENT_DISCONNECT:string;
		static readonly EVENT_RECONNECT:string;
		static readonly EVENT_RECONNECTFAILED:string;
		static readonly EVENT_REQUEST_FAILED:string;
		static readonly EVENT_REQUEST_SUCCEEDED:string;
		static readonly SIZE_UNCOALESCED:number;

		protected constructor();

		batch(userCode:(arg0:unknown)=>void):Promise<Table>;
		/**
		* Retrieve a column by the given name. You should prefer to always retrieve a new Column instance instead of
		* caching a returned value.
		*
		* @param key
		* @return {@link Column}
		*/
		findColumn(key:string):Column;
		/**
		* Retrieve multiple columns specified by the given names.
		*
		* @param keys
		* @return {@link Column} array
		*/
		findColumns(keys:string[]):Column[];
		isBlinkTable():boolean;
		/**
		* If .hasInputTable is true, you may call this method to gain access to an InputTable object which can be used to
		* mutate the data within the table. If the table is not an Input Table, the promise will be immediately rejected.
		*
		* @return Promise of dh.InputTable
		*/
		inputTable():Promise<InputTable>;
		/**
		* Indicates that this Table instance will no longer be used, and its connection to the server can be cleaned up.
		*/
		close():void;
		getAttributes():string[];
		/**
		* null if no property exists, a string if it is an easily serializable property, or a <b>Promise
		* <Table>
		* </b> that will either resolve with a table or error out if the object can't be passed to JS.
		* 
		* @param attributeName
		* @return Object
		*/
		getAttribute(attributeName:string):object|undefined|null;
		/**
		* Replace the currently set sort on this table. Returns the previously set value. Note that the sort property will
		* immediately return the new value, but you may receive update events using the old sort before the new sort is
		* applied, and the <b>sortchanged</b> event fires. Reusing existing, applied sorts may enable this to perform
		* better on the server. The <b>updated</b> event will also fire, but <b>rowadded</b> and <b>rowremoved</b> will
		* not.
		*
		* @param sort
		* @return {@link Sort} array
		*/
		applySort(sort:Sort[]):Array<Sort>;
		/**
		* Replace the currently set filters on the table. Returns the previously set value. Note that the filter property
		* will immediately return the new value, but you may receive update events using the old filter before the new one
		* is applied, and the <b>filterchanged</b> event fires. Reusing existing, applied filters may enable this to
		* perform better on the server. The <b>updated</b> event will also fire, but <b>rowadded</b> and <b>rowremoved</b>
		* will not.
		*
		* @param filter
		* @return {@link FilterCondition} array
		*/
		applyFilter(filter:FilterCondition[]):Array<FilterCondition>;
		/**
		* used when adding new filter and sort operations to the table, as long as they are present.
		*
		* @param customColumns
		* @return {@link CustomColumn} array
		*/
		applyCustomColumns(customColumns:Array<string|CustomColumn>):Array<CustomColumn>;
		/**
		* If the columns parameter is not provided, all columns will be used. If the updateIntervalMs parameter is not
		* provided, a default of one second will be used. Until this is called, no data will be available. Invoking this
		* will result in events to be fired once data becomes available, starting with an `updated` event and a
		* <b>rowadded</b> event per row in that range. The returned object allows the viewport to be closed when no longer
		* needed.
		*
		* @param firstRow
		* @param lastRow
		* @param columns
		* @param updateIntervalMs
		* @return {@link TableViewportSubscription}
		*/
		setViewport(firstRow:number, lastRow:number, columns?:Array<Column>|undefined|null, updateIntervalMs?:number|undefined|null):TableViewportSubscription;
		/**
		* Gets the currently visible viewport. If the current set of operations has not yet resulted in data, it will not
		* resolve until that data is ready.
		* 
		* @return Promise of {@link TableData}
		*/
		getViewportData():Promise<TableData>;
		/**
		* Creates a subscription to the specified columns, across all rows in the table. The optional parameter
		* updateIntervalMs may be specified to indicate how often the server should send updates, defaulting to one second
		* if omitted. Useful for charts or taking a snapshot of the table atomically. The initial snapshot will arrive in a
		* single event, but later changes will be sent as updates. However, this may still be very expensive to run from a
		* browser for very large tables. Each call to subscribe creates a new subscription, which must have <b>close()</b>
		* called on it to stop it, and all events are fired from the TableSubscription instance.
		*
		* @param columns
		* @param updateIntervalMs
		* @return {@link TableSubscription}
		*/
		subscribe(columns:Array<Column>, updateIntervalMs?:number):TableSubscription;
		/**
		* a new table containing the distinct tuples of values from the given columns that are present in the original
		* table. This table can be manipulated as any other table. Sorting is often desired as the default sort is the
		* order of appearance of values from the original table.
		* 
		* @param columns
		* @return Promise of dh.Table
		*/
		selectDistinct(columns:Column[]):Promise<Table>;
		/**
		* Creates a new copy of this table, so it can be sorted and filtered separately, and maintain a different viewport.
		* 
		* @return Promise of dh.Table
		*/
		copy():Promise<Table>;
		/**
		* a promise that will resolve to a Totals Table of this table. This table will obey the configurations provided as
		* a parameter, or will use the table's default if no parameter is provided, and be updated once per second as
		* necessary. Note that multiple calls to this method will each produce a new TotalsTable which must have close()
		* called on it when not in use.
		* 
		* @param config
		* @return Promise of dh.TotalsTable
		*/
		getTotalsTable(config?:TotalsTableConfig|undefined|null):Promise<TotalsTable>;
		/**
		* The default configuration to be used when building a <b>TotalsTable</b> for this table.
		* 
		* @return dh.TotalsTableConfig
		*/
		getTotalsTableConfig():TotalsTableConfig;
		/**
		* a promise that will resolve to a Totals Table of this table, ignoring any filters. See <b>getTotalsTable()</b>
		* above for more specifics.
		* 
		* @param config
		* @return promise of dh.TotalsTable
		*/
		getGrandTotalsTable(config?:TotalsTableConfig|undefined|null):Promise<TotalsTable>;
		/**
		* a promise that will resolve to a new roll-up <b>TreeTable</b> of this table. Multiple calls to this method will
		* each produce a new <b>TreeTable</b> which must have close() called on it when not in use.
		* 
		* @param configObject
		* @return Promise of dh.TreeTable
		*/
		rollup(configObject:RollupConfig):Promise<TreeTable>;
		/**
		* a promise that will resolve to a new `TreeTable` of this table. Multiple calls to this method will each produce a
		* new `TreeTable` which must have close() called on it when not in use.
		* 
		* @param configObject
		* @return Promise dh.TreeTable
		*/
		treeTable(configObject:TreeTableConfig):Promise<TreeTable>;
		/**
		* a "frozen" version of this table (a server-side snapshot of the entire source table). Viewports on the frozen
		* table will not update. This does not change the original table, and the new table will not have any of the client
		* side sorts/filters/columns. New client side sorts/filters/columns can be added to the frozen copy.
		*
		* @return Promise of dh.Table
		*/
		freeze():Promise<Table>;
		snapshot(baseTable:Table, doInitialSnapshot?:boolean, stampColumns?:string[]):Promise<Table>;
		/**
		* @deprecated a promise that will be resolved with a newly created table holding the results of the join operation.
		*             The last parameter is optional, and if not specified or empty, all columns from the right table will
		*             be added to the output. Callers are responsible for ensuring that there are no duplicates - a match
		*             pair can be passed instead of a name to specify the new name for the column. Supported `joinType`
		*             values (consult Deephaven's "Joining Data from Multiple Tables for more detail): "Join" <a href=
		*             'https://docs.deephaven.io/latest/Content/writeQueries/tableOperations/joins.htm#Joining_Data_from_Multiple_Tables'>Joining_Data_from_Multiple_Tables</a>
		*             "Natural" "AJ" "ReverseAJ" "ExactJoin" "LeftJoin"
		* @param joinType
		* @param rightTable
		* @param columnsToMatch
		* @param columnsToAdd
		* @param asOfMatchRule
		* @return Promise of dh.Table
		*/
		join(joinType:object, rightTable:JoinableTable, columnsToMatch:Array<string>, columnsToAdd?:Array<string>|undefined|null, asOfMatchRule?:object|undefined|null):Promise<Table>;
		/**
		* a promise that will be resolved with the newly created table holding the results of the specified as-of join
		* operation. The <b>columnsToAdd</b> parameter is optional, not specifying it will result in all columns from the
		* right table being added to the output. The <b>asOfMatchRule</b> is optional, defaults to <b>LESS_THAN_EQUAL</b>
		*
		* <p>
		* the allowed values are:
		* </p>
		*
		* <ul>
		* <li>LESS_THAN_EQUAL</li>
		* <li>LESS_THAN</li>
		* <li>GREATER_THAN_EQUAL</li>
		* <li>GREATER_THAN</li>
		* </ul>
		*
		* @param rightTable
		* @param columnsToMatch
		* @param columnsToAdd
		* @param asOfMatchRule
		* @return Promise og dh.Table
		*/
		asOfJoin(rightTable:JoinableTable, columnsToMatch:Array<string>, columnsToAdd?:Array<string>|undefined|null, asOfMatchRule?:string|undefined|null):Promise<Table>;
		/**
		* a promise that will be resolved with the newly created table holding the results of the specified cross join
		* operation. The <b>columnsToAdd</b> parameter is optional, not specifying it will result in all columns from the
		* right table being added to the output. The <b>reserveBits</b> optional parameter lets the client control how the
		* key space is distributed between the rows in the two tables, see the Java <b>Table</b> class for details.
		*
		* @param rightTable
		* @param columnsToMatch
		* @param columnsToAdd
		* @param reserve_bits
		*
		* @return Promise of dh.Table
		*/
		crossJoin(rightTable:JoinableTable, columnsToMatch:Array<string>, columnsToAdd?:Array<string>, reserve_bits?:number):Promise<Table>;
		/**
		* a promise that will be resolved with the newly created table holding the results of the specified exact join
		* operation. The `columnsToAdd` parameter is optional, not specifying it will result in all columns from the right
		* table being added to the output.
		*
		* @param rightTable
		* @param columnsToMatch
		* @param columnsToAdd
		*
		* @return Promise of dh.Table
		*/
		exactJoin(rightTable:JoinableTable, columnsToMatch:Array<string>, columnsToAdd?:Array<string>):Promise<Table>;
		/**
		* a promise that will be resolved with the newly created table holding the results of the specified natural join
		* operation. The <b>columnsToAdd</b> parameter is optional, not specifying it will result in all columns from the
		* right table being added to the output.
		*
		* @param rightTable
		* @param columnsToMatch
		* @param columnsToAdd
		*
		* @return Promise of dh.Table
		*/
		naturalJoin(rightTable:JoinableTable, columnsToMatch:Array<string>, columnsToAdd?:Array<string>):Promise<Table>;
		byExternal(keys:object, dropKeys?:boolean):Promise<PartitionedTable>;
		/**
		* Creates a new PartitionedTable from the contents of the current table, partitioning data based on the specified
		* keys.
		*
		* @param keys
		* @param dropKeys
		*
		* @return Promise dh.PartitionedTable
		*/
		partitionBy(keys:object, dropKeys?:boolean):Promise<PartitionedTable>;
		/**
		* Seek the row matching the data provided
		*
		* @param startingRow Row to start the seek from
		* @param column Column to seek for value on
		* @param valueType Type of value provided
		* @param seekValue Value to seek
		* @param insensitive Optional value to flag a search as case-insensitive. Defaults to `false`.
		* @param contains Optional value to have the seek value do a contains search instead of exact equality. Defaults to
		*        `false`.
		* @param isBackwards Optional value to seek backwards through the table instead of forwards. Defaults to `false`.
		* @return A promise that resolves to the row value found.
		*/
		seekRow(startingRow:number, column:Column, valueType:ValueTypeType, seekValue:any, insensitive?:boolean|undefined|null, contains?:boolean|undefined|null, isBackwards?:boolean|undefined|null):Promise<number>;
		toString():string;
		/**
		* True if this table represents a user Input Table (created by InputTable.newInputTable). When true, you may call
		* .inputTable() to add or remove data from the underlying table.
		* 
		* @return boolean
		*/
		get hasInputTable():boolean;
		/**
		* The columns that are present on this table. This is always all possible columns. If you specify fewer columns in
		* .setViewport(), you will get only those columns in your ViewportData. <b>Number size</b> The total count of rows
		* in the table. The size can and will change; see the <b>sizechanged</b> event for details. Size will be negative
		* in exceptional cases (eg. the table is uncoalesced, see the <b>isUncoalesced</b> property for details).
		* 
		* @return {@link Column} array
		*/
		get columns():Array<Column>;
		/**
		* An ordered list of Sorts to apply to the table. To update, call <b>applySort()</b>. Note that this getter will
		* return the new value immediately, even though it may take a little time to update on the server. You may listen
		* for the <b>sortchanged</b> event to know when to update the UI.
		* 
		* @return {@link Sort} array
		*/
		get sort():Array<Sort>;
		/**
		* An ordered list of custom column formulas to add to the table, either adding new columns or replacing existing
		* ones. To update, call <b>applyCustomColumns()</b>.
		* 
		* @return {@link CustomColumn} array
		*/
		get customColumns():Array<CustomColumn>;
		/**
		* An ordered list of Filters to apply to the table. To update, call applyFilter(). Note that this getter will
		* return the new value immediately, even though it may take a little time to update on the server. You may listen
		* for the <b>filterchanged</b> event to know when to update the UI.
		* 
		* @return {@link FilterCondition} array
		*/
		get filter():Array<FilterCondition>;
		/**
		* The total count of the rows in the table, excluding any filters. Unlike <b>size</b>, changes to this value will
		* not result in any event. <b>Sort[] sort</b> an ordered list of Sorts to apply to the table. To update, call
		* applySort(). Note that this getter will return the new value immediately, even though it may take a little time
		* to update on the server. You may listen for the <b>sortchanged</b> event to know when to update the UI.
		* 
		* @return double
		*/
		get totalSize():number;
		/**
		* The total count of rows in the table. The size can and will change; see the <b>sizechanged</b> event for details.
		* Size will be negative in exceptional cases (e.g., the table is uncoalesced; see the <b>isUncoalesced</b>
		* property). for details).
		* 
		* @return double
		*/
		get size():number;
		/**
		* True if this table has been closed.
		* 
		* @return boolean
		*/
		get isClosed():boolean;
		/**
		* Read-only. True if this table is uncoalesced. Set a viewport or filter on the partition columns to coalesce the
		* table. Check the <b>isPartitionColumn</b> property on the table columns to retrieve the partition columns. Size
		* will be unavailable until table is coalesced.
		* 
		* @return boolean
		*/
		get isUncoalesced():boolean;
		/**
		* Listen for events on this object.
		*
		* @param name the name of the event to listen for
		* @param callback a function to call when the event occurs
		* @return Returns a cleanup function.
		* @param <T> the type of the data that the event will provide
		*/
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		/**
		* Removes an event listener added to this table.
		*
		* @param name
		* @param callback
		* @return
		* @param <T>
		*/
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
		/**
		* a Sort than can be used to reverse a table. This can be passed into n array in applySort. Note that Tree Tables
		* do not support reverse.
		* 
		* @return {@link Sort}
		*/
		static reverse():Sort;
	}

	/**
	* Presently, this is the entrypoint into the Deephaven JS API. By creating an instance of this with the server URL and
	* some options, JS applications can run code on the server, and interact with available exportable objects.
	*/
	export class IdeConnection implements HasEventHandling {
		/**
		* @deprecated
		*/
		static readonly HACK_CONNECTION_FAILURE:string;
		static readonly EVENT_DISCONNECT:string;
		static readonly EVENT_RECONNECT:string;
		static readonly EVENT_SHUTDOWN:string;

		/**
		* creates a new instance, from which console sessions can be made. <b>options</b> are optional.
		* 
		* @param serverUrl The url used when connecting to the server. Read-only.
		* @param connectOptions Optional Object
		* @param fromJava Optional boolean
		* @deprecated
		*/
		constructor(serverUrl:string, connectOptions?:ConnectOptions, fromJava?:boolean);

		/**
		* closes the current connection, releasing any resources on the server or client.
		*/
		close():void;
		running():Promise<IdeConnection>;
		getObject(definitionObject:dh.ide.VariableDescriptor):Promise<any>;
		subscribeToFieldUpdates(callback:(arg0:dh.ide.VariableChanges)=>void):()=>void;
		notifyServerShutdown(success:dhinternal.io.deephaven.proto.session_pb.TerminationNotificationResponse):void;
		/**
		* Register a callback function to handle any log messages that are emitted on the server. Returns a function ,
		* which can be invoked to remove this log handler. Any log handler registered in this way will receive as many old
		* log messages as are presently available.
		* 
		* @param callback
		* @return {@link JsRunnable}
		*/
		onLogMessage(callback:(arg0:dh.ide.LogItem)=>void):()=>void;
		startSession(type:string):Promise<IdeSession>;
		getConsoleTypes():Promise<Array<string>>;
		getWorkerHeapInfo():Promise<WorkerHeapInfo>;
	}

	/**
	* Represents a set of Tables each corresponding to some key. The keys are available locally, but a call must be made to
	* the server to get each Table. All tables will have the same structure.
	*/
	export class PartitionedTable implements HasEventHandling {
		/**
		* Indicates that a new key has been added to the array of keys, which can now be fetched with getTable.
		*/
		static readonly EVENT_KEYADDED:string;
		static readonly EVENT_DISCONNECT:string;
		static readonly EVENT_RECONNECT:string;
		static readonly EVENT_RECONNECTFAILED:string;

		protected constructor();

		typedTicket():dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
		/**
		* the table with the given key.
		* 
		* @param key
		* @return Promise of dh.Table
		*/
		getTable(key:object):Promise<Table>;
		getMergedTable():Promise<Table>;
		/**
		* The set of all currently known keys. This is kept up to date, so getting the list after adding an event listener
		* for <b>keyadded</b> will ensure no keys are missed.
		* 
		* @return Set of Object
		*/
		getKeys():Set<object>;
		/**
		* Indicates that this PartitionedTable will no longer be used, removing subcriptions to updated keys, etc. This
		* will not affect tables in use.
		*/
		close():void;
		/**
		* The count of known keys.
		* 
		* @return int
		*/
		get size():number;
		/**
		* Listen for events on this object.
		*
		* @param name the name of the event to listen for
		* @param callback a function to call when the event occurs
		* @return Returns a cleanup function.
		* @param <T> the type of the data that the event will provide
		*/
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		/**
		* Removes an event listener added to this table.
		*
		* @param name
		* @param callback
		* @return
		* @param <T>
		*/
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
	}

	/**
	* This class allows iteration over non-contiguous indexes. In the future, this will support the EcmaScript 2015
	* Iteration protocol, but for now has one method which returns an iterator, and also supports querying the size.
	* Additionally, we may add support for creating RangeSet objects to better serve some use cases.
	*/
	export class RangeSet {
		protected constructor();

		static ofRange(first:number, last:number):RangeSet;
		static ofItems(rows:number[]):RangeSet;
		static ofRanges(ranges:RangeSet[]):RangeSet;
		static ofSortedRanges(ranges:RangeSet[]):RangeSet;
		/**
		* a new iterator over all indexes in this collection.
		* 
		* @return Iterator of {@link LongWrapper}
		*/
		iterator():Iterator<LongWrapper>;
		/**
		* The total count of items contained in this collection. In some cases this can be expensive to compute, and
		* generally should not be needed except for debugging purposes, or preallocating space (i.e., do not call this
		* property each time through a loop).
		* 
		* @return double
		*/
		get size():number;
	}

	/**
	* Configuration object for running Table.treeTable to produce a hierarchical view of a given "flat" table.
	*
	* Like TotalsTableConfig, `TreeTableConfig` supports an operation map indicating how to aggregate the data, as well as
	* an array of column names which will be the layers in the roll-up tree, grouped at each level. An additional optional
	* value can be provided describing the strategy the engine should use when grouping the rows.
	*/
	export class TreeTableConfig {
		/**
		* The column representing the unique ID for each item
		*/
		idColumn:string;
		/**
		* The column representing the parent ID for each item
		*/
		parentColumn:string;
		/**
		* Optional parameter indicating if items with an invalid parent ID should be promoted to root. Defaults to false.
		*/
		promoteOrphansToRoot:boolean;

		constructor();
	}

	export class LoginCredentials {
		type?:string|null;
		username?:string|null;
		token?:string|null;

		constructor();
	}

	/**
	* Represents a non-viewport subscription to a table, and all data currently known to be present in the subscribed
	* columns. This class handles incoming snapshots and deltas, and fires events to consumers to notify of data changes.
	*
	* Unlike {@link TableViewportSubscription}, the "original" table does not have a reference to this instance, only the
	* "private" table instance does, since the original cannot modify the subscription, and the private instance must
	* forward data to it.
	*
	* Represents a subscription to the table on the server. Changes made to the table will not be reflected here - the
	* subscription must be closed and a new one optioned to see those changes. The event model is slightly different from
	* viewports to make it less expensive to compute for large tables.
	*/
	export class TableSubscription implements HasEventHandling {
		/**
		* Indicates that some new data is available on the client, either an initial snapshot or a delta update. The
		* <b>detail</b> field of the event will contain a TableSubscriptionEventData detailing what has changed, or
		* allowing access to the entire range of items currently in the subscribed columns.
		*/
		static readonly EVENT_UPDATED:string;

		protected constructor();

		/**
		* Stops the subscription on the server.
		*/
		close():void;
		/**
		* Listen for events on this object.
		*
		* @param name the name of the event to listen for
		* @param callback a function to call when the event occurs
		* @return Returns a cleanup function.
		* @param <T> the type of the data that the event will provide
		*/
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		/**
		* Removes an event listener added to this table.
		*
		* @param name
		* @param callback
		* @return
		* @param <T>
		*/
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
		/**
		* The columns that were subscribed to when this subscription was created
		* 
		* @return {@link Column}
		*/
		get columns():Array<Column>;
	}


	/**
	* This enum describes the name of each supported operation/aggregation type when creating a `TreeTable`.
	*/
	type AggregationOperationType = string;
	export class AggregationOperation {
		static readonly COUNT:AggregationOperationType;
		static readonly COUNT_DISTINCT:AggregationOperationType;
		static readonly DISTINCT:AggregationOperationType;
		static readonly MIN:AggregationOperationType;
		static readonly MAX:AggregationOperationType;
		static readonly SUM:AggregationOperationType;
		static readonly ABS_SUM:AggregationOperationType;
		static readonly VAR:AggregationOperationType;
		static readonly AVG:AggregationOperationType;
		static readonly STD:AggregationOperationType;
		static readonly FIRST:AggregationOperationType;
		static readonly LAST:AggregationOperationType;
		static readonly UNIQUE:AggregationOperationType;
		static readonly SKIP:AggregationOperationType;
	}

	type SearchDisplayModeType = string;
	export class SearchDisplayMode {
		static readonly SEARCH_DISPLAY_DEFAULT:SearchDisplayModeType;
		static readonly SEARCH_DISPLAY_HIDE:SearchDisplayModeType;
		static readonly SEARCH_DISPLAY_SHOW:SearchDisplayModeType;
	}

	/**
	* A set of string constants that can be used to describe the different objects the JS API can export.
	*/
	type VariableTypeType = string;
	export class VariableType {
		static readonly TABLE:VariableTypeType;
		static readonly TREETABLE:VariableTypeType;
		static readonly HIERARCHICALTABLE:VariableTypeType;
		static readonly TABLEMAP:VariableTypeType;
		static readonly PARTITIONEDTABLE:VariableTypeType;
		static readonly FIGURE:VariableTypeType;
		static readonly OTHERWIDGET:VariableTypeType;
		static readonly PANDAS:VariableTypeType;
		static readonly TREEMAP:VariableTypeType;
	}

	type ValueTypeType = string;
	export class ValueType {
		static readonly STRING:ValueTypeType;
		static readonly NUMBER:ValueTypeType;
		static readonly DOUBLE:ValueTypeType;
		static readonly LONG:ValueTypeType;
		static readonly DATETIME:ValueTypeType;
		static readonly BOOLEAN:ValueTypeType;
	}

}

export namespace dh.ide {

	/**
	* Specifies a type and either id or name (but not both).
	*/
	export interface VariableDescriptor {
		type:string;
		id?:string|null;
		name?:string|null;
	}
	/**
	* Represents a serialized fishlib LogRecord, suitable for display on javascript clients. A log entry sent from the
	* server.
	*/
	export interface LogItem {
		/**
		* The level of the log message, enabling the client to ignore messages.
		* 
		* @return String
		*/
		get logLevel():string;
		/**
		* Timestamp of the message in microseconds since Jan 1, 1970 UTC.
		* 
		* @return double
		*/
		get micros():number;
		/**
		* The log message written on the server.
		* 
		* @return String
		*/
		get message():string;
	}
	/**
	* A format to describe a variable available to be read from the server. Application fields are optional, and only
	* populated when a variable is provided by application mode.
	*
	* APIs which take a VariableDefinition` must at least be provided an object with a <b>type</b> and <b>id</b> field.
	*/
	export interface VariableDefinition {
		get name():string;
		/**
		* Optional description for the variable's contents, typically used to provide more detail that wouldn't be
		* reasonable to put in the title
		* 
		* @return String
		*/
		get description():string;
		/**
		* An opaque identifier for this variable
		* 
		* @return String
		*/
		get id():string;
		/**
		* The type of the variable, one of <b>dh.VariableType</b>
		* 
		* @return dh.VariableType.
		*/
		get type():dh.VariableTypeType;
		/**
		* The name of the variable, to be used when rendering it to a user
		* 
		* @return String
		*/
		get title():string;
		/**
		* Optional description for the variable's contents, typically used to provide more detail that wouldn't be
		* reasonable to put in the title
		* 
		* @return String
		*/
		get applicationId():string;
		/**
		* The name of the application which provided this variable
		* 
		* @return String
		*/
		get applicationName():string;
	}
	/**
	* Indicates the result of code run on the server.
	*/
	export interface CommandResult {
		/**
		* Describes changes made in the course of this command.
		* 
		* @return {@link JsVariableChanges}.
		*/
		get changes():VariableChanges;
		/**
		* If the command failed, the error message will be provided here.
		* 
		* @return String
		*/
		get error():string;
	}
	/**
	* Describes changes in the current set of variables in the script session. Note that variables that changed value
	* without changing type will be included as <b>updated</b>, but if a new value with one type replaces an old value with
	* a different type, this will be included as an entry in both <b>removed</b> and <b>created</b> to indicate the old and
	* new types.
	*/
	export interface VariableChanges {
		/**
		* @return The variables that no longer exist after this operation, or were replaced by some variable with a
		*         different type.
		*/
		get removed():Array<VariableDefinition>;
		/**
		* @return The variables that were created by this operation, or have a new type.
		*/
		get created():Array<VariableDefinition>;
		/**
		* @return The variables that changed value during this operation.
		*/
		get updated():Array<VariableDefinition>;
	}
}

export namespace dh.i18n {

	/**
	* Largely an exported wrapper for the GWT DateFormat, but also includes support for formatting nanoseconds as an
	* additional 6 decimal places after the rest of the number.
	*
	* Other concerns that this handles includes accepting a js Date and ignoring the lack of nanos, accepting a js Number
	* and assuming it to be a lossy nano value, and parsing into a js Date.
	*
	*
	* Utility class to parse and format various date/time values, using the same format patterns as are supported by the
	* standard Java implementation used in the Deephaven server and swing client.
	*
	* As Deephaven internally uses nanosecond precision to record dates, this API expects nanoseconds in most use cases,
	* with the one exception of the JS `Date` type, which is not capable of more precision than milliseconds. Note,
	* however, that when passing nanoseconds as a JS `Number` there is likely to be some loss of precision, though this is
	* still supported for easier interoperability with other JS code. The values returned by `parse()` will be an opaque
	* object wrapping the full precision of the specified date, However, this object supports `toString()` and `valueOf()`
	* to return a string representation of that value, as well as a `asNumber()` to return a JS `Number` value and a
	* `asDate()` to return a JS `Date` value.
	*
	*
	* Caveats:
	*
	*
	* - The `D` format (for "day of year") is not supported by this implementation at this time. - The `%t` format for
	* short timezone code is not supported by this implementation at this time, though `z` will work as expected in the
	* browser to emit the user's own timezone.
	*/
	export class DateTimeFormat {
		static readonly NANOS_PER_MILLI:number;

		/**
		* Creates a new date/time format instance. This generally should be avoided in favor of the static `getFormat`
		* function, which will create and cache an instance so that later calls share the same instance.
		* 
		* @param pattern
		*/
		constructor(pattern:string);

		/**
		*
		* @param pattern
		* @return a date format instance matching the specified format. If this format has not been specified before, a new
		*         instance will be created and stored for later reuse.
		*/
		static getFormat(pattern:string):DateTimeFormat;
		/**
		* Accepts a variety of input objects to interpret as a date, and formats them using the specified pattern. A
		* `TimeZone` object can optionally be provided to format this date as the current date/time in that timezone.See
		* the instance method for more details on input objects.
		* 
		* @param pattern
		* @param date
		* @param timeZone
		* @return
		*/
		static format(pattern:string, date:any, timeZone?:TimeZone):string;
		/**
		* Parses the given input string using the provided pattern, and returns a JS `Date` object in milliseconds.
		* 
		* @param pattern
		* @param text
		* @return
		*/
		static parseAsDate(pattern:string, text:string):Date;
		/**
		* Parses the given input string using the provided pattern, and returns a wrapped Java `long` value in nanoseconds.
		* A `TimeZone` object can optionally be provided to parse to a desired timezone.
		* 
		* @param pattern
		* @param text
		* @param tz
		* @return
		*/
		static parse(pattern:string, text:string, tz?:TimeZone):dh.DateWrapper;
		/**
		* Takes a variety of objects to interpret as a date, and formats them using this instance's pattern. Inputs can
		* include a <b>String</b> value of a number expressed in nanoseconds, a <b>Number</b> value expressed in
		* nanoseconds, a JS <b>Date</b> object (necessarily in milliseconds), or a wrapped Java <b>long</b> value,
		* expressed in nanoseconds. A <b>TimeZone</b> object can optionally be provided to format this date as the current
		* date/time in that timezone.
		* 
		* @param date
		* @param timeZone
		* @return String
		*/
		format(date:any, timeZone?:TimeZone):string;
		/**
		* Parses the given string using this instance's pattern, and returns a wrapped Java <b>long</b> value in
		* nanoseconds. A <b>TimeZone</b> object can optionally be provided to parse to a desired timezone.
		* 
		* @param text
		* @param tz
		* @return
		*/
		parse(text:string, tz?:TimeZone):dh.DateWrapper;
		/**
		* Parses the given string using this instance's pattern, and returns a JS <b>Date</b> object in milliseconds.
		* 
		* @param text
		* @return
		*/
		parseAsDate(text:string):Date;
		toString():string;
	}

	/**
	* Represents the timezones supported by Deephaven. Can be used to format dates, taking into account the offset changing
	* throughout the year (potentially changing each year). These instances mostly are useful at this time to pass to the
	* <b>DateTimeFormat.format()</b> methods, though also support a few properties at this time to see details about each
	* instance.
	*
	*
	* The following timezone codes are supported when getting a timezone object - instances appearing in the same line will
	* return the same details:
	*
	* <ul>
	* <li>GMT/UTC</li>
	* <li>Asia/Tokyo</li>
	* <li>Asia/Seoul</li>
	* <li>Asia/Hong_Kong</li>
	* <li>Asia/Singapore</li>
	* <li>Asia/Calcutta/Asia/Kolkata</li>
	* <li>Europe/Berlin</li>
	* <li>Europe/London</li>
	* <li>America/Sao_Paulo</li>
	* <li>America/St_Johns</li>
	* <li>America/Halifax</li>
	* <li>America/New_York</li>
	* <li>America/Chicago</li>
	* <li>America/Denver</li>
	* <li>America/Los_Angeles</li>
	* <li>America/Anchorage</li>
	* <li>Pacific/Honolulu</li>
	* </ul>
	*
	* A Timezone object can also be created from an abbreviation. The following abbreviations are supported:
	*
	* <ul>
	* <li>UTC</li>
	* <li>GMT</li>
	* <li>Z</li>
	* <li>NY</li>
	* <li>ET</li>
	* <li>EST</li>
	* <li>EDT</li>
	* <li>MN</li>
	* <li>CT</li>
	* <li>CST</li>
	* <li>CDT</li>
	* <li>MT</li>
	* <li>MST</li>
	* <li>MDT</li>
	* <li>PT</li>
	* <li>PST</li>
	* <li>PDT</li>
	* <li>HI</li>
	* <li>HST</li>
	* <li>HDT</li>
	* <li>BT</li>
	* <li>BRST</li>
	* <li>BRT</li>
	* <li>KR</li>
	* <li>KST</li>
	* <li>HK</li>
	* <li>HKT</li>
	* <li>JP</li>
	* <li>JST</li>
	* <li>AT</li>
	* <li>AST</li>
	* <li>ADT</li>
	* <li>NF</li>
	* <li>NST</li>
	* <li>NDT</li>
	* <li>AL</li>
	* <li>AKST</li>
	* <li>AKDT</li>
	* <li>IN</li>
	* <li>IST</li>
	* <li>CE</li>
	* <li>CET</li>
	* <li>CEST</li>
	* <li>SG</li>
	* <li>SGT</li>
	* <li>LON</li>
	* <li>BST</li>
	* <li>MOS</li>
	* <li>SHG</li>
	* <li>CH</li>
	* <li>NL</li>
	* <li>TW</li>
	* <li>SYD</li>
	* <li>AEST</li>
	* <li>AEDT</li>
	* </ul>
	*/
	export class TimeZone {
		protected constructor();

		/**
		* Factory method which creates timezone instances from one of the supported keys.
		* 
		* @param tzCode
		* @return dh.i18n.TimeZone
		*/
		static getTimeZone(tzCode:string):TimeZone;
		/**
		* the standard offset of this timezone, in minutes
		* 
		* @return int
		*/
		get standardOffset():number;
		/**
		* the timezone code that represents this `TimeZone`, usually the same key as was use to create this instance
		* 
		* @return String
		*/
		get id():string;
	}

	/**
	* Exported wrapper of the GWT NumberFormat, plus LongWrapper support
	*
	* Utility class to parse and format numbers, using the same format patterns as are supported by the standard Java
	* implementation used in the Deephaven server and swing client. Works for numeric types including BigInteger and
	* BigDecimal.
	*/
	export class NumberFormat {
		/**
		* Creates a new number format instance. This generally should be avoided in favor of the static `getFormat`
		* function, which will create and cache an instance so that later calls share the same instance.
		*
		* @param pattern
		*/
		constructor(pattern:string);

		/**
		* a number format instance matching the specified format. If this format has not been specified before, a new
		* instance will be created and cached for later reuse. Prefer this method to calling the constructor directly to
		* take advantage of caching
		* 
		* @param pattern
		* @return dh.i18n.NumberFormat
		*/
		static getFormat(pattern:string):NumberFormat;
		/**
		* Parses the given text using the cached format matching the given pattern.
		*
		* @param pattern
		* @param text
		* @return double
		*/
		static parse(pattern:string, text:string):number;
		/**
		* Formats the specified number (or Java <b>long</b>, <b>BigInteger</b> or <b>BigDecimal</b> value) using the cached
		* format matching the given pattern string.
		*
		* @param pattern
		* @param number
		* @return String
		*/
		static format(pattern:string, number:number|dh.BigIntegerWrapper|dh.BigDecimalWrapper|dh.LongWrapper):string;
		/**
		* Parses the given text using this instance's pattern into a JS Number.
		*
		* @param text
		* @return double
		*/
		parse(text:string):number;
		/**
		* Formats the specified number (or Java `long`, `BigInteger` or `BigDecimal` value) using this instance's pattern.
		*
		* @param number
		* @return String
		*/
		format(number:number|dh.BigIntegerWrapper|dh.BigDecimalWrapper|dh.LongWrapper):string;
		toString():string;
	}


}

export namespace dh.plot {

	export interface OneClick {
		setValueForColumn(columnName:string, value:any):void;
		getValueForColumn(columName:string):any;
		get requireAllFiltersToDisplay():boolean;
		get columns():dh.Column[];
	}
	/**
	* Describes a template that will be used to make new series instances when a new table added to a plotBy.
	*/
	export interface MultiSeries {
		/**
		* The name for this multi-series.
		* 
		* @return String
		*/
		get name():string;
		/**
		* The plotting style to use for the series that will be created. See <b>SeriesPlotStyle</b> enum for more details.
		* 
		* @return int
		*/
		get plotStyle():SeriesPlotStyleType;
	}
	/**
	* Defines one axis used with by series. These instances will be found both on the Chart and the Series instances, and
	* may be shared between Series instances.
	*/
	export interface Axis {
		/**
		* The format pattern to use with this axis. Use the type to determine which type of formatter to use.
		* 
		* @return String
		*/
		readonly formatPattern?:string|null;
		readonly gapBetweenMajorTicks?:number|null;

		/**
		* Indicates that this axis is only `widthInPixels` wide, so any extra data can be downsampled out, if this can be
		* done losslessly. The second two arguments represent the current zoom range of this axis, and if provided, most of
		* the data outside of this range will be filtered out automatically and the visible width mapped to that range.
		* When the UI zooms, pans, or resizes, this method should be called again to update these three values to ensure
		* that data is correct and current.
		*
		* @param pixelCount
		* @param min
		* @param max
		*/
		range(pixelCount?:number|undefined|null, min?:object|undefined|null, max?:object|undefined|null):void;
		get tickLabelAngle():number;
		get labelFont():string;
		get color():string;
		get invert():boolean;
		get log():boolean;
		get maxRange():number;
		/**
		* The label for this axis.
		* 
		* @return String
		*/
		get label():string;
		get timeAxis():boolean;
		/**
		* The type for this axis, indicating how it will be drawn. See <b>AxisType</b> enum for more details.
		* 
		* @return int
		*/
		get type():AxisTypeType;
		get minorTicksVisible():boolean;
		get minorTickCount():number;
		get majorTickLocations():number[];
		get majorTicksVisible():boolean;
		get ticksFont():string;
		/**
		* The unique id for this axis.
		* 
		* @return String
		*/
		get id():string;
		/**
		* The position for this axis. See <b>AxisPosition</b> enum for more details.
		* 
		* @return int
		*/
		get position():AxisPositionType;
		/**
		* The calendar with the business hours and holidays to transform plot data against. Defaults to null, or no
		* transform.
		* 
		* @return dh.calendar.BusinessCalendar
		*/
		get businessCalendar():dh.calendar.BusinessCalendar;
		/**
		* The type for this axis. See <b>AxisFormatType</b> enum for more details.
		* 
		* @return int
		*/
		get formatType():AxisFormatTypeType;
		get minRange():number;
	}
	export interface FigureDataUpdatedEvent {
		getArray(series:Series, sourceType:number, mappingFunc?:(arg0:any)=>any):Array<any>;
		get series():Series[];
	}
	/**
	* Provides access to the data for displaying in a figure.
	*/
	export interface Series {
		readonly isLinesVisible?:boolean|null;
		readonly pointLabelFormat?:string|null;
		readonly yToolTipPattern?:string|null;
		readonly shapeSize?:number|null;
		readonly xToolTipPattern?:string|null;
		readonly isShapesVisible?:boolean|null;

		subscribe(forceDisableDownsample?:DownsampleOptions):void;
		/**
		* Disable updates for this Series.
		*/
		unsubscribe():void;
		get shape():string;
		/**
		* Contains details on how to access data within the chart for this series. keyed with the way that this series uses
		* the axis.
		* 
		* @return {@link SeriesDataSource}
		*/
		get sources():SeriesDataSource[];
		get lineColor():string;
		/**
		* The plotting style to use for this series. See <b>SeriesPlotStyle</b> enum for more details.
		* 
		* @return int
		*/
		get plotStyle():SeriesPlotStyleType;
		get oneClick():OneClick;
		get gradientVisible():boolean;
		get shapeColor():string;
		/**
		* The name for this series.
		* 
		* @return String
		*/
		get name():string;
		/**
		* indicates that this series belongs to a MultiSeries, null otherwise
		* 
		* @return dh.plot.MultiSeries
		*/
		get multiSeries():MultiSeries;
		get shapeLabel():string;
	}
	/**
	* Describes how to access and display data required within a series.
	*/
	export interface SeriesDataSource {
		/**
		* the type of data stored in the underlying table's Column.
		* 
		* @return String
		*/
		get columnType():string;
		/**
		* the axis that this source should be drawn on.
		* 
		* @return dh.plot.Axis
		*/
		get axis():Axis;
		/**
		* the feature of this series represented by this source. See the <b>SourceType</b> enum for more details.
		* 
		* @return int
		*/
		get type():SourceTypeType;
	}

	/**
	* A descriptor used with JsFigureFactory.create to create a figure from JS.
	*/
	export class FigureDescriptor {
		title?:string|null;
		titleFont?:string|null;
		titleColor?:string|null;
		isResizable?:boolean|null;
		isDefaultTheme?:boolean|null;
		updateInterval?:number|null;
		cols?:number|null;
		rows?:number|null;
		charts:Array<ChartDescriptor>;

		constructor();
	}

	export class SeriesDescriptor {
		plotStyle:string;
		name?:string|null;
		linesVisible?:boolean|null;
		shapesVisible?:boolean|null;
		gradientVisible?:boolean|null;
		lineColor?:string|null;
		pointLabelFormat?:string|null;
		xToolTipPattern?:string|null;
		yToolTipPattern?:string|null;
		shapeLabel?:string|null;
		shapeSize?:number|null;
		shapeColor?:string|null;
		shape?:string|null;
		dataSources:Array<SourceDescriptor>;

		constructor();
	}

	export class Figure implements dh.HasEventHandling {
		/**
		* The title of the figure.
		* 
		* @return String
		*/
		readonly title?:string|null;
		/**
		* The data within this figure was updated. <b>event.detail</b> is <b>FigureUpdateEventData</b>
		*/
		static readonly EVENT_UPDATED:string;
		static readonly EVENT_SERIES_ADDED:string;
		static readonly EVENT_DISCONNECT:string;
		static readonly EVENT_RECONNECT:string;
		static readonly EVENT_RECONNECTFAILED:string;
		static readonly EVENT_DOWNSAMPLESTARTED:string;
		static readonly EVENT_DOWNSAMPLEFINISHED:string;
		static readonly EVENT_DOWNSAMPLEFAILED:string;
		static readonly EVENT_DOWNSAMPLENEEDED:string;

		static create(config:FigureDescriptor):Promise<Figure>;
		getErrors():string[];
		subscribe(forceDisableDownsample?:DownsampleOptions):void;
		/**
		* Disable updates for all series in this figure.
		*/
		unsubscribe():void;
		/**
		* Close the figure, and clean up subscriptions.
		*/
		close():void;
		/**
		* The charts to draw.
		* 
		* @return dh.plot.Chart
		*/
		get charts():Chart[];
		get updateInterval():number;
		get titleColor():string;
		get titleFont():string;
		get rows():number;
		get cols():number;
		/**
		* Listen for events on this object.
		*
		* @param name the name of the event to listen for
		* @param callback a function to call when the event occurs
		* @return Returns a cleanup function.
		* @param <T> the type of the data that the event will provide
		*/
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		/**
		* Removes an event listener added to this table.
		*
		* @param name
		* @param callback
		* @return
		* @param <T>
		*/
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
	}

	export class ChartDescriptor {
		colspan?:number|null;
		rowspan?:number|null;
		series:Array<SeriesDescriptor>;
		axes:Array<AxisDescriptor>;
		chartType:string;
		title?:string|null;
		titleFont?:string|null;
		titleColor?:string|null;
		showLegend?:boolean|null;
		legendFont?:string|null;
		legendColor?:string|null;
		is3d?:boolean|null;

		constructor();
	}

	export class SeriesDataSourceException {
		protected constructor();

		get source():SeriesDataSource;
		get message():string;
	}

	/**
	* Helper class to manage snapshots and deltas and keep not only a contiguous JS array of data per column in the
	* underlying table, but also support a mapping function to let client code translate data in some way for display and
	* keep that cached as well.
	*/
	export class ChartData {
		constructor(table:dh.Table);

		update(tableData:dh.SubscriptionTableData):void;
		getColumn(columnName:string, mappingFunc:(arg0:any)=>any, currentUpdate:dh.TableData):Array<any>;
		/**
		* Removes some column from the cache, avoiding extra computation on incoming events, and possibly freeing some
		* memory. If this pair of column name and map function are requested again, it will be recomputed from scratch.
		*/
		removeColumn(columnName:string, mappingFunc:(arg0:any)=>any):void;
	}

	export class AxisDescriptor {
		formatType:string;
		type:string;
		position:string;
		log?:boolean|null;
		label?:string|null;
		labelFont?:string|null;
		ticksFont?:string|null;
		formatPattern?:string|null;
		color?:string|null;
		minRange?:number|null;
		maxRange?:number|null;
		minorTicksVisible?:boolean|null;
		majorTicksVisible?:boolean|null;
		minorTickCount?:number|null;
		gapBetweenMajorTicks?:number|null;
		majorTickLocations?:Array<number>|null;
		tickLabelAngle?:number|null;
		invert?:boolean|null;
		isTimeAxis?:boolean|null;

		constructor();
	}

	/**
	* Provide the details for a chart.
	*/
	export class Chart implements dh.HasEventHandling {
		/**
		* a new series was added to this chart as part of a multi-series. The series instance is the detail for this event.
		*/
		static readonly EVENT_SERIES_ADDED:string;
		/**
		* The title of the chart.
		* 
		* @return String
		*/
		readonly title?:string|null;

		protected constructor();

		/**
		* Listen for events on this object.
		*
		* @param name the name of the event to listen for
		* @param callback a function to call when the event occurs
		* @return Returns a cleanup function.
		* @param <T> the type of the data that the event will provide
		*/
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		/**
		* Removes an event listener added to this table.
		*
		* @param name
		* @param callback
		* @return
		* @param <T>
		*/
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
		get column():number;
		get showLegend():boolean;
		/**
		* The axes used in this chart.
		* 
		* @return dh.plot.Axis
		*/
		get axes():Axis[];
		get is3d():boolean;
		get titleFont():string;
		get colspan():number;
		get titleColor():string;
		get series():Series[];
		get rowspan():number;
		/**
		* The type of this chart, see <b>ChartType</b> enum for more details.
		* 
		* @return int
		*/
		get chartType():ChartTypeType;
		get row():number;
		get legendColor():string;
		get legendFont():string;
		get multiSeries():MultiSeries[];
	}

	export class SourceDescriptor {
		axis:AxisDescriptor;
		table:dh.Table;
		columnName:string;
		type:string;

		constructor();
	}

	export class DownsampleOptions {
		/**
		* Max number of items in the series before DEFAULT will not attempt to load the series without downsampling. Above
		* this size if downsample fails or is not applicable, the series won't be loaded unless DISABLE is passed to
		* series.subscribe().
		*/
		static MAX_SERIES_SIZE:number;
		/**
		* Max number of items in the series where the subscription will be allowed at all. Above this limit, even with
		* downsampling disabled, the series will not load data.
		*/
		static MAX_SUBSCRIPTION_SIZE:number;
		/**
		* Flag to let the API decide what data will be available, based on the nature of the data, the series, and how the
		* axes are configured.
		*/
		static readonly DEFAULT:DownsampleOptions;
		/**
		* Flat to entirely disable downsampling, and force all data to load, no matter how many items that would be, up to
		* the limit of MAX_SUBSCRIPTION_SIZE.
		*/
		static readonly DISABLE:DownsampleOptions;
	}

	export class FigureFetchError {
		error:object;
		errors:Array<string>;

		protected constructor();
	}

	export class FigureSourceException {
		table:dh.Table;
		source:SeriesDataSource;

		protected constructor();
	}


	type SeriesPlotStyleType = number;
	export class SeriesPlotStyle {
		static readonly BAR:SeriesPlotStyleType;
		static readonly STACKED_BAR:SeriesPlotStyleType;
		static readonly LINE:SeriesPlotStyleType;
		static readonly AREA:SeriesPlotStyleType;
		static readonly STACKED_AREA:SeriesPlotStyleType;
		static readonly PIE:SeriesPlotStyleType;
		static readonly HISTOGRAM:SeriesPlotStyleType;
		static readonly OHLC:SeriesPlotStyleType;
		static readonly SCATTER:SeriesPlotStyleType;
		static readonly STEP:SeriesPlotStyleType;
		static readonly ERROR_BAR:SeriesPlotStyleType;
		static readonly TREEMAP:SeriesPlotStyleType;
	}

	/**
	* This enum describes what kind of chart is being drawn. This may limit what kinds of series can be found on it, or how
	* those series should be rendered.
	*/
	type ChartTypeType = number;
	export class ChartType {
		static readonly XY:ChartTypeType;
		static readonly PIE:ChartTypeType;
		static readonly OHLC:ChartTypeType;
		static readonly CATEGORY:ChartTypeType;
		static readonly XYZ:ChartTypeType;
		static readonly CATEGORY_3D:ChartTypeType;
		static readonly TREEMAP:ChartTypeType;
	}

	type AxisPositionType = number;
	export class AxisPosition {
		static readonly TOP:AxisPositionType;
		static readonly BOTTOM:AxisPositionType;
		static readonly LEFT:AxisPositionType;
		static readonly RIGHT:AxisPositionType;
		static readonly NONE:AxisPositionType;
	}

	type AxisTypeType = number;
	export class AxisType {
		static readonly X:AxisTypeType;
		static readonly Y:AxisTypeType;
		static readonly SHAPE:AxisTypeType;
		static readonly SIZE:AxisTypeType;
		static readonly LABEL:AxisTypeType;
		static readonly COLOR:AxisTypeType;
	}

	/**
	* This enum describes the source it is in, and how this aspect of the data in the series should be used to render the
	* item. For example, a point in a error-bar plot might have a X value, three Y values (Y, Y_LOW, Y_HIGH), and some
	* COLOR per item - the three SeriesDataSources all would share the same Axis instance, but would have different
	* SourceType enums set. The exact meaning of each source type will depend on the series that they are in.
	*/
	type SourceTypeType = number;
	export class SourceType {
		static readonly X:SourceTypeType;
		static readonly Y:SourceTypeType;
		static readonly Z:SourceTypeType;
		static readonly X_LOW:SourceTypeType;
		static readonly X_HIGH:SourceTypeType;
		static readonly Y_LOW:SourceTypeType;
		static readonly Y_HIGH:SourceTypeType;
		static readonly TIME:SourceTypeType;
		static readonly OPEN:SourceTypeType;
		static readonly HIGH:SourceTypeType;
		static readonly LOW:SourceTypeType;
		static readonly CLOSE:SourceTypeType;
		static readonly SHAPE:SourceTypeType;
		static readonly SIZE:SourceTypeType;
		static readonly LABEL:SourceTypeType;
		static readonly COLOR:SourceTypeType;
		static readonly PARENT:SourceTypeType;
		static readonly TEXT:SourceTypeType;
		static readonly HOVER_TEXT:SourceTypeType;
	}

	type AxisFormatTypeType = number;
	export class AxisFormatType {
		static readonly CATEGORY:AxisFormatTypeType;
		static readonly NUMBER:AxisFormatTypeType;
	}

}

export namespace dh.lsp {

	export class CompletionItem {
		label:string;
		kind:number;
		detail:string;
		documentation:MarkupContent;
		deprecated:boolean;
		preselect:boolean;
		textEdit:TextEdit;
		sortText:string;
		filterText:string;
		insertTextFormat:number;
		additionalTextEdits:Array<TextEdit>;
		commitCharacters:Array<string>;

		constructor();
	}

	export class SignatureInformation {
		label:string;
		documentation:MarkupContent;
		parameters:Array<ParameterInformation>;
		activeParameter:number;

		constructor();
	}

	export class TextDocumentContentChangeEvent {
		range:Range;
		rangeLength:number;
		text:string;

		constructor();
	}

	export class Range {
		start:Position;
		end:Position;

		constructor();

		isInside(innerStart:Position, innerEnd:Position):boolean;
	}

	export class Hover {
		contents:MarkupContent;
		range:Range;

		constructor();
	}

	export class MarkupContent {
		kind:string;
		value:string;

		constructor();
	}

	export class ParameterInformation {
		label:string;
		documentation:MarkupContent;

		constructor();
	}

	export class TextEdit {
		range:Range;
		text:string;

		constructor();
	}

	export class Position {
		line:number;
		character:number;

		constructor();

		lessThan(start:Position):boolean;
		lessOrEqual(start:Position):boolean;
		greaterThan(end:Position):boolean;
		greaterOrEqual(end:Position):boolean;
		copy():Position;
	}

}

export namespace dh.calendar {

	export interface BusinessPeriod {
		get close():string;
		get open():string;
	}
	export interface Holiday {
		/**
		* The date of the Holiday.
		* 
		* @return {@link LocalDateWrapper}
		*/
		get date():dh.LocalDateWrapper;
		/**
		* The business periods that are open on the holiday.
		* 
		* @return dh.calendar.BusinessPeriod
		*/
		get businessPeriods():Array<BusinessPeriod>;
	}
	/**
	* Defines a calendar with business hours and holidays.
	*/
	export interface BusinessCalendar {
		/**
		* All holidays defined for this calendar.
		* 
		* @return dh.calendar.Holiday
		*/
		get holidays():Array<Holiday>;
		/**
		* The name of the calendar.
		* 
		* @return String
		*/
		get name():string;
		/**
		* The days of the week that are business days.
		* 
		* @return String array
		*/
		get businessDays():Array<string>;
		/**
		* The time zone of this calendar.
		* 
		* @return dh.i18n.TimeZone
		*/
		get timeZone():dh.i18n.TimeZone;
		/**
		* The business periods that are open on a business day.
		* 
		* @return dh.calendar.BusinessPeriod
		*/
		get businessPeriods():Array<BusinessPeriod>;
	}

	type DayOfWeekType = string;
	export class DayOfWeek {
		static readonly SUNDAY:DayOfWeekType;
		static readonly MONDAY:DayOfWeekType;
		static readonly TUESDAY:DayOfWeekType;
		static readonly WEDNESDAY:DayOfWeekType;
		static readonly THURSDAY:DayOfWeekType;
		static readonly FRIDAY:DayOfWeekType;
		static readonly SATURDAY:DayOfWeekType;

		static values():string[];
	}

}

