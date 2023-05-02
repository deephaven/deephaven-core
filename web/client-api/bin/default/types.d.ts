// Minimum TypeScript Version: 4.3

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
	*/
	export interface SubscriptionTableData extends TableData {
		get fullIndex():RangeSet;
		get removed():RangeSet;
		get added():RangeSet;
		get columns():Array<Column>;
		get modified():RangeSet;
		get rows():Array<unknown>;
	}
	export interface HasEventHandling {
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
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
	*/
	export interface TotalsTable {
		setViewport(firstRow:number, lastRow:number, columns?:Array<Column>, updateIntervalMs?:number):void;
		getViewportData():Promise<TableData>;
		findColumn(key:string):Column;
		findColumns(keys:string[]):Column[];
		close():void;
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
		nextEvent<T>(eventName:string, timeoutInMillis:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		applySort(sort:Sort[]):Array<Sort>;
		applyCustomColumns(customColumns:object[]):Array<CustomColumn>;
		applyFilter(filter:FilterCondition[]):Array<FilterCondition>;
		get filter():Array<FilterCondition>;
		get size():number;
		get columns():Array<Column>;
		get totalsTableConfig():TotalsTableConfig;
		get sort():Array<Sort>;
		get customColumns():Array<CustomColumn>;
	}
	export interface ViewportData extends TableData {
		get offset():number;
		get columns():Array<Column>;
		get rows():Array<ViewportRow>;
	}
	export interface Widget {
		getDataAsBase64():string;
		getDataAsU8():Uint8Array;
		get exportedObjects():WidgetExportedObject[];
		get type():string;
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
	export interface ViewportRow extends Row {
		get index():LongWrapper;
	}
	export interface RefreshToken {
		get bytes():string;
		get expiry():number;
	}
	/**
	* Row implementation that also provides additional read-only properties.
	*/
	export interface TreeRow extends ViewportRow {
		get isExpanded():boolean;
		get depth():number;
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
	export interface WidgetExportedObject {
		fetch():Promise<any>;
		get type():string;
	}
	/**
	* Javascript wrapper for {@link ColumnStatistics}
	*/
	export interface ColumnStatistics {
		/**
		* Gets the type of formatting that should be used for given statistic.
		*
		* @param name the display name of the statistic
		* @return the format type, null to use column formatting
		*/
		getType(name:string):string;
		/**
		* Gets a map with the name of each unique value as key and the count a the value.
		*
		* @return the unique values map
		*/
		get uniqueValues():Map<string, number>;
		/**
		* Gets a map with the display name of statistics as keys and the numeric stat as a value.
		*
		* @return the statistics map
		*/
		get statisticsMap():Map<string, object>;
	}
	export interface LayoutHints {
		get hiddenColumns():string[]|null|undefined;
		get searchDisplayMode():SearchDisplayModeType|null|undefined;
		get frozenColumns():string[]|null|undefined;
		get columnGroups():ColumnGroup[]|null|undefined;
		get areSavedLayoutsAllowed():boolean;
		get frontColumns():string[];
		get backColumns():string[]|null|undefined;
	}
	/**
	* Common interface for various ways of accessing table data and formatting.
	*
	* Java note: this interface contains some extra overloads that aren't available in JS. Implementations are expected to
	* implement only abstract methods, and default methods present in this interface will dispatch accordingly.
	*/
	export interface TableData {
		get(index:object):Row;
		getData(index:object, column:Column):any;
		getFormat(index:object, column:Column):Format;
		get columns():Array<Column>;
		get rows():Array<unknown>;
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
	*/
	export interface TableViewportSubscription extends HasEventHandling {
		setViewport(firstRow:number, lastRow:number, columns?:Column[]|null|undefined, updateIntervalMs?:number|null|undefined):void;
		close():void;
		getViewportData():Promise<TableData>;
		snapshot(rows:RangeSet, columns:Column[]):Promise<TableData>;
	}
	export interface ColumnGroup {
		get name():string|null|undefined;
		get children():string[]|null|undefined;
		get color():string|null|undefined;
	}
	export interface Format {
		get formatString():string|null|undefined;
		get backgroundColor():string|null|undefined;
		get color():string|null|undefined;
		/**
		* @deprecated Prefer formatString.
		*/
		get numberFormat():string|null|undefined;
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

	export class Sort {
		static readonly ASCENDING:string;
		static readonly DESCENDING:string;
		static readonly REVERSE:string;

		protected constructor();

		asc():Sort;
		desc():Sort;
		abs():Sort;
		toString():string;
		get isAbs():boolean;
		get column():Column;
		get direction():string;
	}

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
		showTotalsByDefault:boolean;
		showGrandTotalsByDefault:boolean;
		defaultOperation:AggregationOperationType;
		operationMap:{ [key: string]: Array<AggregationOperationType>; };
		groupBy:Array<string>;

		constructor();

		toString():string;
	}

	export class FilterCondition {
		protected constructor();

		not():FilterCondition;
		and(...filters:FilterCondition[]):FilterCondition;
		or(...filters:FilterCondition[]):FilterCondition;
		toString():string;
		get columns():Array<Column>;
		static invoke(func:string, ...args:FilterValue[]):FilterCondition;
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
		get expression():string;
		get name():string;
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

	export class ConnectOptions {
		headers:{ [key: string]: string; };

		constructor();
	}

	export class Column {
		protected constructor();

		get(row:Row):any;
		getFormat(row:Row):Format;
		sort():Sort;
		filter():FilterValue;
		formatColor(expression:string):CustomColumn;
		formatNumber(expression:string):CustomColumn;
		formatDate(expression:string):CustomColumn;
		toString():string;
		get constituentType():string|null|undefined;
		get name():string;
		get isPartitionColumn():boolean;
		get index():number;
		get description():string|null|undefined;
		get type():string;
		static formatRowColor(expression:string):CustomColumn;
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
	*/
	export class TreeTable {
		static readonly EVENT_UPDATED:string;
		static readonly EVENT_DISCONNECT:string;
		static readonly EVENT_RECONNECT:string;
		static readonly EVENT_RECONNECTFAILED:string;
		static readonly EVENT_REQUEST_FAILED:string;

		protected constructor();

		expand(row:any, expandDescendants?:boolean):void;
		collapse(row:any):void;
		setExpanded(row:any, isExpanded:boolean, expandDescendants?:boolean):void;
		expandAll():void;
		collapseAll():void;
		isExpanded(row:object):boolean;
		setViewport(firstRow:number, lastRow:number, columns?:Array<Column>|null|undefined, updateInterval?:number|null|undefined):void;
		getViewportData():Promise<TreeViewportData>;
		close():void;
		applySort(sort:Sort[]):Array<Sort>;
		applyFilter(filter:FilterCondition[]):Array<FilterCondition>;
		findColumn(key:string):Column;
		findColumns(keys:string[]):Column[];
		/**
		* Provides Table-like selectDistinct functionality, but with a few quirks, since it is only fetching the distinct
		* values for the given columns in the source table:
		* <ul>
		* <li>Rollups may make no sense, since values are aggregated.</li>
		* <li>Values found on orphaned (and remvoed) nodes will show up in the resulting table, even though they are not in
		* the tree.</li>
		* <li>Values found on parent nodes which are only present in the tree since a child is visible will not be present
		* in the resulting table.</li>
		* </ul>
		*/
		selectDistinct(columns:Column[]):Promise<Table>;
		copy():Promise<TreeTable>;
		get filter():Array<FilterCondition>;
		get includeConstituents():boolean;
		get groupedColumns():Array<Column>;
		get size():number;
		get columns():Array<Column>;
		get description():string|null|undefined;
		get sort():Array<Sort>;
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

		constructor(serverUrl:string, connectOptions:object);

		running():Promise<CoreClient>;
		getServerUrl():string;
		getAuthConfigValues():Promise<string[][]>;
		login(credentials:LoginCredentials):Promise<void>;
		relogin(token:RefreshToken):Promise<void>;
		onConnected(timeoutInMillis?:number):Promise<void>;
		getServerConfigValues():Promise<string[][]>;
		getUserInfo():Promise<unknown>;
		getStorageService():dh.storage.StorageService;
		getAsIdeConnection():Promise<IdeConnection>;
		disconnect():void;
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
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
	*/
	export class InputTable {
		protected constructor();

		addRow(row:{ [key: string]: any; }, userTimeZone?:string):Promise<InputTable>;
		addRows(rows:[], userTimeZone?:string):Promise<InputTable>;
		addTable(tableToAdd:Table):Promise<InputTable>;
		addTables(tablesToAdd:Table[]):Promise<InputTable>;
		deleteTable(tableToDelete:Table):Promise<InputTable>;
		deleteTables(tablesToDelete:Table[]):Promise<InputTable>;
		get keys():string[];
		get values():string[];
		get keyColumns():Column[];
		get valueColumns():Column[];
		get table():Table;
	}

	export class RollupConfig {
		groupingColumns:Array<String>;
		aggregations:{ [key: string]: Array<AggregationOperationType>; };
		includeConstituents:boolean;
		includeOriginalColumns:boolean|null|undefined;
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

	export class FilterValue {
		eq(term:FilterValue):FilterCondition;
		eqIgnoreCase(term:FilterValue):FilterCondition;
		notEq(term:FilterValue):FilterCondition;
		notEqIgnoreCase(term:FilterValue):FilterCondition;
		greaterThan(term:FilterValue):FilterCondition;
		lessThan(term:FilterValue):FilterCondition;
		greaterThanOrEqualTo(term:FilterValue):FilterCondition;
		lessThanOrEqualTo(term:FilterValue):FilterCondition;
		in(terms:FilterValue[]):FilterCondition;
		inIgnoreCase(terms:FilterValue[]):FilterCondition;
		notIn(terms:FilterValue[]):FilterCondition;
		notInIgnoreCase(terms:FilterValue[]):FilterCondition;
		contains(term:FilterValue):FilterCondition;
		containsIgnoreCase(term:FilterValue):FilterCondition;
		matches(pattern:FilterValue):FilterCondition;
		matchesIgnoreCase(pattern:FilterValue):FilterCondition;
		isTrue():FilterCondition;
		isFalse():FilterCondition;
		isNull():FilterCondition;
		invoke(method:string, ...args:FilterValue[]):FilterCondition;
		toString():string;
		static ofString(input:any):FilterValue;
		static ofNumber(input:any):FilterValue;
		static ofBoolean(b:boolean):FilterValue;
	}

	export class IdeSession implements HasEventHandling {
		static readonly EVENT_COMMANDSTARTED:string;
		static readonly EVENT_REQUEST_FAILED:string;

		protected constructor();

		getTable(name:string, applyPreviewColumns?:boolean):Promise<Table>;
		getFigure(name:string):Promise<dh.plot.Figure>;
		getTreeTable(name:string):Promise<TreeTable>;
		getHierarchicalTable(name:string):Promise<TreeTable>;
		getObject(definitionObject:dh.ide.VariableDescriptor):Promise<any>;
		newTable(columnNames:string[], types:string[], data:string[][], userTimeZone:string):Promise<Table>;
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
		emptyTable(size:number):Promise<Table>;
		timeTable(periodNanos:number, startTime?:DateWrapper):Promise<Table>;
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
	}

	/**
	* TODO provide hooks into the event handlers so we can see if no one is listening any more and release the table
	* handle/viewport.
	*/
	export class Table {
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
		findColumn(key:string):Column;
		findColumns(keys:string[]):Column[];
		isStreamTable():boolean;
		inputTable():Promise<InputTable>;
		close():void;
		getAttributes():string[];
		getAttribute(attributeName:string):object|null|undefined;
		applySort(sort:Sort[]):Array<Sort>;
		applyFilter(filter:FilterCondition[]):Array<FilterCondition>;
		applyCustomColumns(customColumns:object[]):Array<CustomColumn>;
		setViewport(firstRow:number, lastRow:number, columns?:Array<Column>|null|undefined, updateIntervalMs?:number|null|undefined):TableViewportSubscription;
		getViewportData():Promise<TableData>;
		subscribe(columns:Array<Column>, updateIntervalMs?:number):TableSubscription;
		selectDistinct(columns:Column[]):Promise<Table>;
		copy():Promise<Table>;
		rollup(configObject:RollupConfig):Promise<TreeTable>;
		treeTable(configObject:TreeTableConfig):Promise<TreeTable>;
		freeze():Promise<Table>;
		snapshot(baseTable:Table, doInitialSnapshot?:boolean, stampColumns?:string[]):Promise<Table>;
		/**
		* @deprecated
		*/
		join(joinType:object, rightTable:Table, columnsToMatch:Array<string>, columnsToAdd?:Array<string>|null|undefined, asOfMatchRule?:object|null|undefined):Promise<Table>;
		asOfJoin(rightTable:Table, columnsToMatch:Array<string>, columnsToAdd?:Array<string>|null|undefined, asOfMatchRule?:string|null|undefined):Promise<Table>;
		crossJoin(rightTable:Table, columnsToMatch:Array<string>, columnsToAdd?:Array<string>, reserve_bits?:number):Promise<Table>;
		exactJoin(rightTable:Table, columnsToMatch:Array<string>, columnsToAdd?:Array<string>):Promise<Table>;
		naturalJoin(rightTable:Table, columnsToMatch:Array<string>, columnsToAdd?:Array<string>):Promise<Table>;
		byExternal(keys:object, dropKeys?:boolean):Promise<PartitionedTable>;
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
		seekRow(startingRow:number, column:Column, valueType:ValueTypeType, seekValue:any, insensitive?:boolean|null|undefined, contains?:boolean|null|undefined, isBackwards?:boolean|null|undefined):Promise<number>;
		toString():string;
		get hasInputTable():boolean;
		get columns():Array<Column>;
		get description():string|null|undefined;
		get sort():Array<Sort>;
		get customColumns():Array<CustomColumn>;
		get filter():Array<FilterCondition>;
		get totalSize():number;
		get size():number;
		get isClosed():boolean;
		get isUncoalesced():boolean;
		get pluginName():string|null|undefined;
		get layoutHints():LayoutHints|null|undefined;
		static reverse():Sort;
	}

	export class IdeConnection {
		/**
		* @deprecated
		*/
		static readonly HACK_CONNECTION_FAILURE:string;
		static readonly EVENT_DISCONNECT:string;
		static readonly EVENT_RECONNECT:string;
		static readonly EVENT_SHUTDOWN:string;

		/**
		* @deprecated
		*/
		constructor(serverUrl:string, connectOptions?:object, fromJava?:boolean);

		close():void;
		running():Promise<IdeConnection>;
		getObject(definitionObject:dh.ide.VariableDescriptor):Promise<any>;
		subscribeToFieldUpdates(callback:(arg0:dh.ide.VariableChanges)=>void):()=>void;
		notifyServerShutdown(success:dhinternal.io.deephaven.proto.session_pb.TerminationNotificationResponse):void;
		onLogMessage(callback:(arg0:dh.ide.LogItem)=>void):()=>void;
		startSession(type:string):Promise<IdeSession>;
		getConsoleTypes():Promise<Array<string>>;
		getWorkerHeapInfo():Promise<WorkerHeapInfo>;
	}

	export class PartitionedTable {
		static readonly EVENT_KEYADDED:string;
		static readonly EVENT_DISCONNECT:string;
		static readonly EVENT_RECONNECT:string;
		static readonly EVENT_RECONNECTFAILED:string;

		protected constructor();

		getTable(key:object):Promise<Table>;
		getMergedTable():Promise<Table>;
		getKeys():Set<object>;
		close():void;
		get size():number;
	}

	/**
	* Simple wrapper to emulate RangeSet/Index in JS, with the caveat that LongWrappers may make poor keys in plain JS.
	*/
	export class RangeSet {
		protected constructor();

		static ofRange(first:number, last:number):RangeSet;
		static ofItems(rows:number[]):RangeSet;
		static ofRanges(ranges:RangeSet[]):RangeSet;
		static ofSortedRanges(ranges:RangeSet[]):RangeSet;
		iterator():Iterator<LongWrapper>;
		get size():number;
	}

	/**
	* Configuration object for running Table.treeTable to produce a hierarchical view of a given "flat" table.
	*/
	export class TreeTableConfig {
		idColumn:string;
		parentColumn:string;
		promoteOrphansToRoot:boolean;

		constructor();
	}

	export class LoginCredentials {
		type:string|null|undefined;
		username:string|null|undefined;
		token:string|null|undefined;

		constructor();
	}

	/**
	* Represents a non-viewport subscription to a table, and all data currently known to be present in the subscribed
	* columns. This class handles incoming snapshots and deltas, and fires events to consumers to notify of data changes.
	*
	* Unlike {@link TableViewportSubscription}, the "original" table does not have a reference to this instance, only the
	* "private" table instance does, since the original cannot modify the subscription, and the private instance must
	* forward data to it.
	*/
	export class TableSubscription implements HasEventHandling {
		static readonly EVENT_UPDATED:string;

		protected constructor();

		close():void;
		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
		get columns():Array<Column>;
	}


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
		id:string|null|undefined;
		name:string|null|undefined;
	}
	/**
	* Represents a serialized fishlib LogRecord, suitable for display on javascript clients.
	*/
	export interface LogItem {
		get logLevel():string;
		get micros():number;
		get message():string;
	}
	export interface VariableDefinition {
		get name():string;
		get description():string;
		get id():string;
		get type():dh.VariableTypeType;
		get title():string;
		get applicationId():string;
		get applicationName():string;
	}
	export interface CommandResult {
		get changes():VariableChanges;
		get error():string;
	}
	export interface VariableChanges {
		get removed():Array<VariableDefinition>;
		get created():Array<VariableDefinition>;
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
	*/
	export class DateTimeFormat {
		static readonly NANOS_PER_MILLI:number;

		constructor(pattern:string);

		static getFormat(pattern:string):DateTimeFormat;
		static format(pattern:string, date:any, timeZone?:TimeZone):string;
		static parseAsDate(pattern:string, text:string):Date;
		static parse(pattern:string, text:string, tz?:TimeZone):dh.DateWrapper;
		format(date:any, timeZone?:TimeZone):string;
		parse(text:string, tz?:TimeZone):dh.DateWrapper;
		parseAsDate(text:string):Date;
		toString():string;
	}

	export class TimeZone {
		protected constructor();

		static getTimeZone(tzCode:string):TimeZone;
		get standardOffset():number;
		get id():string;
	}

	/**
	* Exported wrapper of the GWT NumberFormat, plus LongWrapper support
	*/
	export class NumberFormat {
		constructor(pattern:string);

		static getFormat(pattern:string):NumberFormat;
		static parse(pattern:string, text:string):number;
		static format(pattern:string, number:any):string;
		parse(text:string):number;
		format(number:any):string;
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
	export interface MultiSeries {
		get name():string;
		get plotStyle():SeriesPlotStyleType;
	}
	export interface Axis {
		range(pixelCount?:number|null|undefined, min?:object|null|undefined, max?:object|null|undefined):void;
		get tickLabelAngle():number;
		get labelFont():string;
		get color():string;
		get invert():boolean;
		get log():boolean;
		get formatPattern():string|null|undefined;
		get maxRange():number;
		get label():string;
		get timeAxis():boolean;
		get type():AxisTypeType;
		get minorTicksVisible():boolean;
		get minorTickCount():number;
		get majorTickLocations():number[];
		get majorTicksVisible():boolean;
		get ticksFont():string;
		get gapBetweenMajorTicks():number|null|undefined;
		get id():string;
		get position():AxisPositionType;
		get businessCalendar():dh.calendar.BusinessCalendar;
		get formatType():AxisFormatTypeType;
		get minRange():number;
	}
	export interface FigureDataUpdatedEvent {
		getArray(series:Series, sourceType:number, mappingFunc?:(arg0:any)=>any):Array<any>;
		get series():Series[];
	}
	export interface Series {
		subscribe(forceDisableDownsample?:DownsampleOptions):void;
		unsubscribe():void;
		get isLinesVisible():boolean|null|undefined;
		get pointLabelFormat():string|null|undefined;
		get shape():string;
		get sources():SeriesDataSource[];
		get lineColor():string;
		get yToolTipPattern():string|null|undefined;
		get shapeSize():number|null|undefined;
		get plotStyle():SeriesPlotStyleType;
		get oneClick():OneClick;
		get xToolTipPattern():string|null|undefined;
		get gradientVisible():boolean;
		get shapeColor():string;
		get isShapesVisible():boolean|null|undefined;
		get name():string;
		get multiSeries():MultiSeries;
		get shapeLabel():string;
	}
	export interface SeriesDataSource {
		get columnType():string;
		get axis():Axis;
		get type():SourceTypeType;
	}

	/**
	* A descriptor used with JsFigureFactory.create to create a figure from JS.
	*/
	export class FigureDescriptor {
		title:string;
		titleFont:string;
		titleColor:string;
		isResizable:boolean;
		isDefaultTheme:boolean;
		updateInterval:number;
		cols:number;
		rows:number;
		charts:Array<ChartDescriptor>;

		constructor();
	}

	export class SeriesDescriptor {
		plotStyle:string;
		name:string;
		linesVisible:boolean;
		shapesVisible:boolean;
		gradientVisible:boolean;
		lineColor:string;
		pointLabelFormat:string;
		xToolTipPattern:string;
		yToolTipPattern:string;
		shapeLabel:string;
		shapeSize:number;
		shapeColor:string;
		shape:string;
		dataSources:Array<SourceDescriptor>;

		constructor();
	}

	export class Figure {
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
		unsubscribe():void;
		close():void;
		get charts():Chart[];
		get updateInterval():number;
		get titleColor():string;
		get titleFont():string;
		get title():string|null|undefined;
		get rows():number;
		get cols():number;
	}

	export class ChartDescriptor {
		colspan:number|null|undefined;
		rowspan:number|null|undefined;
		series:Array<SeriesDescriptor>|null|undefined;
		axes:Array<AxisDescriptor>|null|undefined;
		chartType:string|null|undefined;
		title:string|null|undefined;
		titleFont:string|null|undefined;
		titleColor:string|null|undefined;
		showLegend:boolean|null|undefined;
		legendFont:string|null|undefined;
		legendColor:string|null|undefined;
		is3d:boolean|null|undefined;

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
		log:boolean;
		label:string;
		labelFont:string;
		ticksFont:string;
		formatPattern:string;
		color:string;
		minRange:number;
		maxRange:number;
		minorTicksVisible:boolean;
		majorTicksVisible:boolean;
		minorTickCount:number;
		gapBetweenMajorTicks:number;
		majorTickLocations:Array<number>;
		tickLabelAngle:number;
		invert:boolean;
		isTimeAxis:boolean;

		constructor();
	}

	export class Chart implements dh.HasEventHandling {
		static readonly EVENT_SERIES_ADDED:string;

		protected constructor();

		addEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):()=>void;
		nextEvent<T>(eventName:string, timeoutInMillis?:number):Promise<CustomEvent<T>>;
		hasListeners(name:string):boolean;
		removeEventListener<T>(name:string, callback:(e:CustomEvent<T>)=>void):boolean;
		get column():number;
		get showLegend():boolean;
		get axes():Axis[];
		get is3d():boolean;
		get titleFont():string;
		get title():string|null|undefined;
		get colspan():number;
		get titleColor():string;
		get series():Series[];
		get rowspan():number;
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
		commitCharacters:object;
		additionalTextEdits:object;

		constructor();

		addAdditionalTextEdits(...edit:TextEdit[]):void;
	}

	export class SignatureInformation {
		label:string;
		documentation:MarkupContent;
		activeParameter:number;
		parameters:object;

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
		get date():dh.LocalDateWrapper;
		get businessPeriods():Array<BusinessPeriod>;
	}
	export interface BusinessCalendar {
		get holidays():Array<Holiday>;
		get name():string;
		get businessDays():Array<string>;
		get timeZone():dh.i18n.TimeZone;
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

