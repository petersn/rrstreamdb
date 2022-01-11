import React from 'react';

export const StreamDBContext = React.createContext<StreamDBProvider | null>(null);

export interface IStreamDBProps {
  host: string | null;
  authToken: string;
}

export type IFieldTypeSpecifier = (
  'Text' |
  'Float' |
  'Integer' |
  'Boolean' |
  'NullableText' |
  'NullableFloat' |
  'NullableInteger' |
  'NullableBoolean' |
  'JSON' |
  // A list of strings corresponds to an enum type.
  string[]
);

function getNumberOfRows(rows: {[key: string]: any[]}): number {
  for (const value of Object.values(rows))
    return value.length;
  return 0;
}

export interface ISchemaTable {
  fields: {[key: string]: IFieldTypeSpecifier}
}

export interface ISchemaSubscription {
  table: string;
  mostRecent: boolean;
  groupBy?: string; 
}

export class StreamDBProvider extends React.PureComponent<IStreamDBProps> {
  socket: WebSocket | null = null;
  status: 'not-connected' | 'connecting' | 'connected' | 'error' = 'not-connected';
  queuedMessages: string[] = [];
  listeners = new Set<Query<unknown>>();
  debugLog: string[] = [];
  requestToken: number = 0;
  callbackTable = new Map<number, {
    isOneShot: boolean;
    callback: (data: any, isConnected: boolean) => void;
  }>();
  schemaTables: {[key: string]: ISchemaTable} = {};
  schemaSubscriptions: {[key: string]: ISchemaSubscription} = {};

  constructor(props: IStreamDBProps) {
    super(props);
    this.state = {};
  }

  componentDidMount(): void {
    this.reconnect();
  }

  reconnect(): void {
    if (this.props.host === null)
      return;
    this.status = 'connecting';
    this.socket = new WebSocket(this.props.host);
    this.socket.addEventListener('open', (event) => {
      this.status = 'connected';
      if (this.socket !== null)
        this.socket.send(this.props.authToken);
      this.sendMessage({ kind: "getSchema" }, true, (data: any) => {
        this.schemaTables = data.tables;
        this.schemaSubscriptions = data.subscriptions;
      });
      // If we have a backlog of messages from when we were disconnected, attempt to send them.
      const queuedMessages = this.queuedMessages;
      this.queuedMessages = []
      for (const message of queuedMessages)
        this.sendRaw(message);
    }); 
    this.socket.addEventListener('error', (event) => {
      this.status = 'error';
    }); 
    this.socket.addEventListener('close', (event) => {
      this.status = 'not-connected';
    });
    // Listen for messages 
    this.socket.addEventListener('message', (event) => { 
      this.onMessage(JSON.parse(event.data));
    });
  }

  sendRaw(message: string): void {
    if (this.socket !== null && this.socket.readyState === WebSocket.OPEN) {
      this.socket.send(message)
    } else {
      this.queuedMessages.push(message);
    }
  }

  sendMessage(
    message: any,
    isOneShot: boolean,
    callback: (data: any, isConnected: boolean) => void,
  ): number {
    this.requestToken++;
    this.callbackTable.set(this.requestToken, { isOneShot, callback });
    this.sendRaw(JSON.stringify({
      ...message,
      token: this.requestToken,
    }));
    return this.requestToken;
  }

  removeCallback(token: number) {
    this.callbackTable.delete(token);
  }

  onMessage(payload: any): void {
    if (this.callbackTable.has(payload.token)) {
      const { isOneShot, callback } = this.callbackTable.get(payload.token)!;
      callback(payload, true);
      if (isOneShot)
        this.callbackTable.delete(payload.token);
    }
    switch (payload.kind) {
      case 'error': {
        this.status = 'error';
        break;
      }
    }
  }

  append(table: string, rows: {[key: string]: any}[]): Promise<any> {
    const soaRows: {[key: string]: any} = {};
    // FIXME: Make sure all rows have exactly the same set of fields!!
    for (const row of rows) {
      for (const [k, v] of Object.entries(row)) {
        if (!soaRows.hasOwnProperty(k))
          soaRows[k] = [];
        soaRows[k].push(v);
      }
    }
    return new Promise(
      (resolve) => this.sendMessage(
        { kind: 'appendBatch', table, rows: soaRows },
        true,
        (data: any) => resolve(data),
      )
    );
  }

  render() {
    return <StreamDBContext.Provider value={this}>
      {this.props.children}
    </StreamDBContext.Provider>
  }
}

export interface IQueryResults<RowType> {
  isConnected: boolean;
  rows: RowType[];
  rowsByGroup: Map<any, RowType[]>;
}

export interface IQueryProps<RowType> {
  query: string;
  cursor?: number;
  groups?: (number | string | [number | string, number])[];
  limit?: number;
  onData?: (results: IQueryResults<RowType>) => void;
  children?: (results: IQueryResults<RowType>) => React.ReactNode;
  subscribe?: boolean;
}

export class Query<RowType> extends React.PureComponent<IQueryProps<RowType>> {
  static contextType = StreamDBContext;
  isConnected = false;
  rows: any[] = [];
  rowsByGroup = new Map<any, any[]>();
  token: number | null = null;

  constructor(props: IQueryProps<RowType>) {
    super(props);
    this.state = {};
  }

  componentDidMount(): void {
    if (this.context === null) {
      console.error("Query component couldn't find context in componentDidMount!");
      return;
    }
    this.context.listeners.add(this);
    this.context.debug('Registering listener');
    this.token = this.context.sendMessage(
      {
        kind: this.props.subscribe ? 'subscribe' : 'query',
        subscription: this.props.query,
        cursor: this.props.cursor,
        groups: this.props.groups?.map((value) => Array.isArray(value) ? value : [value, 0]),
        limit: this.props.limit,
      },
      !this.props.subscribe,
      (data: any, isConnected: boolean) => {
        this.isConnected = isConnected;
        const sub = (this.context as StreamDBProvider).schemaSubscriptions[this.props.query];
        const numRows = getNumberOfRows(data.rows);
        for (let i = 0; i < numRows; i++) {
          const row: {[key: string]: any} = {};
          for (const [k, v] of Object.entries(data.rows))
            row[k] = (v as any[])[i];
          if (sub.groupBy) {
            const groupByValue = row[sub.groupBy];
            if (sub.mostRecent) {
              this.rowsByGroup.set(groupByValue, [row]);
            } else {
              if (!this.rowsByGroup.has(groupByValue))
                this.rowsByGroup.set(groupByValue, []);
              this.rowsByGroup.get(groupByValue)!.push(row);
            }
          } else {
            if (sub.mostRecent) {
              this.rows = [row];
            } else {
              this.rows.push(row);
            }
          }
        }

        if (numRows > 0) {
          this.forceUpdate();
          if (this.props.onData !== undefined)
            this.props.onData(this.makeQueryResults());
        }
      },
    );
  }

  componentWillUnmount(): void {
    if (this.context !== null) {
      this.context.listeners.delete(this);
      this.context.debug('Deregistering listener');
      if (this.token !== null)
        this.context.removeCallback(this.token);
    }
  }

  makeQueryResults(): IQueryResults<RowType> {
    const rowsByGroupCopy = new Map<any, any[]>();
    for (const [k, v] of this.rowsByGroup)
      rowsByGroupCopy.set(k, [...v]);
    return {
      isConnected: this.isConnected,
      rows: [...this.rows],
      rowsByGroup: rowsByGroupCopy,
    };
  }

  render() {
    if (this.props.children === undefined)
      return null;
    return this.props.children(this.makeQueryResults());
  }
}
