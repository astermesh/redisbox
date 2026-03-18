/** Parsed RESP2 value types */
export type RespValue =
  | { type: 'simple'; value: string }
  | { type: 'error'; value: string }
  | { type: 'integer'; value: number | bigint }
  | { type: 'bulk'; value: Buffer | null }
  | { type: 'array'; value: RespValue[] | null };

export type RespCallback = (value: RespValue) => void;
