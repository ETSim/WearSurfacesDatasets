import * as wasm from "./parquet_wasm_bg.wasm";
import { __wbg_set_wasm } from "./parquet_wasm_bg.js";
__wbg_set_wasm(wasm);
export * from "./parquet_wasm_bg.js";
