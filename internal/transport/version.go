package transport

// ProtocolVersion は本ビルドが実装するソケットプロトコルの版番号。
// 値の形式・初期値・増分条件は protocol-envelope.md「版番号（protocol_version）の扱い」節が
// 正典として定める。単調増加する整数の文字列を採用し、初期値は "1" とする。
const ProtocolVersion = "1"
